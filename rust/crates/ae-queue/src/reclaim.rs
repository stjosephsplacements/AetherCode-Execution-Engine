use crate::consumer::{Delivery, Entry};
use crate::{Queue, Result};
use redis::streams::{StreamAutoClaimReply, StreamPendingCountReply};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A pending message idle this long is reclaimed (Go: 30s).
pub const RECLAIM_MIN_IDLE: Duration = Duration::from_secs(30);
/// A message delivered this many times is dead-lettered instead of retried (Go: 5).
pub const MAX_DELIVERIES: usize = 5;
pub const DLQ_STREAM: &str = "ac:dlq:poison";
const RECLAIMER: &str = "reclaimer";

impl Queue {
    /// Runs [`reclaim_once`] every 30s with a 30s idle threshold until `tx`'s receiver drops.
    ///
    /// [`reclaim_once`]: Queue::reclaim_once
    #[must_use]
    pub fn spawn_reclaimer(&self, tx: mpsc::Sender<Delivery>) -> JoinHandle<()> {
        let q = self.clone();
        tokio::spawn(async move {
            tracing::info!(interval = "30s", "reclaimer started");
            let mut tick = tokio::time::interval(RECLAIM_MIN_IDLE);
            tick.tick().await; // the first tick is immediate
            loop {
                tokio::select! {
                    () = tx.closed() => return,
                    _ = tick.tick() => {}
                }
                if let Err(e) = q.reclaim_once(&tx, RECLAIM_MIN_IDLE).await {
                    tracing::error!(err = %e, "reclaimer: cycle failed");
                }
            }
        })
    }

    /// One pass over every stream's pending list (Go's `reclaimStream`): `XAUTOCLAIM`s
    /// entries idle at least `min_idle` to the `reclaimer` consumer; those delivered
    /// [`MAX_DELIVERIES`] times go to [`DLQ_STREAM`] and are acked unprocessed, expired
    /// ones are failed, the rest are sent to `tx` and stay pending until acked.
    ///
    /// # Errors
    /// Fails on a Redis error; the remaining entries are retried next cycle.
    pub async fn reclaim_once(
        &self,
        tx: &mpsc::Sender<Delivery>,
        min_idle: Duration,
    ) -> Result<()> {
        for s in self.streams().into_iter().filter(|s| !s.is_empty()) {
            if !self.reclaim_stream(s, tx, min_idle).await? {
                return Ok(());
            }
        }
        Ok(())
    }

    /// False when `tx` is closed.
    async fn reclaim_stream(
        &self,
        stream: &str,
        tx: &mpsc::Sender<Delivery>,
        min_idle: Duration,
    ) -> Result<bool> {
        let mut conn = self.conn.clone();
        let mut cursor = "0-0".to_owned();
        loop {
            let claim: StreamAutoClaimReply = redis::cmd("XAUTOCLAIM")
                .arg(stream)
                .arg(&self.group)
                .arg(RECLAIMER)
                .arg(u64::try_from(min_idle.as_millis()).unwrap_or(u64::MAX))
                .arg(&cursor)
                .arg("COUNT")
                .arg(50)
                .query_async(&mut conn)
                .await?;
            if !claim.claimed.is_empty() {
                // The reclaimer's whole PEL, so counts left by interrupted cycles are known too.
                let pending: StreamPendingCountReply = redis::cmd("XPENDING")
                    .arg(stream)
                    .arg(&self.group)
                    .arg("-")
                    .arg("+")
                    .arg(1000)
                    .arg(RECLAIMER)
                    .query_async(&mut conn)
                    .await?;
                let counts: HashMap<&str, usize> = pending
                    .ids
                    .iter()
                    .map(|p| (p.id.as_str(), p.times_delivered))
                    .collect();
                for m in &claim.claimed {
                    let count = counts.get(m.id.as_str()).copied().unwrap_or(0);
                    if count >= MAX_DELIVERIES
                        && let Some(job_id) = m.get::<String>("submission_id")
                    {
                        self.dead_letter(stream, &m.id, &job_id, count).await;
                        continue;
                    }
                    if let Entry::Deliver(d) = self.admit(stream, m).await {
                        tracing::info!(stream, job_id = %d.job_id, msg_id = %d.msg_id,
                            delivery_count = count, "reclaimer: reprocessing");
                        if tx.send(d).await.is_err() {
                            return Ok(false);
                        }
                    }
                }
            }
            if claim.next_stream_id == "0-0" {
                return Ok(true);
            }
            cursor = claim.next_stream_id;
        }
    }

    /// Copies a poison message to the DLQ, then acks it. A failed DLQ write leaves it pending.
    async fn dead_letter(&self, stream: &str, msg_id: &str, job_id: &str, count: usize) {
        tracing::error!(
            stream,
            job_id,
            msg_id,
            delivery_count = count,
            "reclaimer: dead-lettering poison message"
        );
        let r: redis::RedisResult<String> = redis::cmd("XADD")
            .arg(DLQ_STREAM)
            .arg("MAXLEN")
            .arg("~")
            .arg(10_000)
            .arg("*")
            .arg("submission_id")
            .arg(job_id)
            .arg("source_stream")
            .arg(stream)
            .arg("orig_msg_id")
            .arg(msg_id)
            .arg("delivery_count")
            .arg(count)
            .arg("reclaimed_at")
            .arg(rfc3339_utc_now())
            .query_async(&mut self.conn.clone())
            .await;
        if let Err(e) = r {
            tracing::error!(stream, job_id, msg_id, err = %e,
                "reclaimer: failed to write poison message to DLQ, leaving unacknowledged for retry");
            return;
        }
        self.poisoned.fetch_add(1, Ordering::Relaxed);
        let mut c = self.conn.clone();
        let r: redis::RedisResult<i64> = redis::cmd("XACK")
            .arg(stream)
            .arg(&self.group)
            .arg(msg_id)
            .query_async(&mut c)
            .await;
        if let Err(e) = r {
            tracing::error!(stream, msg_id, err = %e, "reclaimer: ack after dead-letter failed");
        }
    }
}

/// `time.Now().UTC().Format(time.RFC3339)`: second precision, `Z` suffix.
fn rfc3339_utc_now() -> String {
    let secs = crate::now_ms().div_euclid(1000);
    rfc3339(secs)
}

fn rfc3339(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn rfc3339_formats_like_go() {
        assert_eq!(super::rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(super::rfc3339(1_700_000_000), "2023-11-14T22:13:20Z");
        assert_eq!(super::rfc3339(951_782_400), "2000-02-29T00:00:00Z");
    }
}
