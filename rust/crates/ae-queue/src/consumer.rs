use crate::{Queue, Result, now_ms};
use ae_core::eventlog::{Event, EventType};
use ae_core::model::Verdict;
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamId, StreamReadReply};
use serde_json::json;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::mpsc;

/// Stderr recorded for a job whose `expires_at` passed before a worker dequeued it.
pub(crate) const EXPIRED_MESSAGE: &str = "job expired in the queue before a worker picked it up";

const LEGACY_BLOCK: Duration = Duration::from_secs(2);

/// One dequeued message. It stays in the consumer group's pending list until
/// [`Queue::ack_many`] is called with it, so a crash before the ack leaves it for the reclaimer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub stream: String,
    pub msg_id: String,
    pub job_id: String,
    /// Unix milliseconds; `None` for Go-enqueued messages.
    pub expires_at: Option<i64>,
}

pub(crate) enum Entry {
    Deliver(Delivery),
    /// Acked or deliberately left pending; nothing to hand out.
    Skip,
}

impl Queue {
    /// Classifies one stream entry: malformed entries are acked (Go acks them too) and
    /// expired jobs get a terminal event and an ack; everything else is delivered.
    pub(crate) async fn admit(&self, stream: &str, m: &StreamId) -> Entry {
        let Some(job_id) = m.get::<String>("submission_id") else {
            tracing::error!(stream, id = %m.id, "queue: invalid message, missing submission_id");
            self.ack_one(stream, &m.id).await;
            return Entry::Skip;
        };
        let expires_at = m
            .get::<String>("expires_at")
            .and_then(|s| s.parse::<i64>().ok());
        if expires_at.is_some_and(|e| now_ms() > e) {
            self.expire(stream, &m.id, &job_id).await;
            return Entry::Skip;
        }
        Entry::Deliver(Delivery {
            stream: stream.to_owned(),
            msg_id: m.id.clone(),
            job_id,
            expires_at,
        })
    }

    /// Fails an expired job without running it. The message is acked only once the
    /// failure is recorded, so a Redis or DB error leaves it for the reclaimer to retry.
    async fn expire(&self, stream: &str, msg_id: &str, job_id: &str) {
        tracing::warn!(stream, job_id, "queue: dropping expired job");
        // false: a verdict is already persisted; do not announce a second one.
        let publish = match self
            .store
            .fail_expired(job_id, Verdict::InternalError, EXPIRED_MESSAGE)
            .await
        {
            Ok(updated) => updated,
            Err(e) => {
                tracing::error!(job_id, err = %e, "queue: failing expired job");
                return;
            }
        };
        if publish {
            let ev = Event::new(
                job_id,
                EventType::Verdict,
                Some(json!({"verdict": Verdict::InternalError, "compile_stderr": EXPIRED_MESSAGE})),
            );
            if let Err(e) = self.publish(&ev).await {
                tracing::error!(job_id, err = %e, "queue: publishing expiry verdict");
                return;
            }
        }
        self.ack_one(stream, msg_id).await;
    }

    async fn ack_one(&self, stream: &str, msg_id: &str) {
        let r: redis::RedisResult<i64> = redis::cmd("XACK")
            .arg(stream)
            .arg(&self.group)
            .arg(msg_id)
            .query_async(&mut self.conn.clone())
            .await;
        if let Err(e) = r {
            // Best effort, like Go: the entry stays pending for the reclaimer.
            tracing::error!(stream, msg_id, err = %e, "queue: ack failed");
        }
    }

    /// Acks every delivery in one pipeline.
    ///
    /// # Errors
    /// Fails on a Redis error; unacked entries remain pending for the reclaimer.
    pub async fn ack_many(&self, ds: &[Delivery]) -> Result<()> {
        if ds.is_empty() {
            return Ok(());
        }
        let mut pipe = redis::pipe();
        for d in ds {
            pipe.cmd("XACK")
                .arg(&d.stream)
                .arg(&self.group)
                .arg(&d.msg_id)
                .ignore();
        }
        pipe.query_async::<()>(&mut self.conn.clone()).await?;
        Ok(())
    }

    async fn admit_reply(&self, reply: StreamReadReply, out: &mut Vec<Delivery>) {
        for key in reply.keys {
            for m in &key.ids {
                if let Entry::Deliver(d) = self.admit(&key.key, m).await {
                    out.push(d);
                }
            }
        }
    }

    /// One weighted, non-blocking read: up to `submit_weight` submit messages, one run
    /// message and up to `bulk_weight` bulk messages, in that order (Go's `DualConsumer`
    /// cycle), fetched as one pipelined round trip of `XREADGROUP COUNT=n`.
    ///
    /// # Errors
    /// Fails on a Redis error.
    pub async fn read_batch(&self) -> Result<Vec<Delivery>> {
        self.read_up_to(self.batch_size()).await
    }

    /// Messages one weighted cycle reads at most.
    pub(crate) fn batch_size(&self) -> usize {
        self.schedule().len()
    }

    /// One weighted cycle as slots: `submit_weight` submit slots, one run slot,
    /// `bulk_weight` bulk slots (Go's `DualConsumer` cycle), skipping unset streams.
    fn schedule(&self) -> Vec<&str> {
        let mut out = Vec::new();
        for (s, w) in [
            (&self.submit, self.submit_weight),
            (&self.run, 1),
            (&self.bulk, self.bulk_weight),
        ] {
            if !s.is_empty() {
                out.extend(std::iter::repeat_n(s.as_str(), w));
            }
        }
        out
    }

    /// Per-stream `COUNT`s for the next `limit` slots of the weighted cycle, starting where
    /// the previous read stopped (weighted round robin), so long-run proportions equal the
    /// weights even when backpressure allows one message per read.
    fn tier_counts(&self, limit: usize) -> Vec<(&str, usize)> {
        let sched = self.schedule();
        if sched.is_empty() || limit == 0 {
            return Vec::new();
        }
        let n = limit.min(sched.len());
        let start = self.cursor.fetch_add(n, Ordering::Relaxed) % sched.len();
        let mut out: Vec<(&str, usize)> = Vec::new();
        for i in 0..n {
            let s = sched[(start + i) % sched.len()];
            match out.last_mut() {
                Some((last, c)) if *last == s => *c += 1,
                _ => out.push((s, 1)),
            }
        }
        out
    }

    /// [`read_batch`](Queue::read_batch) reading at most `limit` messages.
    ///
    /// # Errors
    /// Fails on a Redis error.
    pub async fn read_up_to(&self, limit: usize) -> Result<Vec<Delivery>> {
        let tiers = self.tier_counts(limit);
        if tiers.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = redis::pipe();
        for (stream, count) in tiers {
            pipe.cmd("XREADGROUP")
                .arg("GROUP")
                .arg(&self.group)
                .arg(&self.consumer)
                .arg("COUNT")
                .arg(count)
                .arg("STREAMS")
                .arg(stream)
                .arg(">");
        }
        let replies: Vec<Option<StreamReadReply>> =
            pipe.query_async(&mut self.conn.clone()).await?;
        let mut out = Vec::new();
        for r in replies.into_iter().flatten() {
            self.admit_reply(r, &mut out).await;
        }
        Ok(out)
    }

    /// All streams empty: block up to the read timeout for the next message on any of them.
    async fn block_read(&self, conn: &mut MultiplexedConnection) -> Result<Vec<Delivery>> {
        let streams = self.streams();
        let mut cmd = redis::cmd("XREADGROUP");
        cmd.arg("GROUP")
            .arg(&self.group)
            .arg(&self.consumer)
            .arg("COUNT")
            .arg(1)
            .arg("BLOCK")
            .arg(u64::try_from(self.block.as_millis()).unwrap_or(u64::MAX))
            .arg("STREAMS")
            .arg(&streams);
        for _ in &streams {
            cmd.arg(">");
        }
        let reply: Option<StreamReadReply> = cmd.query_async(conn).await?;
        let mut out = Vec::new();
        if let Some(r) = reply {
            self.admit_reply(r, &mut out).await;
        }
        Ok(out)
    }

    /// A connection for `XREADGROUP BLOCK`: the client's default 500ms response timeout
    /// would cut off any block at least that long.
    async fn open_blocking(&self, block: Duration) -> redis::RedisResult<MultiplexedConnection> {
        let cfg = redis::AsyncConnectionConfig::new()
            .set_response_timeout(Some(block + Duration::from_secs(5)));
        self.client
            .get_multiplexed_async_connection_with_config(&cfg)
            .await
    }

    async fn blocking_conn(&self) -> MultiplexedConnection {
        loop {
            match self.open_blocking(self.block).await {
                Ok(c) => return c,
                Err(e) => {
                    tracing::error!(err = %e, "queue: opening consumer connection");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Feeds `tx` until its receiver is dropped. Each cycle first waits for at least one free
    /// channel slot and reserves up to one batch of them, then does one weighted read of at
    /// most that many messages; when every stream is empty it blocks on all of them
    /// (`AC_BLOCK_READ_TIMEOUT`) for one. A message therefore enters this consumer's pending
    /// list only when a slot is waiting for it, so time spent behind a full channel never
    /// counts toward the reclaimer's idle threshold. Blocking reads use a dedicated
    /// connection so they never stall other commands.
    pub async fn consume(&self, tx: mpsc::Sender<Delivery>) {
        let mut bconn = self.blocking_conn().await;
        loop {
            let Some(mut permits) = reserve(&tx, self.batch_size()).await else {
                return;
            };
            let batch = match self.read_up_to(permits.len()).await {
                Ok(b) if !b.is_empty() => Ok(b),
                Ok(_) => {
                    // COUNT 1: hold one slot while blocking, free the rest for the reclaimer.
                    permits.truncate(1);
                    self.block_read(&mut bconn).await
                }
                Err(e) => Err(e),
            };
            match batch {
                Ok(b) => {
                    // A blocking read applies COUNT per stream, so it can return one entry per
                    // stream while only one slot is held: wait for a slot for each extra entry
                    // so nothing read is left pending undelivered.
                    while permits.len() < b.len() {
                        match tx.reserve().await {
                            Ok(p) => permits.push(p),
                            Err(_) => return,
                        }
                    }
                    for (d, p) in b.into_iter().zip(permits) {
                        p.send(d);
                    }
                }
                Err(e) => {
                    tracing::error!(err = %e, "queue: read error");
                    drop(permits);
                    // Recreates groups lost to a Redis restart (NOGROUP).
                    for s in self.streams() {
                        let _ = self.create_group(s).await;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Hands every message left in the legacy single stream (`AC_STREAM_NAME`) to `tx`
    /// and returns how many it delivered once two consecutive 2s reads come back empty.
    ///
    /// # Errors
    /// Fails when the group cannot be created or a read fails.
    pub async fn drain_legacy(&self, tx: &mpsc::Sender<Delivery>) -> Result<usize> {
        if self.legacy.is_empty() {
            return Ok(0);
        }
        self.create_group(&self.legacy).await?;
        let mut conn = self.open_blocking(LEGACY_BLOCK).await?;
        let (mut empty, mut n) = (0, 0);
        while empty < 2 {
            let Some(permits) = reserve(tx, 10).await else {
                return Ok(n);
            };
            let reply: Option<StreamReadReply> = redis::cmd("XREADGROUP")
                .arg("GROUP")
                .arg(&self.group)
                .arg("legacy-drain")
                .arg("COUNT")
                .arg(permits.len())
                .arg("BLOCK")
                .arg(u64::try_from(LEGACY_BLOCK.as_millis()).unwrap_or(2000))
                .arg("STREAMS")
                .arg(&self.legacy)
                .arg(">")
                .query_async(&mut conn)
                .await?;
            let mut out = Vec::new();
            let got = reply
                .as_ref()
                .is_some_and(|r| r.keys.iter().any(|k| !k.ids.is_empty()));
            if let Some(r) = reply {
                self.admit_reply(r, &mut out).await;
            }
            if got {
                empty = 0;
            } else {
                empty += 1;
            }
            // COUNT = permits.len() on a single stream, so out.len() <= permits.len().
            debug_assert!(out.len() <= permits.len());
            for (d, p) in out.into_iter().zip(permits) {
                p.send(d);
                n += 1;
            }
        }
        tracing::info!(stream = %self.legacy, "legacy stream drained");
        Ok(n)
    }
}

/// Waits for one free slot in `tx`, then takes up to `max` without waiting. `None` when the
/// receiver is gone. Unused permits are released when dropped.
pub(crate) async fn reserve(
    tx: &mpsc::Sender<Delivery>,
    max: usize,
) -> Option<Vec<mpsc::Permit<'_, Delivery>>> {
    let mut permits = vec![tx.reserve().await.ok()?];
    try_reserve_more(tx, max, &mut permits);
    Some(permits)
}

/// Adds permits to `permits` without waiting, up to `max` in total.
pub(crate) fn try_reserve_more<'a>(
    tx: &'a mpsc::Sender<Delivery>,
    max: usize,
    permits: &mut Vec<mpsc::Permit<'a, Delivery>>,
) {
    while permits.len() < max {
        match tx.try_reserve() {
            Ok(p) => permits.push(p),
            Err(_) => break,
        }
    }
}
