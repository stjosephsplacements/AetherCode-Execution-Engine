use crate::{Queue, Result, now_ms};
use redis::streams::{StreamInfoGroupsReply, StreamRangeReply};
use std::time::Duration;

/// Approximate `MAXLEN` of every submission stream (Go: 100,000).
pub(crate) const STREAM_MAXLEN: usize = 100_000;

/// Millisecond part of a stream id (`<ms>-<seq>`).
pub(crate) fn id_ms(id: &str) -> Option<i64> {
    id.split_once('-')?.0.parse().ok()
}

impl Queue {
    pub(crate) async fn create_group(&self, stream: &str) -> Result<()> {
        let r: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(stream)
            .arg(&self.group)
            .arg(self.group_start)
            .arg("MKSTREAM")
            .query_async(&mut self.conn.clone())
            .await;
        match r {
            Err(e) if e.code() == Some("BUSYGROUP") => Ok(()),
            other => Ok(other?),
        }
    }

    /// Adds a Go-shaped entry (`submission_id` only), as Go's `Stream.Enqueue` does.
    ///
    /// # Errors
    /// Fails on a Redis error.
    pub async fn enqueue(&self, stream: &str, job_id: &str) -> Result<()> {
        let _: String = redis::cmd("XADD")
            .arg(stream)
            .arg("MAXLEN")
            .arg("~")
            .arg(STREAM_MAXLEN)
            .arg("*")
            .arg("submission_id")
            .arg(job_id)
            .query_async(&mut self.conn.clone())
            .await?;
        Ok(())
    }

    /// Jobs waiting or in flight on a stream: the group's undelivered `lag` plus its pending
    /// (delivered, unacknowledged) entries. `XLEN` is only the retained history, because acked
    /// entries stay until `MAXLEN` trims them; admitting on it sheds every request once the stream
    /// has filled, with an idle queue. Falls back to `XLEN` when the group or its `lag` is unknown
    /// (no group yet, or Redis could not compute it after a deletion), which over-counts at worst.
    ///
    /// # Errors
    /// Fails on a Redis error.
    pub async fn depth(&self, stream: &str) -> Result<u64> {
        let mut conn = self.conn.clone();
        let groups: redis::RedisResult<StreamInfoGroupsReply> = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(stream)
            .query_async(&mut conn)
            .await;
        if let Ok(reply) = groups
            && let Some(g) = reply.groups.iter().find(|g| g.name == self.group)
            && let Some(lag) = g.lag
        {
            return Ok(lag as u64 + g.pending as u64);
        }
        Ok(redis::cmd("XLEN")
            .arg(stream)
            .query_async(&mut conn)
            .await?)
    }

    /// Age of the oldest entry the group has not yet delivered (zero when none are waiting): how
    /// long the next job has been queued. Acked history and in-flight jobs do not count, because
    /// they stay in the stream until `MAXLEN` trims them.
    ///
    /// # Errors
    /// Fails on a Redis error.
    pub async fn oldest_age(&self, stream: &str) -> Result<Duration> {
        let mut conn = self.conn.clone();
        let groups: StreamInfoGroupsReply = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(stream)
            .query_async(&mut conn)
            .await?;
        let Some(g) = groups.groups.iter().find(|g| g.name == self.group) else {
            return Ok(Duration::ZERO);
        };
        let r: StreamRangeReply = redis::cmd("XRANGE")
            .arg(stream)
            .arg(format!("({}", g.last_delivered_id))
            .arg("+")
            .arg("COUNT")
            .arg(1)
            .query_async(&mut conn)
            .await?;
        let age = r
            .ids
            .first()
            .and_then(|m| id_ms(&m.id))
            .map_or(0, |ms| now_ms().saturating_sub(ms).max(0));
        Ok(Duration::from_millis(u64::try_from(age).unwrap_or(0)))
    }

    /// Startup purge: `XTRIM MINID` drops entries older than `job_timeout` from the configured
    /// (non-empty) submit, run and bulk streams; any such job's caller has already timed out.
    /// Returns the count.
    ///
    /// `XTRIM MINID` also drops delivered-but-unacked entries older than `job_timeout`, so a
    /// job that was still running when the engine restarted is not retried by the reclaimer.
    ///
    /// # Errors
    /// Fails on a Redis error.
    pub async fn trim_stale(&self, job_timeout: Duration) -> Result<usize> {
        let timeout_ms = i64::try_from(job_timeout.as_millis()).unwrap_or(i64::MAX);
        let min_id = format!("{}-0", now_ms().saturating_sub(timeout_ms).max(0));
        let mut pipe = redis::pipe();
        for s in self.streams() {
            pipe.cmd("XTRIM").arg(s).arg("MINID").arg(&min_id);
        }
        let counts: Vec<usize> = pipe.query_async(&mut self.conn.clone()).await?;
        let n = counts.iter().sum();
        tracing::info!("purged {n} stale entries from queue");
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn id_ms_parses_stream_ids() {
        assert_eq!(super::id_ms("1700000000000-3"), Some(1_700_000_000_000));
        assert_eq!(super::id_ms("bad"), None);
        assert_eq!(super::id_ms("x-1"), None);
    }
}
