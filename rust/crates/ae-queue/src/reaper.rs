use crate::{Queue, Result};
use ae_db::Db;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Go's `reapOrphans` ticker.
pub const REAP_INTERVAL: Duration = Duration::from_secs(60);

impl Queue {
    /// Re-enqueues submissions stuck in `queued` (untouched for 2 minutes, see
    /// `Db::orphaned_submissions`) onto the run stream with Go's message shape, after claiming each
    /// row by bumping `updated_at` (see `Db::claim_orphan`) so the next pass skips it. Returns how many were
    /// re-enqueued.
    ///
    /// Gap: the re-enqueued message carries no `expires_at`, because the `submissions` table
    /// stores no deadline (only the stream message did, and the message is what was lost).
    /// Expired jobs cannot come back this way: expiry at dequeue marks the row `failed`
    /// through the queue's [`ExpiryStore`](crate::ExpiryStore) before acking, and only
    /// `queued` rows are reaped. A job whose message vanished before ever being dequeued is
    /// re-enqueued without a deadline and runs, as in the Go engine.
    ///
    /// # Errors
    /// Fails when the orphan query fails; per-row failures are logged and skipped.
    pub async fn reap_orphans_once(&self, db: &Db) -> Result<usize> {
        let mut n = 0;
        for id in db.orphaned_submissions().await? {
            // Claim before enqueueing: a worker that took the row since the query, or another
            // pass, makes the claim fail and the row is left alone.
            match db.claim_orphan(&id).await {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => {
                    tracing::error!(submission_id = %id, err = %e, "orphan reaper: claim failed");
                    continue;
                }
            }
            if let Err(e) = self.enqueue(&self.run, &id).await {
                // The claim bumped `updated_at`, so the row is retried after the next 2 minutes.
                tracing::error!(submission_id = %id, err = %e, "orphan reaper: re-enqueue failed");
                continue;
            }
            tracing::warn!(submission_id = %id, "orphan reaper: re-enqueued stuck submission");
            n += 1;
        }
        Ok(n)
    }

    /// Runs [`reap_orphans_once`] every 60s for the life of the task.
    ///
    /// [`reap_orphans_once`]: Queue::reap_orphans_once
    #[must_use]
    pub fn spawn_reaper(&self, db: Db) -> JoinHandle<()> {
        let q = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(REAP_INTERVAL);
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(e) = q.reap_orphans_once(&db).await {
                    tracing::error!(err = %e, "orphan reaper: query failed");
                }
            }
        })
    }
}
