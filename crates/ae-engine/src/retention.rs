//! Go's `pruneRetention` background task.

use ae_db::Db;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const RETENTION_PRUNE_INTERVAL: Duration = Duration::from_secs(3600);

/// Hourly retention pruner; stops when `cancel` is fired.
#[must_use]
pub fn spawn_retention_pruner(
    db: Db,
    retention_days: u32,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(RETENTION_PRUNE_INTERVAL);
        tick.tick().await;
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                _ = tick.tick() => {
                    loop {
                        if cancel.is_cancelled() {
                            return;
                        }
                        match db.prune_retention_eligible(retention_days).await {
                            Ok(0) => break,
                            Ok(n) => tracing::info!(
                                deleted = n,
                                retention_days,
                                "retention pruner: deleted submissions"
                            ),
                            Err(e) => {
                                tracing::error!(err = %e, "retention pruner: batch failed");
                                break;
                            }
                        }
                    }
                }
            }
        }
    })
}