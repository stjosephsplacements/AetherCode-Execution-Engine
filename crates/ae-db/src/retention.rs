use crate::{Db, Result};

const BATCH_SIZE: i32 = 500;

impl Db {
    /// Deletes up to [`BATCH_SIZE`] terminal submissions older than `retention_days`.
    ///
    /// # Errors
    /// Fails on database errors; rolls the batch back.
    pub async fn prune_retention_batch(&self, retention_days: u32) -> Result<u64> {
        if retention_days == 0 {
            return Ok(0);
        }

        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT id FROM submissions \
             WHERE status IN ('completed', 'failed') \
               AND updated_at < now() - make_interval(days => $1) \
             ORDER BY updated_at \
             LIMIT $2 \
             FOR UPDATE SKIP LOCKED",
        )
        .bind(i32::try_from(retention_days).unwrap_or(i32::MAX))
        .bind(BATCH_SIZE)
        .fetch_all(&mut *tx)
        .await?;

        if rows.is_empty() {
            tx.rollback().await?;
            return Ok(0);
        }

        sqlx::query("DELETE FROM submission_test_results WHERE submission_id = ANY($1)")
            .bind(&rows)
            .execute(&mut *tx)
            .await?;
        let n = sqlx::query("DELETE FROM submissions WHERE id = ANY($1)")
            .bind(&rows)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(n)
    }

    /// Drains [`prune_retention_batch`] until a batch deletes zero rows.
    ///
    /// # Errors
    /// See [`Db::prune_retention_batch`].
    pub async fn prune_retention_eligible(&self, retention_days: u32) -> Result<u64> {
        let mut total = 0;
        loop {
            let n = self.prune_retention_batch(retention_days).await?;
            total += n;
            if n == 0 {
                return Ok(total);
            }
        }
    }
}