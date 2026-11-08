use crate::{Db, Error, Result};

/// Same key as the Go runner so Go and Rust engines serialise against each other.
const LOCK_ID: i64 = 5_523_798_343;

macro_rules! m {
    ($v:expr, $f:literal) => {
        ($v, $f, include_str!(concat!("../../../../migrations/", $f)))
    };
}

pub(crate) const MIGRATIONS: &[(i32, &str, &str)] = &[
    m!(1, "001_initial.sql"),
    m!(2, "002_step3.sql"),
    m!(3, "003_unique_schema_version.sql"),
    m!(4, "004_orphan_reaper_index.sql"),
    m!(5, "005_idempotency_mode.sql"),
    m!(6, "006_idempotency_without_mode.sql"),
    m!(7, "007_drop_legacy_problem_bank.sql"),
    m!(8, "008_webhook_delivered.sql"),
];

impl Db {
    /// Applies pending migrations, tracked in the Go runner's `schema_version` table.
    /// Each one runs in its own transaction holding `pg_advisory_xact_lock` on the key the
    /// Go runner locks (session-level) so the two engines exclude each other.
    ///
    /// # Errors
    /// Fails when a migration cannot be applied; that migration's transaction is rolled back.
    pub async fn migrate(&self) -> Result<()> {
        self.migrate_to(i32::MAX).await
    }

    /// Like [`Db::migrate`] but stops after `max_version`.
    ///
    /// # Errors
    /// See [`Db::migrate`].
    pub async fn migrate_to(&self, max_version: i32) -> Result<()> {
        for &(version, name, sql) in MIGRATIONS {
            if version > max_version {
                break;
            }
            self.apply(version, name, sql)
                .await
                .map_err(|source| Error::Migration {
                    name: name.to_owned(),
                    source,
                })?;
        }
        Ok(())
    }

    async fn apply(
        &self,
        version: i32,
        name: &str,
        sql: &'static str,
    ) -> std::result::Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(LOCK_ID)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&mut *tx)
        .await?;
        let current: i32 =
            sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM schema_version")
                .fetch_one(&mut *tx)
                .await?;
        if version <= current {
            return tx.commit().await;
        }
        tracing::info!(file = name, version, "applying migration");
        // Simple-query protocol: migrations hold several statements.
        sqlx::Executor::execute(&mut *tx, sql).await?;
        sqlx::query("INSERT INTO schema_version (version) VALUES ($1)")
            .bind(version)
            .execute(&mut *tx)
            .await?;
        tx.commit().await
    }
}
