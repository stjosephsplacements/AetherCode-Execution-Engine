use crate::Result;
use ae_core::Config;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Postgres, Transaction};
use std::time::Duration;

/// Main pool plus the optional row-level-security pool.
#[derive(Clone)]
pub struct Db {
    pub(crate) pool: PgPool,
    app: Option<PgPool>,
}

/// Opens the main pool (`db_max_conns`, min a sixteenth clamped to 4..=16) and, when
/// `AC_DATABASE_APP_URL` is set, the small RLS pool (4..=32 connections).
///
/// # Errors
/// Fails when `db_max_conns` is outside 4..=2000 or a database is unreachable.
pub async fn connect(cfg: &Config) -> Result<Db> {
    let max = cfg.db_max_conns;
    if !(4..=2000).contains(&max) {
        return Err(crate::Error::Value(format!(
            "max connections {max} outside 4-2000"
        )));
    }
    let pool = open(&cfg.database_url, (max / 16).clamp(4, 16), max).await?;
    let app = if cfg.database_app_url.is_empty() {
        None
    } else {
        Some(open(&cfg.database_app_url, 4, 32).await?)
    };
    Ok(Db { pool, app })
}

async fn open(url: &str, min: usize, max: usize) -> Result<PgPool> {
    // Bounds are validated or constant, so the narrowing cannot truncate.
    #[allow(clippy::cast_possible_truncation)]
    Ok(PgPoolOptions::new()
        .min_connections(min as u32)
        .max_connections(max as u32)
        .acquire_timeout(Duration::from_secs(10))
        .connect(url)
        .await?)
}

impl Db {
    /// Pool for auth lookups governed by RLS. Falls back to the main pool (owner role,
    /// bypassing RLS) when `AC_DATABASE_APP_URL` is unset, as the Go engine does.
    #[must_use]
    pub fn app_pool(&self) -> &PgPool {
        self.app.as_ref().unwrap_or(&self.pool)
    }

    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Begins a transaction on the app pool scoped to `tenant_id` (`app.tenant_id` is set
    /// locally, which is what the RLS policies read).
    ///
    /// # Errors
    /// Fails when the transaction cannot start.
    pub async fn app_tx(&self, tenant_id: &str) -> Result<Transaction<'static, Postgres>> {
        let mut tx = self.app_pool().begin().await?;
        sqlx::query("SELECT set_config('app.tenant_id', $1, true)")
            .bind(tenant_id)
            .execute(&mut *tx)
            .await?;
        Ok(tx)
    }
}
