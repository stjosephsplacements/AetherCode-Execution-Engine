//! Redis Streams queue, event log, SSE fan-out hub, reclaimer and orphan reaper.
//!
//! Wire formats match the Go engine (`internal/queue`): stream entries carry
//! `submission_id` (plus an optional `expires_at`, unix ms, that Go ignores),
//! test data lives in the `ac:tests:<id>` hash and events in `ac:events:<id>:log`.

mod consumer;
mod enqueue;
mod events;
mod reaper;
mod reclaim;
mod stream;

pub use consumer::Delivery;
pub use enqueue::{JobSpec, TestData, TestSet};
pub use events::{SUBSCRIBER_BUFFER, Subscription};
pub use reclaim::{DLQ_STREAM, MAX_DELIVERIES, RECLAIM_MIN_IDLE};

use ae_core::Config;
use redis::aio::ConnectionManager;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("queue: redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("queue: db: {0}")]
    Db(#[from] ae_db::Error),
    #[error("queue: {0}")]
    Corrupt(String),
    #[error("queue: crypto: {0}")]
    Crypto(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Handle to the queue. Cheap to clone; clones share connections, the hub and counters.
#[derive(Clone)]
pub struct Queue {
    client: redis::Client,
    conn: ConnectionManager,
    group: String,
    consumer: String,
    submit: String,
    run: String,
    bulk: String,
    legacy: String,
    submit_weight: usize,
    bulk_weight: usize,
    block: Duration,
    master_key: Option<Arc<[u8]>>,
    db: Option<ae_db::Db>,
    poisoned: Arc<AtomicU64>,
    hub: Arc<tokio::sync::Mutex<Option<events::HubHandle>>>,
}

impl Queue {
    /// Connects to `AC_REDIS_ADDR` (with `AC_REDIS_PASSWORD` when set) and idempotently
    /// creates the consumer group on the submit, run and bulk streams.
    ///
    /// # Errors
    /// Fails when Redis is unreachable or a group cannot be created.
    pub async fn connect(cfg: &Config) -> Result<Queue> {
        let url = if cfg.redis_password.is_empty() {
            format!("redis://{}/", cfg.redis_addr)
        } else {
            format!(
                "redis://:{}@{}/",
                percent_encode(&cfg.redis_password),
                cfg.redis_addr
            )
        };
        let client = redis::Client::open(url)?;
        let conn = client.get_connection_manager().await?;
        let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "engine".to_owned());
        let q = Queue {
            client,
            conn,
            group: cfg.consumer_group.clone(),
            consumer: format!("{host}-{}", std::process::id()),
            submit: cfg.submit_stream_name.clone(),
            run: cfg.run_stream_name.clone(),
            bulk: cfg.bulk_stream_name.clone(),
            legacy: cfg.stream_name.clone(),
            // Go's NewTieredConsumer: non-positive weights fall back to 3 and 1.
            submit_weight: if cfg.submit_weight == 0 {
                3
            } else {
                cfg.submit_weight as usize
            },
            bulk_weight: if cfg.bulk_weight == 0 {
                1
            } else {
                cfg.bulk_weight as usize
            },
            block: if cfg.block_read_timeout.is_zero() {
                Duration::from_millis(500)
            } else {
                cfg.block_read_timeout
            },
            master_key: cfg.webhook_master_key.as_deref().map(Arc::from),
            db: None,
            poisoned: Arc::new(AtomicU64::new(0)),
            hub: Arc::new(tokio::sync::Mutex::new(None)),
        };
        for s in q.streams() {
            q.create_group(s).await?;
        }
        Ok(q)
    }

    /// Attaches the database so expired jobs are also marked failed in Postgres.
    #[must_use]
    pub fn with_db(mut self, db: ae_db::Db) -> Queue {
        self.db = Some(db);
        self
    }

    /// Consumer name used for `XREADGROUP` (`$HOSTNAME-<pid>`).
    #[must_use]
    pub fn consumer_name(&self) -> &str {
        &self.consumer
    }

    /// Submit, run and bulk stream names, in weighted-read order.
    #[must_use]
    pub fn streams(&self) -> [&str; 3] {
        [&self.submit, &self.run, &self.bulk]
    }

    #[must_use]
    pub fn run_stream(&self) -> &str {
        &self.run
    }

    /// Messages dead-lettered by the reclaimer since start.
    #[must_use]
    pub fn poisoned(&self) -> u64 {
        self.poisoned.load(std::sync::atomic::Ordering::Relaxed)
    }
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #[test]
    fn percent_encode_escapes_reserved() {
        assert_eq!(super::percent_encode("a@b:c/d%"), "a%40b%3Ac%2Fd%25");
        assert_eq!(super::percent_encode("Ab0-._~"), "Ab0-._~");
    }
}
