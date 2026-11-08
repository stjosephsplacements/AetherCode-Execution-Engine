//! Shadow mode (`--shadow`): the worker runs every job beside the Go engine through its
//! own consumer group (`<AC_CONSUMER_GROUP>-rust`) on the same streams, but its output
//! path is swapped: no Postgres writes, no events, no webhooks. The verdict and per-test
//! results go to `ac:shadow:<job_id>` (JSON, 1 h TTL) for `ae-shadowdiff` to compare with
//! the Go engine's row. The runner is the real one; only [`Store`], [`Bus`] and the
//! queue's [`ExpiryStore`] are replaced.

use ae_core::eventlog::Event;
use ae_core::model::{Status, Submission, TestResult, Verdict};
use ae_queue::{BoxFuture, Delivery, ExpiryStore, JobSpec, Queue};
use ae_worker::{Bus, Error, Result, Store};
use async_trait::async_trait;
use redis::aio::ConnectionManager;
use serde::Serialize;
use std::time::Duration;
use tokio::sync::mpsc;

/// Lifetime of an `ac:shadow:<job_id>` record.
pub const SHADOW_TTL: Duration = Duration::from_secs(60 * 60);

/// The consumer group shadow workers read through.
#[must_use]
pub fn group(base: &str) -> String {
    format!("{base}-rust")
}

#[must_use]
pub fn key(job_id: &str) -> String {
    format!("ac:shadow:{job_id}")
}

/// What `ac:shadow:<job_id>` holds.
#[derive(Debug, Serialize)]
pub struct ShadowRecord<'a> {
    pub job_id: &'a str,
    pub engine: &'static str,
    pub verdict: Verdict,
    pub tests_passed: usize,
    pub test_count: usize,
    pub results: &'a [TestResult],
    /// The failure message for compile errors and internal errors; empty otherwise.
    pub compile_stderr: &'a str,
    /// Unix milliseconds.
    pub recorded_at: i64,
}

/// Writes shadow records to Redis.
#[derive(Clone)]
pub struct Sink {
    conn: ConnectionManager,
}

impl Sink {
    #[must_use]
    pub fn new(conn: ConnectionManager) -> Self {
        Self { conn }
    }

    /// Stores the record with [`SHADOW_TTL`]; a later write for the same job replaces it.
    ///
    /// # Errors
    /// Serialisation or Redis failure.
    pub async fn record(
        &self,
        job_id: &str,
        verdict: Verdict,
        results: &[TestResult],
        stderr: &str,
    ) -> Result<()> {
        let rec = ShadowRecord {
            job_id,
            engine: "rust",
            verdict,
            tests_passed: results
                .iter()
                .filter(|r| r.verdict == Verdict::Accepted)
                .count(),
            test_count: results.len(),
            results,
            compile_stderr: stderr,
            recorded_at: chrono::Utc::now().timestamp_millis(),
        };
        let body = serde_json::to_string(&rec).map_err(|e| Error::Other(e.to_string()))?;
        redis::cmd("SET")
            .arg(key(job_id))
            .arg(body)
            .arg("EX")
            .arg(SHADOW_TTL.as_secs())
            .query_async::<()>(&mut self.conn.clone())
            .await
            .map_err(|e| Error::Other(format!("shadow: write {}: {e}", key(job_id))))?;
        tracing::info!(job_id, ?verdict, "shadow verdict recorded");
        Ok(())
    }
}

/// Reads submissions from Postgres, never writes them.
pub struct ShadowStore {
    pub db: ae_db::Db,
    pub sink: Sink,
}

#[async_trait]
impl Store for ShadowStore {
    /// The row as the Go engine left it, presented as queued: the shadow run must not
    /// depend on how far the authoritative engine got.
    async fn get_submission(&self, id: &str) -> Result<Option<Submission>> {
        Ok(self.db.get_submission(id).await?.map(|mut s| {
            s.status = Status::Queued;
            s.verdict = None;
            s
        }))
    }
    async fn set_status(&self, _id: &str, _status: Status) -> Result<bool> {
        Ok(true)
    }
    async fn touch(&self, _id: &str) -> Result<()> {
        Ok(())
    }
    async fn finish_with_results(
        &self,
        id: &str,
        verdict: Verdict,
        results: &[TestResult],
    ) -> Result<bool> {
        self.sink.record(id, verdict, results, "").await?;
        Ok(true)
    }
    async fn fail_submission(&self, id: &str, verdict: Verdict, stderr: &str) -> Result<bool> {
        self.sink.record(id, verdict, &[], stderr).await?;
        Ok(true)
    }
    /// Always "already delivered", so the worker never dispatches a webhook.
    async fn mark_webhook_delivered(&self, _id: &str) -> Result<bool> {
        Ok(false)
    }
}

/// The shadow group's queue with publishing disabled.
pub struct ShadowBus(pub Queue);

#[async_trait]
impl Bus for ShadowBus {
    async fn get_tests(&self, job_id: &str) -> Result<JobSpec> {
        Bus::get_tests(&self.0, job_id).await
    }
    async fn publish(&self, _ev: &Event) -> Result<()> {
        Ok(())
    }
    async fn ack(&self, d: &Delivery) -> Result<()> {
        Bus::ack(&self.0, d).await
    }
    async fn consume(&self, tx: mpsc::Sender<Delivery>) {
        Bus::consume(&self.0, tx).await;
    }
}

/// A job that expired before the shadow worker got it is recorded as a shadow verdict,
/// not as a failed Postgres row.
pub struct ShadowExpiry(pub Sink);

impl ExpiryStore for ShadowExpiry {
    fn fail_expired<'a>(
        &'a self,
        job_id: &'a str,
        verdict: Verdict,
        message: &'a str,
    ) -> BoxFuture<'a, ae_queue::Result<bool>> {
        Box::pin(async move {
            self.0
                .record(job_id, verdict, &[], message)
                .await
                .map_err(|e| ae_queue::Error::Corrupt(e.to_string()))?;
            Ok(true)
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn names() {
        assert_eq!(super::group("workers"), "workers-rust");
        assert_eq!(super::key("j"), "ac:shadow:j");
    }
}
