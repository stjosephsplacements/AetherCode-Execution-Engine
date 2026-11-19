//! Worker agent: capacity scheduler, compile cache, test runner, custom checker,
//! interactor and webhook delivery (Go: `internal/worker`).
//!
//! The worker talks to Postgres through [`Store`] and to Redis through [`Bus`];
//! [`ae_db::Db`] and [`ae_queue::Queue`] implement them.

mod cache;
pub mod checker;
pub mod interactor;
pub mod runner;
mod scheduler;
pub mod webhook;
mod worker;

pub use cache::{CacheKey, CachedFile, CompileCache};
pub use scheduler::{Cost, Permit, Scheduler};
pub use webhook::WebhookClient;
pub use worker::{Worker, WorkerOptions};

use ae_core::eventlog::Event;
use ae_core::model::{Status, Submission, TestResult, Verdict};
use ae_queue::{Delivery, JobSpec};
use async_trait::async_trait;
use tokio::sync::mpsc;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Db(#[from] ae_db::Error),
    #[error(transparent)]
    Queue(#[from] ae_queue::Error),
    #[error(transparent)]
    Sandbox(#[from] ae_sandbox::Error),
    /// The program did not compile; carries the compiler's diagnostics.
    #[error("{0}")]
    Compile(String),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// The submission rows the worker reads and writes.
#[async_trait]
pub trait Store: Send + Sync + 'static {
    async fn get_submission(&self, id: &str) -> Result<Option<Submission>>;
    /// False when the submission is missing or already terminal.
    async fn set_status(&self, id: &str, status: Status) -> Result<bool>;
    async fn touch(&self, id: &str) -> Result<()>;
    /// Writes the verdict and every result at once; false when already terminal.
    async fn finish_with_results(
        &self,
        id: &str,
        verdict: Verdict,
        results: &[TestResult],
    ) -> Result<bool>;
    /// False when already terminal.
    async fn fail_submission(&self, id: &str, verdict: Verdict, stderr: &str) -> Result<bool>;
    /// True only for the first call per submission.
    async fn mark_webhook_delivered(&self, id: &str) -> Result<bool>;
    /// False for stores whose results are not authoritative (shadow mode): the worker
    /// then never marks or dispatches webhooks.
    fn webhooks_enabled(&self) -> bool {
        true
    }
}

/// Job input, progress events and the delivery stream.
#[async_trait]
pub trait Bus: Send + Sync + 'static {
    async fn get_tests(&self, job_id: &str) -> Result<JobSpec>;
    async fn publish(&self, ev: &Event) -> Result<()>;
    async fn ack(&self, d: &Delivery) -> Result<()>;
    /// Feeds `tx` until its receiver is dropped.
    async fn consume(&self, tx: mpsc::Sender<Delivery>);
}

#[async_trait]
impl Store for ae_db::Db {
    async fn get_submission(&self, id: &str) -> Result<Option<Submission>> {
        Ok(ae_db::Db::get_submission(self, id).await?)
    }
    async fn set_status(&self, id: &str, status: Status) -> Result<bool> {
        Ok(ae_db::Db::set_status(self, id, status).await?)
    }
    async fn touch(&self, id: &str) -> Result<()> {
        Ok(self.touch_submission(id).await?)
    }
    async fn finish_with_results(
        &self,
        id: &str,
        verdict: Verdict,
        results: &[TestResult],
    ) -> Result<bool> {
        Ok(ae_db::Db::finish_with_results(self, id, verdict, results).await?)
    }
    async fn fail_submission(&self, id: &str, verdict: Verdict, stderr: &str) -> Result<bool> {
        Ok(ae_db::Db::fail_submission(self, id, verdict, stderr).await?)
    }
    async fn mark_webhook_delivered(&self, id: &str) -> Result<bool> {
        Ok(ae_db::Db::mark_webhook_delivered(self, id).await?)
    }
}

#[async_trait]
impl Bus for ae_queue::Queue {
    async fn get_tests(&self, job_id: &str) -> Result<JobSpec> {
        Ok(ae_queue::Queue::get_tests(self, job_id).await?)
    }
    async fn publish(&self, ev: &Event) -> Result<()> {
        Ok(ae_queue::Queue::publish(self, ev).await?)
    }
    async fn ack(&self, d: &Delivery) -> Result<()> {
        Ok(self.ack_many(std::slice::from_ref(d)).await?)
    }
    /// Runs the stream consumer and the reclaimer until `tx`'s receiver is dropped.
    async fn consume(&self, tx: mpsc::Sender<Delivery>) {
        // Aborted on drop too, so cancelling `consume` stops the reclaimer.
        let _reclaimer = AbortOnDrop(self.spawn_reclaimer(tx.clone()));
        ae_queue::Queue::consume(self, tx).await;
    }
}

pub(crate) struct AbortOnDrop<T>(pub(crate) tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
