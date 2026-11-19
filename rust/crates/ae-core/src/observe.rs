//! Hook through which the worker reports job progress (metrics, drain rate) without
//! depending on the crate that records it. The engine passes the gateway's
//! implementation (`ae_gateway::metrics::WorkerMetrics`) in `WorkerOptions::observer`.

use crate::model::Verdict;
use std::sync::Arc;
use std::time::Duration;

/// Every method defaults to a no-op.
pub trait JobObserver: Send + Sync {
    /// A claimed job starts processing (active-worker gauge up).
    fn job_started(&self) {}
    /// The job left the worker, whatever its outcome (gauge down, one drain completion).
    fn job_ended(&self) {}
    /// This worker recorded the job's verdict; `elapsed` runs from the claim.
    fn job_finished(&self, _language: &str, _verdict: Verdict, _elapsed: Duration) {}
    /// A compile finished or was served from the cache.
    fn compiled(&self, _language: &str, _elapsed: Duration, _cache_hit: bool) {}
    /// One stage of a job (`claim`, `run`, `persist`, `publish`).
    fn stage(&self, _stage: &str, _elapsed: Duration) {}
}

struct Noop;
impl JobObserver for Noop {}

/// Shared handle to a [`JobObserver`]; the default observes nothing.
#[derive(Clone)]
pub struct Observer(pub Arc<dyn JobObserver>);

impl Default for Observer {
    fn default() -> Self {
        Self(Arc::new(Noop))
    }
}

impl std::fmt::Debug for Observer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Observer")
    }
}

impl std::ops::Deref for Observer {
    type Target = dyn JobObserver;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}
