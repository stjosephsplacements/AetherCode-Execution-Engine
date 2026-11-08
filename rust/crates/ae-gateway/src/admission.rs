//! Admission control: shed new work when the target stream is too deep, weighted by the
//! job's estimated cost, with a `Retry-After` from the measured drain rate (port of Go's
//! `AdmissionCheck`, `busyRetryAfter` and `metrics.Drain`).

use crate::{ResponseFuture, json_error, retry_after_header};
use ae_queue::Queue;
use axum::extract::Request;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tower::{Layer, Service};

const DRAIN_WINDOW: Duration = Duration::from_secs(1);
/// Weight of the newest second against the history.
const DRAIN_ALPHA: f64 = 0.3;
/// Retry-After when no drain rate has been measured yet (Go's fallback).
const NO_RATE_RETRY: Duration = Duration::from_secs(10);
const DEPTH_TIMEOUT: Duration = Duration::from_secs(2);

type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

struct DrainState {
    rate: f64,
    window: Option<Instant>,
    count: u64,
}

/// Smoothed job completions per second. Workers call [`Drain::complete`].
pub struct Drain {
    now: Clock,
    st: Mutex<DrainState>,
}

impl Default for Drain {
    fn default() -> Self {
        Self::new()
    }
}

impl Drain {
    #[must_use]
    pub fn new() -> Self {
        Self::with_clock(Arc::new(Instant::now))
    }

    #[must_use]
    pub fn with_clock(now: Clock) -> Self {
        Self {
            now,
            st: Mutex::new(DrainState {
                rate: 0.0,
                window: None,
                count: 0,
            }),
        }
    }

    /// Records one finished job.
    pub fn complete(&self) {
        let mut s = self.lock();
        self.advance(&mut s);
        s.count += 1;
    }

    /// Completions per second; zero until a full second has passed.
    pub fn rate(&self) -> f64 {
        let mut s = self.lock();
        self.advance(&mut s);
        s.rate
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, DrainState> {
        self.st
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Folds every full elapsed window into the rate; idle windows decay it.
    fn advance(&self, s: &mut DrainState) {
        let now = (self.now)();
        let Some(mut w) = s.window else {
            s.window = Some(now);
            return;
        };
        while now.duration_since(w) >= DRAIN_WINDOW {
            #[allow(clippy::cast_precision_loss)]
            let c = s.count as f64;
            s.rate = DRAIN_ALPHA * c + (1.0 - DRAIN_ALPHA) * s.rate;
            s.count = 0;
            w += DRAIN_WINDOW;
        }
        s.window = Some(w);
    }

    /// Time for `excess` queued jobs to drain, clamped to 1..=60 s; 10 s with no rate.
    #[must_use]
    pub fn retry_after(&self, excess: f64) -> Duration {
        let rate = self.rate();
        if rate <= 0.0 {
            return NO_RATE_RETRY;
        }
        Duration::from_secs_f64((excess / rate).ceil().clamp(1.0, 60.0))
    }
}

/// Rejection: 503 with `Retry-After`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shed {
    pub retry_after: Duration,
}

impl IntoResponse for Shed {
    fn into_response(self) -> Response {
        let mut r = json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "server is busy, try again later",
        );
        r.headers_mut()
            .insert(header::RETRY_AFTER, retry_after_header(self.retry_after));
        r
    }
}

pub struct Admission {
    queue: Queue,
    max_depth: usize,
    drain: Arc<Drain>,
}

impl Admission {
    /// `max_depth` is `Config::max_queue_depth()`.
    #[must_use]
    pub fn new(queue: Queue, max_depth: usize, drain: Arc<Drain>) -> Self {
        Self {
            queue,
            max_depth,
            drain,
        }
    }

    #[must_use]
    pub fn drain(&self) -> &Arc<Drain> {
        &self.drain
    }

    /// Admits when `depth + est_cost <= max_depth`, where `est_cost` is the job's cost in
    /// typical-job units (1.0 reproduces Go's `depth < max`). Fails open on Redis errors.
    ///
    /// # Errors
    /// [`Shed`] when over the limit.
    pub async fn check(&self, stream: &str, est_cost: f64) -> Result<(), Shed> {
        let cost = if est_cost.is_finite() && est_cost > 0.0 {
            est_cost
        } else {
            1.0
        };
        let depth = match tokio::time::timeout(DEPTH_TIMEOUT, self.queue.depth(stream)).await {
            Ok(Ok(d)) => d,
            Ok(Err(e)) => {
                tracing::error!(error = %e, "admission check failed, allowing");
                return Ok(());
            }
            Err(_) => {
                tracing::error!("admission check timed out, allowing");
                return Ok(());
            }
        };
        #[allow(clippy::cast_precision_loss)]
        let excess = depth as f64 + cost - self.max_depth as f64;
        if excess <= 0.0 {
            return Ok(());
        }
        Err(Shed {
            retry_after: self.drain.retry_after(excess),
        })
    }

    /// Age of the oldest queued entry: the autoscaling signal.
    ///
    /// # Errors
    /// Redis errors.
    pub async fn queue_age(&self, stream: &str) -> ae_queue::Result<Duration> {
        self.queue.oldest_age(stream).await
    }
}

/// Applies [`Admission::check`] for a fixed stream and cost (innermost layer).
#[derive(Clone)]
pub struct AdmissionLayer {
    admission: Arc<Admission>,
    stream: Arc<str>,
    est_cost: f64,
}

impl AdmissionLayer {
    #[must_use]
    pub fn new(admission: Arc<Admission>, stream: &str, est_cost: f64) -> Self {
        Self {
            admission,
            stream: Arc::from(stream),
            est_cost,
        }
    }
}

impl<S> Layer<S> for AdmissionLayer {
    type Service = AdmissionService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        AdmissionService {
            inner,
            cfg: self.clone(),
        }
    }
}

#[derive(Clone)]
pub struct AdmissionService<S> {
    inner: S,
    cfg: AdmissionLayer,
}

impl<S> Service<Request> for AdmissionService<S>
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = Response;
    type Error = Infallible;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let cfg = self.cfg.clone();
        Box::pin(async move {
            match cfg.admission.check(&cfg.stream, cfg.est_cost).await {
                Ok(()) => inner.call(req).await,
                Err(shed) => Ok(shed.into_response()),
            }
        })
    }
}
