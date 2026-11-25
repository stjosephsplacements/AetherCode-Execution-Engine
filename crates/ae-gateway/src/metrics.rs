//! Prometheus metrics (port of `internal/metrics`), served by [`metrics_router`] on the
//! loopback debug port, never the public one. Every `ac_*` name from docs/ARCHITECTURE.md
//! is kept; stage, queue-age, compile-cache and scheduler-budget series are added.

use crate::admission::Drain;
use ae_core::model::Verdict;
use ae_core::observe::JobObserver;
use ae_queue::Queue;
use axum::Router;
use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use prometheus::{
    Encoder, Gauge, GaugeVec, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts,
    Registry, TextEncoder,
};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

const HTTP_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];
const JOB_BUCKETS: &[f64] = &[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 90.0];
const STAGE_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.025, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];
const VERDICTS: &[&str] = &[
    "accepted",
    "wrong_answer",
    "time_limit",
    "memory_limit",
    "runtime_error",
    "compilation_error",
    "internal_error",
    "output_limit",
    "cancelled",
];

/// The process-wide collectors.
pub struct Metrics {
    registry: Registry,
    pub http_requests: IntCounterVec,
    pub http_duration: HistogramVec,
    pub jobs_processed: IntCounterVec,
    pub job_duration: HistogramVec,
    pub compile_duration: HistogramVec,
    pub active_workers: Gauge,
    pub queue_depth: GaugeVec,
    pub rate_limit_rejections: IntCounterVec,
    pub poison_messages: IntCounter,
    pub stage_duration: HistogramVec,
    pub queue_age: GaugeVec,
    pub compile_cache_hits: IntCounter,
    pub scheduler_budget_used: GaugeVec,
}

fn opts(name: &str, help: &str) -> Opts {
    Opts::new(name, help).namespace("ac")
}

fn hopts(name: &str, help: &str, buckets: &[f64]) -> HistogramOpts {
    HistogramOpts::new(name, help)
        .namespace("ac")
        .buckets(buckets.to_vec())
}

impl Metrics {
    #[allow(clippy::too_many_lines)]
    fn new() -> prometheus::Result<Self> {
        let m = Self {
            registry: Registry::new(),
            http_requests: IntCounterVec::new(
                opts(
                    "http_requests_total",
                    "Total HTTP requests by method, path, and status code.",
                ),
                &["method", "path", "code"],
            )?,
            http_duration: HistogramVec::new(
                hopts(
                    "http_request_duration_seconds",
                    "HTTP request latency in seconds.",
                    HTTP_BUCKETS,
                ),
                &["method", "path"],
            )?,
            jobs_processed: IntCounterVec::new(
                opts("jobs_processed_total", "Total jobs processed by verdict."),
                &["verdict"],
            )?,
            job_duration: HistogramVec::new(
                hopts(
                    "job_processing_duration_seconds",
                    "Time from dequeue to verdict in seconds.",
                    JOB_BUCKETS,
                ),
                &["language", "verdict"],
            )?,
            compile_duration: HistogramVec::new(
                hopts(
                    "compile_duration_seconds",
                    "Time spent in the compilation step per language.",
                    JOB_BUCKETS,
                ),
                &["language"],
            )?,
            active_workers: Gauge::with_opts(opts(
                "active_workers",
                "Number of workers currently processing a job.",
            ))?,
            queue_depth: GaugeVec::new(
                opts("queue_depth", "Approximate queue depth per stream."),
                &["stream"],
            )?,
            rate_limit_rejections: IntCounterVec::new(
                opts(
                    "rate_limit_rejections_total",
                    "Rate limit rejections by type (user or ip).",
                ),
                &["type"],
            )?,
            poison_messages: IntCounter::with_opts(opts(
                "poison_messages_total",
                "Messages dead-lettered after exceeding max delivery count.",
            ))?,
            stage_duration: HistogramVec::new(
                hopts(
                    "stage_duration_seconds",
                    "Time spent in each stage of processing a job.",
                    STAGE_BUCKETS,
                ),
                &["stage"],
            )?,
            queue_age: GaugeVec::new(
                opts(
                    "queue_age_seconds",
                    "Age of the oldest entry in each stream (the autoscaling signal).",
                ),
                &["stream"],
            )?,
            compile_cache_hits: IntCounter::with_opts(opts(
                "compile_cache_hits_total",
                "Compile cache hits: the binary was reused, skipping recompilation.",
            ))?,
            scheduler_budget_used: GaugeVec::new(
                opts(
                    "scheduler_budget_used",
                    "Fraction of the node's scheduler budget in use, by resource.",
                ),
                &["resource"],
            )?,
        };
        let r = &m.registry;
        r.register(Box::new(m.http_requests.clone()))?;
        r.register(Box::new(m.http_duration.clone()))?;
        r.register(Box::new(m.jobs_processed.clone()))?;
        r.register(Box::new(m.job_duration.clone()))?;
        r.register(Box::new(m.compile_duration.clone()))?;
        r.register(Box::new(m.active_workers.clone()))?;
        r.register(Box::new(m.queue_depth.clone()))?;
        r.register(Box::new(m.rate_limit_rejections.clone()))?;
        r.register(Box::new(m.poison_messages.clone()))?;
        r.register(Box::new(m.stage_duration.clone()))?;
        r.register(Box::new(m.queue_age.clone()))?;
        r.register(Box::new(m.compile_cache_hits.clone()))?;
        r.register(Box::new(m.scheduler_budget_used.clone()))?;
        // Series with a fixed label set exist from the start, so dashboards see zeros.
        for v in VERDICTS {
            m.jobs_processed.with_label_values(&[*v]);
        }
        for t in ["user", "ip"] {
            m.rate_limit_rejections.with_label_values(&[t]);
        }
        for r in ["cpu", "memory"] {
            m.scheduler_budget_used.with_label_values(&[r]);
        }
        Ok(m)
    }

    /// Text exposition of every collector.
    #[must_use]
    pub fn render(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        if let Err(e) = TextEncoder::new().encode(&self.registry.gather(), &mut buf) {
            tracing::error!(error = %e, "metrics: encode failed");
        }
        buf
    }
}

/// The global collectors (Go's `promauto` registrations).
///
/// # Panics
/// Never in practice: the collector definitions are static and valid.
pub fn metrics() -> &'static Metrics {
    static M: OnceLock<Metrics> = OnceLock::new();
    M.get_or_init(|| Metrics::new().expect("metrics: static collector definitions"))
}

/// Wire name of a verdict (`accepted`, `time_limit`, ...).
#[must_use]
pub fn verdict_label(v: Verdict) -> String {
    match serde_json::to_value(v) {
        Ok(serde_json::Value::String(s)) => s,
        _ => "unknown".to_owned(),
    }
}

/// The debug listener (`AC_DEBUG_ADDR:AC_DEBUG_PORT`, loopback by default): `/metrics`
/// plus `/livez` (always 200) and `/readyz` (200 while `ready` is set, else 503), so
/// worker-only pods, which serve no public HTTP, have probe endpoints. Nothing else.
pub fn metrics_router(ready: std::sync::Arc<std::sync::atomic::AtomicBool>) -> Router {
    Router::new()
        .route(
            "/metrics",
            get(|| async {
                (
                    [(
                        header::CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; version=0.0.4"),
                    )],
                    metrics().render(),
                )
            }),
        )
        .route("/livez", get(livez))
        .route(
            "/readyz",
            get(move || async move {
                if ready.load(std::sync::atomic::Ordering::SeqCst) {
                    (StatusCode::OK, "ok\n").into_response()
                } else {
                    (StatusCode::SERVICE_UNAVAILABLE, "not ready\n").into_response()
                }
            }),
        )
}

/// Records `ac_http_requests_total` and `ac_http_request_duration_seconds` with the route
/// label from [`crate::middleware::normalize_path`] (Go's logging middleware).
pub async fn record_http(req: Request, next: Next) -> Response {
    let method = method_label(req.method()).to_owned();
    let path = crate::middleware::normalize_path(req.uri().path());
    let start = Instant::now();
    let resp = next.run(req).await;
    let m = metrics();
    m.http_requests
        .with_label_values(&[method.as_str(), path, resp.status().as_str()])
        .inc();
    m.http_duration
        .with_label_values(&[method.as_str(), path])
        .observe(start.elapsed().as_secs_f64());
    resp
}

/// Standard methods verbatim, anything else `other`, so callers cannot grow the label set.
#[must_use]
pub fn method_label(m: &axum::http::Method) -> &'static str {
    use axum::http::Method;
    match *m {
        Method::GET => "GET",
        Method::POST => "POST",
        Method::PUT => "PUT",
        Method::DELETE => "DELETE",
        Method::PATCH => "PATCH",
        Method::HEAD => "HEAD",
        Method::OPTIONS => "OPTIONS",
        Method::CONNECT => "CONNECT",
        Method::TRACE => "TRACE",
        _ => "other",
    }
}

/// The worker hook: job metrics plus one [`Drain::complete`] per finished job, which
/// the admission `Retry-After` is computed from.
pub struct WorkerMetrics {
    drain: Arc<Drain>,
}

impl WorkerMetrics {
    #[must_use]
    pub fn new(drain: Arc<Drain>) -> Self {
        Self { drain }
    }
}

impl JobObserver for WorkerMetrics {
    fn job_started(&self) {
        metrics().active_workers.inc();
    }
    fn job_ended(&self) {
        metrics().active_workers.dec();
        self.drain.complete();
    }
    fn job_finished(&self, language: &str, verdict: Verdict, elapsed: Duration) {
        let v = verdict_label(verdict);
        let m = metrics();
        m.jobs_processed.with_label_values(&[v.as_str()]).inc();
        m.job_duration
            .with_label_values(&[language, v.as_str()])
            .observe(elapsed.as_secs_f64());
    }
    fn compiled(&self, language: &str, elapsed: Duration, cache_hit: bool) {
        let m = metrics();
        if cache_hit {
            m.compile_cache_hits.inc();
        } else {
            m.compile_duration
                .with_label_values(&[language])
                .observe(elapsed.as_secs_f64());
        }
    }
    fn stage(&self, stage: &str, elapsed: Duration) {
        metrics()
            .stage_duration
            .with_label_values(&[stage])
            .observe(elapsed.as_secs_f64());
    }
}

/// One reading of `ac_queue_depth`, `ac_queue_age_seconds` and `ac_poison_messages_total`
/// (the poison counter follows the queue's dead-letter count).
pub async fn sample_queues(queue: &Queue) {
    let m = metrics();
    for s in queue.streams() {
        if let Ok(d) = queue.depth(s).await {
            #[allow(clippy::cast_precision_loss)]
            m.queue_depth.with_label_values(&[s]).set(d as f64);
        }
        if let Ok(age) = queue.oldest_age(s).await {
            m.queue_age.with_label_values(&[s]).set(age.as_secs_f64());
        }
    }
    let poisoned = queue.poisoned();
    let seen = m.poison_messages.get();
    if poisoned > seen {
        m.poison_messages.inc_by(poisoned - seen);
    }
}

/// Samples the queues now and then every `every` (Go: `scrapeQueueDepth`, 15 s).
pub fn spawn_queue_sampler(queue: Queue, every: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(every);
        loop {
            tick.tick().await;
            sample_queues(&queue).await;
        }
    })
}

/// Sets `ac_scheduler_budget_used{resource}` to `used / total` (0 when `total` is 0).
#[allow(clippy::cast_precision_loss)]
pub fn set_scheduler_budget_used(resource: &str, used: u64, total: u64) {
    let f = if total == 0 {
        0.0
    } else {
        used as f64 / total as f64
    };
    metrics()
        .scheduler_budget_used
        .with_label_values(&[resource])
        .set(f);
}

/// `/livez`: the process is serving.
pub async fn livez() -> Response {
    (StatusCode::OK, "ok\n").into_response()
}
