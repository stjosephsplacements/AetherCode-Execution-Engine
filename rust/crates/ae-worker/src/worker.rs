//! The job loop and one job's lifecycle (Go: `worker.go`).

use crate::cache::{CacheKey, CachedFile, CompileCache};
use crate::checker::{Program, compile, file_name, prepare_program};
use crate::runner::{RunSpec, job_budget, job_cost, run_tests, verdict_results_for};
use crate::scheduler::{Cost, Scheduler};
use crate::webhook::{WebhookClient, WebhookPayload};
use crate::{AbortOnDrop, Bus, Error, Result, Store};
use ae_core::Config;
use ae_core::eventlog::{Event, EventType};
use ae_core::model::{Status, Verdict};
use ae_core::observe::Observer;
use ae_db::truncate;
use ae_queue::Delivery;
use ae_sandbox::{Language, Registry, RunLimits, Sandbox};
use futures_util::FutureExt;
use serde_json::json;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const HEARTBEAT: Duration = Duration::from_secs(30);
const PANIC_BACKOFF: Duration = Duration::from_secs(1);
const MAX_STDERR_BYTES: usize = 4096;
/// Streams the queue reads from (submit, run, bulk); the channel holds at least one per stream.
const MIN_CHANNEL: usize = 3;

#[derive(Debug, Clone)]
pub struct WorkerOptions {
    /// Minimum time a job may take (`AC_JOB_TIMEOUT`); raised to the job's
    /// worst-case budget for large test sets.
    pub job_timeout: Duration,
    /// Jobs processed at once; also the delivery channel capacity (at least one per stream).
    pub max_inflight: usize,
    /// How long shutdown waits for in-flight jobs.
    pub shutdown_grace: Duration,
    /// Node budget the scheduler admits jobs against.
    pub budget: Cost,
    pub cache_ttl: Duration,
    pub cache_max_bytes: u64,
    pub webhook: WebhookClient,
    /// Job progress hook (metrics and the gateway's drain rate). The engine passes
    /// `ae_gateway::AppState::worker_observer()`; the default records nothing.
    pub observer: Observer,
}

impl WorkerOptions {
    pub fn from_config(cfg: &Config) -> Self {
        Self {
            job_timeout: cfg.job_timeout,
            max_inflight: (cfg.effective_workers() * 2).max(MIN_CHANNEL),
            shutdown_grace: Duration::from_secs(120),
            budget: Scheduler::budget_for(cfg),
            cache_ttl: cfg.compile_cache_ttl,
            cache_max_bytes: cfg.compile_cache_max_mb.saturating_mul(1 << 20),
            webhook: WebhookClient::new(),
            observer: Observer::default(),
        }
    }
}

struct Inner {
    store: Arc<dyn Store>,
    bus: Arc<dyn Bus>,
    sandbox: Arc<dyn Sandbox>,
    registry: Arc<Registry>,
    cache: CompileCache,
    scheduler: Scheduler,
    opts: WorkerOptions,
}

/// Cheap to clone; clones share the cache and the scheduler.
#[derive(Clone)]
pub struct Worker {
    inner: Arc<Inner>,
}

impl Worker {
    pub fn new(
        store: Arc<dyn Store>,
        bus: Arc<dyn Bus>,
        sandbox: Arc<dyn Sandbox>,
        registry: Arc<Registry>,
        opts: WorkerOptions,
    ) -> Self {
        let cache = CompileCache::new(sandbox.clone(), opts.cache_ttl, opts.cache_max_bytes);
        let scheduler = Scheduler::new(opts.budget);
        Self {
            inner: Arc::new(Inner {
                store,
                bus,
                sandbox,
                registry,
                cache,
                scheduler,
                opts,
            }),
        }
    }

    pub fn cache(&self) -> &CompileCache {
        &self.inner.cache
    }

    pub fn scheduler(&self) -> &Scheduler {
        &self.inner.scheduler
    }

    /// Consumes deliveries until `shutdown`, then stops taking new ones and waits
    /// up to the shutdown grace for in-flight jobs. A job that panics stays
    /// unacked (the reclaimer redelivers it) and its slot is freed after a 1 s backoff.
    pub async fn run(self, shutdown: CancellationToken) {
        let cap = self.inner.opts.max_inflight.max(1);
        let (tx, mut rx) = mpsc::channel(cap.max(MIN_CHANNEL));
        let bus = self.inner.bus.clone();
        let consumer = AbortOnDrop(tokio::spawn(async move { bus.consume(tx).await }));
        let sweeper = (!self.inner.cache.ttl().is_zero()).then(|| {
            let cache = self.inner.cache.clone();
            AbortOnDrop(tokio::spawn(async move {
                let mut tick = tokio::time::interval(cache.ttl());
                tick.tick().await;
                loop {
                    tick.tick().await;
                    cache.sweep();
                }
            }))
        });
        let slots = Arc::new(Semaphore::new(cap));
        let mut jobs = JoinSet::new();
        tracing::info!(slots = cap, budget = ?self.inner.scheduler.budget(), "workers started");
        loop {
            let slot = tokio::select! {
                () = shutdown.cancelled() => break,
                s = slots.clone().acquire_owned() => match s {
                    Ok(s) => s,
                    Err(_) => break,
                },
            };
            let d = tokio::select! {
                () = shutdown.cancelled() => break,
                d = rx.recv() => match d {
                    Some(d) => d,
                    None => break,
                },
            };
            let w = self.clone();
            jobs.spawn(async move {
                let job_id = d.job_id.clone();
                match AssertUnwindSafe(w.process(d)).catch_unwind().await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => tracing::error!(job_id, error = %e, "queue: handler error"),
                    Err(_) => {
                        tracing::error!(job_id, "worker panic recovered; restarting after backoff");
                        tokio::time::sleep(PANIC_BACKOFF).await;
                    }
                }
                drop(slot);
            });
            while jobs.try_join_next().is_some() {}
        }
        // Stop reading; anything still buffered stays pending for the reclaimer.
        drop(consumer);
        drop(sweeper);
        drop(rx);
        let in_flight = jobs.len();
        if in_flight > 0 {
            tracing::info!(in_flight, "waiting for in-flight jobs");
        }
        let drained = tokio::time::timeout(self.inner.opts.shutdown_grace, async {
            while jobs.join_next().await.is_some() {}
        })
        .await;
        if drained.is_err() {
            tracing::warn!(
                abandoned = jobs.len(),
                "shutdown grace expired; abandoning jobs"
            );
            jobs.shutdown().await;
        }
    }

    /// Processes one delivery and acks it once the outcome is recorded. On error
    /// the message is left pending for redelivery.
    ///
    /// Order: one database write (`finish_with_results` / `fail_submission`),
    /// then the VERDICT event, then the webhook marker and dispatch, then XACK.
    /// Redelivery is idempotent: a terminal row makes the write return false and
    /// the job is only acked. Known trade-off: a crash after the database write
    /// but before the VERDICT publish or `mark_webhook_delivered` loses that
    /// event and webhook, since redelivery sees the terminal row and only acks.
    /// The verdict itself is never lost (clients can read it from the API).
    ///
    /// # Errors
    /// A database, queue or other failure before the outcome was recorded, or the ack itself.
    pub async fn process(&self, d: Delivery) -> Result<()> {
        self.handle(&d.job_id).await?;
        self.inner.bus.ack(&d).await
    }

    async fn handle(&self, id: &str) -> Result<()> {
        let inner = &*self.inner;
        let obs = &inner.opts.observer;
        let started = Instant::now();
        // Test data (Redis) and the claim (Postgres) are independent; overlap them.
        let (spec, claim) = tokio::join!(inner.bus.get_tests(id), self.claim(id));
        let Some(sub) = claim? else {
            return Ok(());
        };
        obs.stage("claim", started.elapsed());
        let _active = Active::new(obs);
        let lang_id = sub.language.as_str();
        let _heartbeat = self.heartbeat(id);

        let spec = match spec {
            Ok(s) => s,
            Err(e) => {
                return self
                    .fail(
                        id,
                        lang_id,
                        started,
                        Verdict::InternalError,
                        &format!("load tests: {e}"),
                    )
                    .await;
            }
        };
        if spec.set.tests.is_empty() {
            return self
                .fail(
                    id,
                    lang_id,
                    started,
                    Verdict::InternalError,
                    "no tests found",
                )
                .await;
        }
        let Some(lang) = inner.registry.get(&sub.language).cloned() else {
            return if inner.registry.languages().get(&sub.language).is_some() {
                let msg = format!("language unavailable: {}", sub.language);
                self.fail(id, lang_id, started, Verdict::CompilationError, &msg)
                    .await
            } else {
                let msg = format!("unknown language: {}", sub.language);
                self.fail(id, lang_id, started, Verdict::InternalError, &msg)
                    .await
            };
        };
        let set = &spec.set;
        let limits = RunLimits {
            cpu: spec.limits.cpu_ns,
            memory: spec.limits.mem_bytes,
            stack: spec.limits.stack_bytes,
            procs: spec.limits.procs,
            args: spec.limits.args.clone(),
        };

        let _permit = inner
            .scheduler
            .acquire(job_cost(&lang, &limits, set.tests.len()))
            .await;
        let budget = job_budget(&lang, &limits, set.tests.len()).max(inner.opts.job_timeout);
        let deadline = Instant::now() + budget;

        let mut binary: Option<CachedFile> = None;
        if lang.is_compiled() {
            if !inner.store.set_status(id, Status::Compiling).await? {
                tracing::info!(job_id = id, "submission became terminal before compile");
                return Ok(());
            }
            self.publish(id, EventType::Compiling, None).await;
            let version = inner
                .registry
                .toolchain_version(&lang.id)
                .unwrap_or_default();
            let key = CacheKey::for_compile(
                &sub.source_code,
                &lang.id,
                &set.files,
                &set.compiler_flags,
                &version,
            );
            let build = compile(
                inner.sandbox.clone(),
                &lang,
                &sub.source_code,
                &set.files,
                &set.compiler_flags,
            );
            let compile_start = Instant::now();
            let compiled =
                tokio::time::timeout_at(deadline, inner.cache.get_or_compile(key, build))
                    .await
                    .unwrap_or_else(|_| Err(Error::Other("compile: job deadline exceeded".into())));
            match compiled {
                Ok((f, hit)) => {
                    obs.compiled(lang_id, compile_start.elapsed(), hit);
                    tracing::info!(job_id = id, file_id = %f.id().0, cache_hit = hit, "compiled");
                    binary = Some(f);
                }
                Err(Error::Compile(stderr)) => {
                    return self
                        .fail(id, lang_id, started, Verdict::CompilationError, &stderr)
                        .await;
                }
                Err(e) => {
                    return self
                        .fail(id, lang_id, started, Verdict::InternalError, &e.to_string())
                        .await;
                }
            }
            if !inner.store.set_status(id, Status::Running).await? {
                tracing::info!(job_id = id, "submission became terminal before test run");
                return Ok(());
            }
        }
        self.publish(id, EventType::Running, None).await;

        // Interpreted source shared by many tests is uploaded once.
        if binary.is_none()
            && set.tests.len() > 1
            && !lang.placement.stdin_is_source
            && let Some(src) = lang.placement.source.as_deref()
        {
            match inner
                .sandbox
                .cache_file(file_name(src), sub.source_code.as_bytes())
                .await
            {
                Ok(fid) => binary = Some(CachedFile::owned(fid, inner.sandbox.clone())),
                Err(e) => tracing::warn!(job_id = id, error = %e, "source upload failed; inlining"),
            }
        }

        let run_start = Instant::now();
        let outcome = match self
            .prepare_and_run(
                id,
                &lang,
                &sub.source_code,
                binary.as_ref(),
                &limits,
                &spec,
                deadline,
            )
            .await
        {
            Ok(o) => o,
            Err(e) => {
                return self
                    .fail(
                        id,
                        lang_id,
                        started,
                        Verdict::InternalError,
                        &format!("batch run: {e}"),
                    )
                    .await;
            }
        };
        obs.stage("run", run_start.elapsed());
        drop(binary);

        let results = &outcome.results;
        let persist_start = Instant::now();
        let persisted = inner
            .store
            .finish_with_results(id, outcome.verdict, results)
            .await?;
        obs.stage("persist", persist_start.elapsed());
        if !persisted {
            tracing::warn!(
                job_id = id,
                "duplicate execution detected, verdict already persisted"
            );
            return Ok(());
        }
        let passed = results
            .iter()
            .filter(|r| r.verdict == Verdict::Accepted)
            .count();
        let cpu: i64 = results.iter().map(|r| r.cpu_time_ns).sum();
        let peak = results.iter().map(|r| r.memory_bytes).max().unwrap_or(0);
        let event_results = verdict_results_for(results, &set.output_mode);
        let (applied_cpu, applied_mem) = lang.effective(&limits);
        let publish_start = Instant::now();
        self.publish(
            id,
            EventType::Verdict,
            Some(json!({
                "verdict": outcome.verdict,
                "tests_passed": passed,
                "test_count": results.len(),
                "cpu_time_ns": cpu,
                "memory_bytes": peak,
                "applied_cpu_limit_ns": applied_cpu,
                "applied_memory_limit_bytes": applied_mem,
                "results": event_results,
            })),
        )
        .await;
        obs.stage("publish", publish_start.elapsed());
        obs.job_finished(lang_id, outcome.verdict, started.elapsed());

        if !set.callback_url.is_empty() {
            match inner.store.mark_webhook_delivered(id).await {
                Ok(true) => {
                    let payload = WebhookPayload {
                        submission_id: id.to_owned(),
                        verdict: verdict_name(outcome.verdict),
                        tests_passed: i64::try_from(passed).unwrap_or(i64::MAX),
                        test_count: i64::try_from(results.len()).unwrap_or(i64::MAX),
                        cpu_time_ns: cpu,
                        memory_bytes: peak,
                        run_time_ns: 0,
                        timestamp: chrono::Utc::now()
                            .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
                        results: event_results,
                    };
                    let (client, url, secret) = (
                        inner.opts.webhook.clone(),
                        set.callback_url.clone(),
                        set.callback_secret.clone(),
                    );
                    tokio::spawn(async move { client.dispatch(&url, &secret, &payload).await });
                }
                Ok(false) => tracing::info!(job_id = id, "webhook already dispatched"),
                Err(e) => {
                    tracing::warn!(job_id = id, error = %e, "webhook marker failed; not dispatching")
                }
            }
        }
        tracing::info!(job_id = id, verdict = ?outcome.verdict, passed, total = results.len(), "completed");
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_and_run(
        &self,
        id: &str,
        lang: &Language,
        source: &str,
        binary: Option<&CachedFile>,
        limits: &RunLimits,
        spec: &ae_queue::JobSpec,
        deadline: Instant,
    ) -> Result<crate::runner::Outcome> {
        let inner = &*self.inner;
        let set = &spec.set;
        let custom = if set.checker_code.is_empty() {
            None
        } else {
            Some(
                self.program("checker", &set.checker_language, &set.checker_code)
                    .await
                    .map_err(|e| Error::Other(format!("custom checker setup: {e}")))?,
            )
        };
        let interactor = if set.interactor_code.is_empty() {
            None
        } else {
            Some(
                self.program("interactor", &set.interactor_language, &set.interactor_code)
                    .await
                    .map_err(|e| Error::Other(format!("interactor setup: {e}")))?,
            )
        };
        let run = RunSpec {
            sandbox: inner.sandbox.as_ref(),
            bus: inner.bus.as_ref(),
            job_id: id,
            lang,
            source,
            file_id: binary.map(CachedFile::id),
            limits,
            files: &set.files,
            caps: inner.registry.output_caps(),
            checker: spec.checker,
            custom: custom.as_ref(),
            interactor: interactor.as_ref(),
            deadline,
        };
        run_tests(&run, &set.tests).await
    }

    async fn program(&self, kind: &str, lang: &str, code: &str) -> Result<Program> {
        let inner = &*self.inner;
        prepare_program(
            &inner.cache,
            &inner.sandbox,
            &inner.registry,
            kind,
            lang,
            code,
        )
        .await
    }

    /// Loads the submission and moves it to running so the orphan reaper leaves it
    /// alone. `None` when it is already finished.
    async fn claim(&self, id: &str) -> Result<Option<ae_core::model::Submission>> {
        let store = &self.inner.store;
        let sub = store
            .get_submission(id)
            .await?
            .ok_or_else(|| Error::Other(format!("worker: submission {id} not found")))?;
        if matches!(sub.status, Status::Completed | Status::Failed) {
            tracing::info!(job_id = id, status = ?sub.status, "skipping already-finished submission");
            return Ok(None);
        }
        if !store.set_status(id, Status::Running).await? {
            tracing::info!(job_id = id, "submission became terminal before claim");
            return Ok(None);
        }
        Ok(Some(sub))
    }

    /// Bumps `updated_at` every 30 s while the job runs.
    fn heartbeat(&self, id: &str) -> AbortOnDrop<()> {
        let (store, id) = (self.inner.store.clone(), id.to_owned());
        AbortOnDrop(tokio::spawn(async move {
            let mut tick = tokio::time::interval(HEARTBEAT);
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(e) = store.touch(&id).await {
                    tracing::warn!(job_id = id, error = %e, "heartbeat failed");
                }
            }
        }))
    }

    async fn publish(&self, id: &str, kind: EventType, data: Option<serde_json::Value>) {
        if let Err(e) = self.inner.bus.publish(&Event::new(id, kind, data)).await {
            tracing::warn!(job_id = id, event = kind.as_str(), error = %e, "failed to publish event");
        }
    }

    /// Records a failed job and, when this call recorded it, publishes its VERDICT.
    async fn fail(
        &self,
        id: &str,
        language: &str,
        started: Instant,
        verdict: Verdict,
        stderr: &str,
    ) -> Result<()> {
        let stderr = truncate(stderr, MAX_STDERR_BYTES);
        if !self
            .inner
            .store
            .fail_submission(id, verdict, stderr)
            .await?
        {
            tracing::warn!(
                job_id = id,
                "duplicate execution detected, verdict already persisted"
            );
            return Ok(());
        }
        self.publish(
            id,
            EventType::Verdict,
            Some(json!({ "verdict": verdict, "compile_stderr": stderr })),
        )
        .await;
        self.inner
            .opts
            .observer
            .job_finished(language, verdict, started.elapsed());
        tracing::info!(job_id = id, ?verdict, "submission failed");
        Ok(())
    }
}

/// Marks a claimed job active for its lifetime; the drop reports `job_ended` even
/// when the job errors or panics (Go: `defer ActiveWorkers.Dec(); defer JobsDrain.Complete()`).
struct Active<'a>(&'a Observer);

impl<'a> Active<'a> {
    fn new(obs: &'a Observer) -> Self {
        obs.job_started();
        Self(obs)
    }
}

impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.job_ended();
    }
}

fn verdict_name(v: Verdict) -> String {
    serde_json::to_value(v)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}
