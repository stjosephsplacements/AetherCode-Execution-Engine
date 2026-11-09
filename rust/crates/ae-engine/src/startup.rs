//! Startup sequence, background tasks and graceful shutdown (Go: `cmd/stj-exec/main.go`).

use crate::Settings;
use crate::shadow::{self, ShadowBus, ShadowExpiry, ShadowStore, Sink};
use crate::supervisor::Supervisor;
use ae_core::Config;
use ae_gateway::AppState;
use ae_gateway::admission::Drain;
use ae_gateway::auth::AuthLayer;
use ae_gateway::metrics::{WorkerMetrics, set_scheduler_budget_used, spawn_queue_sampler};
use ae_queue::{Delivery, Queue};
use ae_sandbox::{Cmd, CmdFile, GoJudge, REFRESH_INTERVAL, Registry, STATUS_ACCEPTED, Sandbox};
use ae_worker::{Bus, Store, Worker, WorkerOptions};
use futures_util::future::join_all;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// How long shutdown waits for in-flight jobs (Go: `workerDrainTimeout`).
pub const DRAIN_GRACE: Duration = Duration::from_secs(120);
/// How long open HTTP connections (SSE) get to finish once the workers are done.
const HTTP_CLOSE: Duration = Duration::from_secs(5);
/// Go's `scrapeQueueDepth` ticker.
const QUEUE_SAMPLE: Duration = Duration::from_secs(15);
const BUDGET_SAMPLE: Duration = Duration::from_secs(5);
/// Slack on top of `AC_JOB_TIMEOUT` for one go-judge request.
const JUDGE_SLACK: Duration = Duration::from_secs(30);
/// The pre-warm command: present in every go-judge image, needs no toolchain.
pub const PREWARM_ARGS: [&str; 3] = ["/usr/bin/test", "-x", "/usr/bin/test"];

/// Overall timeout of one go-judge request: a job never waits longer than its own
/// timeout plus slack for a single sandbox call.
#[must_use]
pub fn judge_request_timeout(cfg: &Config) -> Duration {
    cfg.job_timeout + JUDGE_SLACK
}

/// `redis://` URL for `AC_REDIS_ADDR` with the percent-encoded password. Never log it.
fn redis_url(cfg: &Config) -> String {
    if cfg.redis_password.is_empty() {
        return format!("redis://{}/", cfg.redis_addr);
    }
    let mut pw = String::new();
    for b in cfg.redis_password.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            pw.push(char::from(b));
        } else {
            pw.push_str(&format!("%{b:02X}"));
        }
    }
    format!("redis://:{pw}@{}/", cfg.redis_addr)
}

/// First SIGTERM/SIGINT cancels `token`; a second one exits at once with status 1.
fn spawn_signals(token: CancellationToken) -> Result<JoinHandle<()>, String> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).map_err(|e| format!("signals: {e}"))?;
    let mut int = signal(SignalKind::interrupt()).map_err(|e| format!("signals: {e}"))?;
    Ok(tokio::spawn(async move {
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
        token.cancel();
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
        tracing::error!("second signal: forcing exit");
        std::process::exit(1);
    }))
}

fn prewarm_cmd() -> Cmd {
    Cmd {
        args: PREWARM_ARGS.iter().map(|s| (*s).to_owned()).collect(),
        env: vec!["PATH=/usr/bin:/bin".into()],
        files: vec![
            CmdFile::stdin(""),
            CmdFile::collector("stdout", 0),
            CmdFile::collector("stderr", 0),
        ],
        cpu_limit: 1_000_000_000,
        clock_limit: 2_000_000_000,
        memory_limit: 16 << 20,
        proc_limit: 1,
        ..Cmd::default()
    }
}

/// Runs `n` trivial sandbox commands at once so go-judge has `n` environments and the
/// client `n` pooled connections before the first job. Failures only log.
pub async fn prewarm(sb: &dyn Sandbox, n: usize) {
    if n == 0 {
        return;
    }
    let start = Instant::now();
    let results = join_all((0..n).map(|_| sb.run(vec![prewarm_cmd()]))).await;
    let ok = results
        .iter()
        .filter(|r| {
            r.as_ref()
                .is_ok_and(|v| v.first().is_some_and(|r| r.status == STATUS_ACCEPTED))
        })
        .count();
    if ok < n {
        tracing::warn!(slots = n, ok, "sandbox pre-warm partly failed");
    }
    tracing::info!(slots = n, ok, elapsed = ?start.elapsed(), "sandbox pre-warmed");
}

/// Binds the metrics and probe listener (`AC_DEBUG_ADDR:AC_DEBUG_PORT`, loopback by
/// default). Fatal on failure.
async fn bind_debug(cfg: &Config) -> Result<tokio::net::TcpListener, String> {
    let addr = SocketAddr::new(cfg.debug_addr, cfg.debug_port);
    if !addr.ip().is_loopback() {
        tracing::warn!(
            addr = %addr,
            "debug listener (/metrics, /livez, /readyz) is exposed beyond loopback; restrict it with a NetworkPolicy or firewall"
        );
    }
    let l = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("debug/metrics listen {addr}: {e}"))?;
    tracing::info!(addr = %addr, "debug/metrics listening");
    Ok(l)
}

/// Sets `ac_scheduler_budget_used` from the worker's scheduler every 5 s.
async fn budget_gauge(worker: Worker) -> Result<(), String> {
    let mut tick = tokio::time::interval(BUDGET_SAMPLE);
    loop {
        tick.tick().await;
        let (total, free) = (worker.scheduler().budget(), worker.scheduler().available());
        set_scheduler_budget_used(
            "cpu",
            total.cpu_millis.saturating_sub(free.cpu_millis),
            total.cpu_millis,
        );
        set_scheduler_budget_used(
            "memory",
            total.mem_bytes.saturating_sub(free.mem_bytes),
            total.mem_bytes,
        );
    }
}

/// Processes whatever is left in the legacy single stream (`AC_STREAM_NAME`). Stops
/// reading on shutdown; jobs already read are finished.
fn spawn_legacy_drain(queue: Queue, worker: Worker, token: CancellationToken) -> JoinHandle<()> {
    let (tx, mut rx) = mpsc::channel::<Delivery>(10);
    tokio::spawn(async move {
        let feed = async move {
            tokio::select! {
                r = queue.drain_legacy(&tx) => match r {
                    Ok(0) => {}
                    Ok(n) => tracing::info!(jobs = n, "legacy stream drained"),
                    Err(e) => tracing::warn!(error = %e, "legacy stream drain failed"),
                },
                () = token.cancelled() => {}
            }
        };
        let work = async move {
            while let Some(d) = rx.recv().await {
                let job_id = d.job_id.clone();
                if let Err(e) = worker.process(d).await {
                    tracing::error!(job_id, error = %e, "queue: handler error");
                }
            }
        };
        tokio::join!(feed, work);
    })
}

/// `fut`, or `None` once `token` is cancelled (a signal during startup).
async fn or_stop<T>(token: &CancellationToken, fut: impl Future<Output = T>) -> Option<T> {
    tokio::select! {
        v = fut => Some(v),
        () = token.cancelled() => None,
    }
}

/// Runs the engine until shutdown. `Err` is fatal (non-zero exit).
///
/// A signal during startup stops it at the current step and exits 0: nothing has been
/// consumed yet, so there is nothing to drain.
///
/// # Errors
/// Any startup step fails (database, migrations, Redis, sandbox unreachable, auth
/// configuration, binding a listener), or a supervised background task exits.
#[allow(clippy::too_many_lines)]
pub async fn run(s: Settings) -> Result<(), String> {
    let Settings { cfg, role, shadow } = s;
    tracing::info!(
        config = ?cfg,
        role = ?role,
        shadow,
        workers = cfg.effective_workers(),
        job_timeout = ?cfg.job_timeout,
        log_format = %cfg.log_format,
        "aethercode-exec starting"
    );
    // `token`: the signal; stops startup, readiness and consumption. `http_stop`: closes
    // the listeners, only after the workers have drained.
    let token = CancellationToken::new();
    let http_stop = CancellationToken::new();
    let signals = spawn_signals(token.clone())?;
    let interrupted = || {
        tracing::info!("shutdown requested during startup");
        Ok(())
    };

    // Postgres. A shadow engine leaves the schema to the authoritative one.
    let Some(db) = or_stop(&token, ae_db::connect(&cfg)).await else {
        return interrupted();
    };
    let db = db.map_err(|e| format!("database connection failed: {e}"))?;
    tracing::info!("database connected");
    if shadow {
        tracing::info!("shadow mode: migrations left to the authoritative engine");
    } else {
        let Some(m) = or_stop(&token, db.migrate()).await else {
            return interrupted();
        };
        m.map_err(|e| format!("migration failed: {e}"))?;
        tracing::info!("migrations applied");
    }

    // Redis.
    let client = redis::Client::open(redis_url(&cfg)).map_err(|e| format!("redis client: {e}"))?;
    let Some(redis) = or_stop(&token, client.get_connection_manager()).await else {
        return interrupted();
    };
    let redis = redis.map_err(|e| format!("redis connection failed ({}): {e}", cfg.redis_addr))?;
    let (queue, sink) = if shadow {
        let sink = Sink::new(redis.clone());
        let mut qcfg = cfg.clone();
        qcfg.consumer_group = shadow::group(&cfg.consumer_group);
        let q = Queue::connect(&qcfg, Arc::new(ShadowExpiry(sink.clone())))
            .await
            .map_err(|e| format!("redis stream group creation failed: {e}"))?
            .with_dead_letter_stream(shadow::DLQ_STREAM);
        (q, Some(sink))
    } else {
        let q = Queue::connect(&cfg, Arc::new(db.clone()))
            .await
            .map_err(|e| format!("redis stream group creation failed: {e}"))?;
        (q, None)
    };
    tracing::info!(
        addr = %cfg.redis_addr,
        streams = ?queue.streams(),
        group = if shadow { shadow::group(&cfg.consumer_group) } else { cfg.consumer_group.clone() },
        "redis connected"
    );
    // The streams are shared with the authoritative engine in shadow mode: never trim them.
    if !shadow {
        queue
            .trim_stale(cfg.job_timeout)
            .await
            .map_err(|e| format!("purge stale queue entries: {e}"))?;
    }

    // Sandbox health and language self-tests.
    let judge = GoJudge::with_request_timeout(&cfg, judge_request_timeout(&cfg))
        .map_err(|e| e.to_string())?;
    let sandbox: Arc<dyn Sandbox> = Arc::new(judge);
    let Some(registry) = or_stop(&token, Registry::load(&cfg, sandbox.as_ref())).await else {
        return interrupted();
    };
    let registry = Arc::new(registry.map_err(|e| match e {
        ae_sandbox::Error::SandboxUnreachable => {
            format!("go-judge unreachable at {}", cfg.judge_url)
        }
        e => format!("language registry: {e}"),
    })?);
    let (up, down): (Vec<_>, Vec<_>) = registry.catalogue().into_iter().partition(|l| l.available);
    tracing::info!(
        available = ?up.iter().map(|l| l.id.as_str()).collect::<Vec<_>>(),
        unavailable = ?down.iter().map(|l| l.id.as_str()).collect::<Vec<_>>(),
        "language self-tests done"
    );

    let mut sup = Supervisor::default();
    sup.adopt(
        "language refresher",
        registry
            .clone()
            .spawn_refresher(sandbox.clone(), REFRESH_INTERVAL),
    );
    sup.adopt(
        "queue sampler",
        spawn_queue_sampler(queue.clone(), QUEUE_SAMPLE),
    );
    // Shared by the debug listener's /readyz and, when there is one, the gateway's.
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let debug = bind_debug(&cfg).await?;
    let stop = http_stop.clone().cancelled_owned();
    let debug_ready = ready.clone();
    let debug = tokio::spawn(async move {
        axum::serve(debug, ae_gateway::metrics_router(debug_ready))
            .with_graceful_shutdown(stop)
            .await
            .map_err(|e| format!("debug server: {e}"))
    });

    // Public HTTP: up for probes now, ready only after pre-warm.
    let mut http = None;
    let state = if role.gateway() {
        let auth = AuthLayer::from_config(&cfg, Arc::new(db.clone()))
            .await
            .map_err(|e| format!("auth middleware init failed: {e}"))?;
        let mut state = AppState::new(
            &cfg,
            db.clone(),
            queue.clone(),
            sandbox.clone(),
            registry.clone(),
            auth,
            redis.clone(),
        );
        state.ready = ready.clone();
        let listener = tokio::net::TcpListener::bind(&cfg.gateway_addr)
            .await
            .map_err(|e| format!("gateway listen {}: {e}", cfg.gateway_addr))?;
        tracing::info!(addr = %cfg.gateway_addr, "gateway listening");
        let app = ae_gateway::router(state.clone());
        let stop = http_stop.clone().cancelled_owned();
        http = Some(tokio::spawn(async move {
            ae_gateway::serve(listener, app, stop)
                .await
                .map_err(|e| format!("gateway: {e}"))
        }));
        Some(state)
    } else {
        None
    };

    if or_stop(&token, prewarm(sandbox.as_ref(), cfg.prewarm))
        .await
        .is_none()
    {
        http_stop.cancel();
        sup.shutdown().await;
        return interrupted();
    }

    // Workers.
    let mut draining: Vec<JoinHandle<()>> = Vec::new();
    if role.worker() {
        let (store, bus): (Arc<dyn Store>, Arc<dyn Bus>) = match sink {
            Some(sink) => (
                Arc::new(ShadowStore {
                    db: db.clone(),
                    sink,
                }),
                Arc::new(ShadowBus(queue.clone())),
            ),
            None => (Arc::new(db.clone()), Arc::new(queue.clone())),
        };
        let mut opts = WorkerOptions::from_config(&cfg);
        opts.shutdown_grace = DRAIN_GRACE;
        opts.observer = match &state {
            Some(st) => st.worker_observer(),
            None => {
                ae_core::observe::Observer(Arc::new(WorkerMetrics::new(Arc::new(Drain::new()))))
            }
        };
        let worker = Worker::new(store, bus, sandbox.clone(), registry.clone(), opts);
        sup.spawn("scheduler budget gauge", budget_gauge(worker.clone()));
        if !shadow {
            sup.adopt("orphan reaper", queue.spawn_reaper(db.clone()));
            draining.push(spawn_legacy_drain(
                queue.clone(),
                worker.clone(),
                token.clone(),
            ));
        }
        draining.push(tokio::spawn(worker.run(token.clone())));
    }

    ready.store(true, std::sync::atomic::Ordering::SeqCst);
    tracing::info!(role = ?role, shadow, "engine ready");

    // Runs until a signal, or until a listener or supervised task dies. A listener that
    // finished is taken out so it is not awaited twice.
    let mut http = http;
    let mut debug = Some(debug);
    let failure = tokio::select! {
        () = token.cancelled() => None,
        (name, why) = sup.failure() => Some(format!("{name}: {why}")),
        r = finished(&mut debug) => Some(format!("metrics server: {r}")),
        r = finished(&mut http) => Some(format!("public server: {r}")),
    };
    if let Some(f) = &failure {
        tracing::error!(component = %f, "background task exited unexpectedly; shutting down");
    }
    token.cancel();
    tracing::info!("shutting down");
    ready.store(false, std::sync::atomic::Ordering::SeqCst);
    // Workers stop reading at once and get the grace period for in-flight jobs; HTTP keeps
    // serving (not ready) meanwhile so clients can still read results.
    tracing::info!("waiting for in-flight jobs to complete");
    match tokio::time::timeout(DRAIN_GRACE + HTTP_CLOSE, join_all(draining)).await {
        Ok(_) => tracing::info!("all workers stopped"),
        Err(_) => tracing::warn!("worker drain timeout, proceeding with shutdown"),
    }
    // Now close the listeners; open connections (SSE) get a short window.
    http_stop.cancel();
    for h in [http, debug].into_iter().flatten() {
        let mut h = h;
        if tokio::time::timeout(HTTP_CLOSE, &mut h).await.is_err() {
            h.abort();
        }
    }
    sup.shutdown().await;
    signals.abort();
    tracing::info!("shutdown complete");
    failure.map_or(Ok(()), |f| Err(format!("background task failed: {f}")))
}

/// Waits for a server task to finish and takes it out of `slot`; pending when empty.
async fn finished(slot: &mut Option<JoinHandle<Result<(), String>>>) -> String {
    let Some(h) = slot.as_mut() else {
        return std::future::pending().await;
    };
    let r = h.await;
    *slot = None;
    match r {
        Ok(Ok(())) => "exited".to_owned(),
        Ok(Err(e)) => e,
        Err(e) => e.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn redis_url_encodes_password() {
        let m: HashMap<String, String> = [
            ("AC_REDIS_ADDR".to_owned(), "h:1".to_owned()),
            ("AC_REDIS_PASSWORD".to_owned(), "p@ss:w/rd".to_owned()),
        ]
        .into();
        let cfg = Config::from_map(&m).unwrap();
        assert_eq!(redis_url(&cfg), "redis://:p%40ss%3Aw%2Frd@h:1/");
        let cfg = Config::from_map(&HashMap::new()).unwrap();
        assert_eq!(redis_url(&cfg), "redis://127.0.0.1:6379/");
    }

    #[test]
    fn judge_timeout_follows_job_timeout() {
        let m: HashMap<String, String> = [("AC_JOB_TIMEOUT".to_owned(), "10s".to_owned())].into();
        let cfg = Config::from_map(&m).unwrap();
        assert_eq!(judge_request_timeout(&cfg), Duration::from_secs(40));
    }
}
