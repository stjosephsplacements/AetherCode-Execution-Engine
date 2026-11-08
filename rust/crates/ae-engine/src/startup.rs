//! Startup sequence, background tasks and graceful shutdown (Go: `cmd/stj-exec/main.go`).

use crate::Settings;
use crate::shadow::{self, ShadowBus, ShadowExpiry, ShadowStore, Sink};
use ae_core::Config;
use ae_gateway::AppState;
use ae_gateway::admission::Drain;
use ae_gateway::auth::AuthLayer;
use ae_gateway::metrics::{WorkerMetrics, set_scheduler_budget_used, spawn_queue_sampler};
use ae_queue::{Delivery, Queue};
use ae_sandbox::{Cmd, CmdFile, GoJudge, REFRESH_INTERVAL, Registry, STATUS_ACCEPTED, Sandbox};
use ae_worker::{Bus, Store, Worker, WorkerOptions};
use futures_util::future::join_all;
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
/// Port of the loopback-only metrics listener; `AC_DEBUG_PORT` overrides it.
const DEBUG_PORT: u16 = 6060;
/// The pre-warm command: present in every go-judge image, needs no toolchain.
pub const PREWARM_ARGS: [&str; 3] = ["/usr/bin/test", "-x", "/usr/bin/test"];

/// Overall timeout of one go-judge request: a job never waits longer than its own
/// timeout plus slack for a single sandbox call.
#[must_use]
pub fn judge_request_timeout(cfg: &Config) -> Duration {
    cfg.job_timeout + JUDGE_SLACK
}

fn debug_addr() -> Result<SocketAddr, String> {
    let port = match std::env::var("AC_DEBUG_PORT") {
        Ok(v) if !v.is_empty() => v
            .parse()
            .map_err(|_| format!("AC_DEBUG_PORT must be a port number, got {v:?}"))?,
        _ => DEBUG_PORT,
    };
    Ok(SocketAddr::from(([127, 0, 0, 1], port)))
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

/// Serves `ae_gateway::metrics_router` on the loopback address only.
async fn spawn_debug(token: &CancellationToken) -> Result<Option<JoinHandle<()>>, String> {
    let addr = debug_addr()?;
    match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => {
            tracing::info!(addr = %addr, "debug/metrics listening");
            let stop = token.clone().cancelled_owned();
            Ok(Some(tokio::spawn(async move {
                if let Err(e) = axum::serve(l, ae_gateway::metrics_router())
                    .with_graceful_shutdown(stop)
                    .await
                {
                    tracing::error!(error = %e, "debug server error");
                }
            })))
        }
        Err(e) => {
            tracing::error!(addr = %addr, error = %e, "debug server error");
            Ok(None)
        }
    }
}

/// Sets `ac_scheduler_budget_used` from the worker's scheduler every 5 s.
fn spawn_budget_gauge(worker: Worker) -> JoinHandle<()> {
    tokio::spawn(async move {
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
    })
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

/// Runs the engine until shutdown. `Err` is fatal (non-zero exit).
///
/// # Errors
/// Any startup step fails: database, migrations, Redis, sandbox unreachable, auth
/// configuration or binding the public listener.
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
    let token = CancellationToken::new();
    let signals = spawn_signals(token.clone())?;

    // Postgres. A shadow engine leaves the schema to the authoritative one.
    let db = ae_db::connect(&cfg)
        .await
        .map_err(|e| format!("database connection failed: {e}"))?;
    tracing::info!("database connected");
    if shadow {
        tracing::info!("shadow mode: migrations left to the authoritative engine");
    } else {
        db.migrate()
            .await
            .map_err(|e| format!("migration failed: {e}"))?;
        tracing::info!("migrations applied");
    }

    // Redis.
    let redis = redis::Client::open(redis_url(&cfg))
        .map_err(|e| format!("redis client: {e}"))?
        .get_connection_manager()
        .await
        .map_err(|e| format!("redis connection failed ({}): {e}", cfg.redis_addr))?;
    let (queue, sink) = if shadow {
        let sink = Sink::new(redis.clone());
        let mut qcfg = cfg.clone();
        qcfg.consumer_group = shadow::group(&cfg.consumer_group);
        let q = Queue::connect(&qcfg, Arc::new(ShadowExpiry(sink.clone())))
            .await
            .map_err(|e| format!("redis stream group creation failed: {e}"))?;
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
    let registry = Arc::new(
        Registry::load(&cfg, sandbox.as_ref())
            .await
            .map_err(|e| match e {
                ae_sandbox::Error::SandboxUnreachable => {
                    format!("go-judge unreachable at {}", cfg.judge_url)
                }
                e => format!("language registry: {e}"),
            })?,
    );
    let (up, down): (Vec<_>, Vec<_>) = registry.catalogue().into_iter().partition(|l| l.available);
    tracing::info!(
        available = ?up.iter().map(|l| l.id.as_str()).collect::<Vec<_>>(),
        unavailable = ?down.iter().map(|l| l.id.as_str()).collect::<Vec<_>>(),
        "language self-tests done"
    );

    let mut background: Vec<JoinHandle<()>> = vec![
        registry
            .clone()
            .spawn_refresher(sandbox.clone(), REFRESH_INTERVAL),
        spawn_queue_sampler(queue.clone(), QUEUE_SAMPLE),
    ];
    let debug = spawn_debug(&token).await?;

    // Public HTTP: up for probes now, ready only after pre-warm.
    let mut http = None;
    let state = if role.gateway() {
        let auth = AuthLayer::from_config(&cfg, Arc::new(db.clone()))
            .await
            .map_err(|e| format!("auth middleware init failed: {e}"))?;
        let state = AppState::new(
            &cfg,
            db.clone(),
            queue.clone(),
            sandbox.clone(),
            registry.clone(),
            auth,
            redis.clone(),
        );
        let listener = tokio::net::TcpListener::bind(&cfg.gateway_addr)
            .await
            .map_err(|e| format!("gateway listen {}: {e}", cfg.gateway_addr))?;
        tracing::info!(addr = %cfg.gateway_addr, "gateway listening");
        let app = ae_gateway::router(state.clone());
        let stop = token.clone().cancelled_owned();
        let t = token.clone();
        http = Some(tokio::spawn(async move {
            if let Err(e) = ae_gateway::serve(listener, app, stop).await {
                tracing::error!(error = %e, "gateway error");
                t.cancel();
            }
        }));
        Some(state)
    } else {
        None
    };

    prewarm(sandbox.as_ref(), cfg.prewarm).await;

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
        background.push(spawn_budget_gauge(worker.clone()));
        if !shadow {
            background.push(queue.spawn_reaper(db.clone()));
            draining.push(spawn_legacy_drain(
                queue.clone(),
                worker.clone(),
                token.clone(),
            ));
        }
        draining.push(tokio::spawn(worker.run(token.clone())));
    }

    if let Some(st) = &state {
        st.set_ready(true);
    }
    tracing::info!(role = ?role, shadow, "engine ready");

    token.cancelled().await;
    tracing::info!("shutting down");
    if let Some(st) = &state {
        st.set_ready(false);
    }
    // Workers stop reading at once and get the grace period for in-flight jobs.
    tracing::info!("waiting for in-flight jobs to complete");
    let drained = tokio::time::timeout(DRAIN_GRACE + HTTP_CLOSE, join_all(draining)).await;
    match drained {
        Ok(_) => tracing::info!("all workers stopped"),
        Err(_) => tracing::warn!("worker drain timeout, proceeding with shutdown"),
    }
    // HTTP stopped accepting at the signal; open connections get a short window.
    if let Some(mut h) = http
        && tokio::time::timeout(HTTP_CLOSE, &mut h).await.is_err()
    {
        h.abort();
    }
    if let Some(mut d) = debug
        && tokio::time::timeout(HTTP_CLOSE, &mut d).await.is_err()
    {
        d.abort();
    }
    for t in background {
        t.abort();
    }
    signals.abort();
    tracing::info!("shutdown complete");
    Ok(())
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
