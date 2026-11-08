//! The `engine` binary end to end: startup order, readiness, shadow mode, metrics
//! listener and SIGTERM drain, against Postgres and Redis containers and a fake go-judge.

mod common;

use ae_core::model::{Checker, DEFAULT_TENANT_ID, Job, Limits, Status, Submission};
use ae_queue::{TestData, TestSet};
use common::{EngineOpts, FakeJudge, eventually, execute, infra, run_body, spawn_engine};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

const STARTUP: Duration = Duration::from_secs(60);

#[tokio::test]
async fn startup_purges_stale_entries_and_logs_count() {
    let infra = infra().await;
    let judge = FakeJudge::start(&[]).await;
    let mut r = infra.redis().await;
    for i in 1..=3 {
        let _: String = redis::cmd("XADD")
            .arg("ac:submissions:submit")
            .arg(format!("{i}000-0"))
            .arg("submission_id")
            .arg(format!("stale-{i}"))
            .query_async(&mut r)
            .await
            .unwrap();
    }
    let e = spawn_engine(
        &infra,
        &EngineOpts {
            args: &["--role", "worker"],
            judge_url: &judge.url,
            env: &[],
        },
    );
    assert!(
        e.wait_log("purged 3 stale entries from queue", STARTUP)
            .await,
        "no purge log"
    );
    let len: u64 = redis::cmd("XLEN")
        .arg("ac:submissions:submit")
        .query_async(&mut r)
        .await
        .unwrap();
    assert_eq!(len, 0);
}

#[tokio::test]
async fn unavailable_language_not_reported_available() {
    let infra = infra().await;
    let judge = FakeJudge::start(&["/usr/bin/ghost"]).await;
    let e = spawn_engine(
        &infra,
        &EngineOpts {
            args: &["--role", "gateway"],
            judge_url: &judge.url,
            env: &[],
        },
    );
    e.wait_ready().await;
    let langs: Vec<Value> = reqwest::get(format!("{}/api/v1/languages", e.url))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let avail = |id: &str| {
        langs
            .iter()
            .find(|l| l["id"] == id)
            .unwrap_or_else(|| panic!("{id} missing from {langs:?}"))["available"]
            .as_bool()
            .unwrap()
    };
    assert!(avail("python"));
    assert!(!avail("ghost"));
    let r = reqwest::Client::new()
        .post(format!("{}/api/v1/execute", e.url))
        .header("content-type", "application/json")
        .body(
            r#"{"language":"ghost","source_code":"x","tests":[{"input":"","expected_output":""}]}"#,
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn readyz_false_until_prewarm_done() {
    let infra = infra().await;
    let judge = FakeJudge::start(&[]).await;
    let hold = judge.prewarm_gate.clone().write_owned().await;
    let e = spawn_engine(
        &infra,
        &EngineOpts {
            args: &["--role", "all"],
            judge_url: &judge.url,
            env: &[("AC_PREWARM", "2")],
        },
    );
    assert!(
        eventually(STARTUP, || async {
            judge.prewarms.load(Ordering::SeqCst) == 2
        })
        .await,
        "prewarm never reached the sandbox"
    );
    // The HTTP server is up for probes while prewarm is still running.
    assert!(
        eventually(STARTUP, || async { e.status("/livez").await == Some(200) }).await,
        "gateway not serving during prewarm"
    );
    assert_eq!(e.status("/readyz").await, Some(503));
    drop(hold);
    e.wait_ready().await;
}

#[tokio::test]
async fn sandbox_unreachable_exits_nonzero() {
    let infra = infra().await;
    let dead = format!("http://127.0.0.1:{}", common::free_port());
    let mut e = spawn_engine(
        &infra,
        &EngineOpts {
            args: &["--role", "all"],
            judge_url: &dead,
            env: &[],
        },
    );
    let status = e
        .wait_exit(Duration::from_secs(90))
        .await
        .expect("engine kept running without a sandbox");
    assert!(!status.success(), "{status:?}");
    assert!(e.has_log("go-judge unreachable"));
}

#[tokio::test]
async fn metrics_only_on_loopback() {
    let infra = infra().await;
    let judge = FakeJudge::start(&[]).await;
    let e = spawn_engine(
        &infra,
        &EngineOpts {
            args: &["--role", "all"],
            judge_url: &judge.url,
            env: &[],
        },
    );
    e.wait_ready().await;
    assert_eq!(
        e.status("/metrics").await,
        Some(404),
        "metrics on public port"
    );
    let body = reqwest::get(format!("http://127.0.0.1:{}/metrics", e.debug_port))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("ac_queue_depth"), "{body}");
    assert!(body.contains("ac_scheduler_budget_used"), "{body}");
    assert!(e.has_log(&format!("127.0.0.1:{}", e.debug_port)));
    // A non-loopback address of this host must not reach the debug listener.
    if let Some(ip) = local_ip() {
        let r = tokio::time::timeout(
            Duration::from_secs(2),
            tokio::net::TcpStream::connect((ip, e.debug_port)),
        )
        .await;
        assert!(!matches!(r, Ok(Ok(_))), "debug listener reachable on {ip}");
    }
}

/// This host's primary non-loopback address (a UDP connect sends no packets).
fn local_ip() -> Option<std::net::IpAddr> {
    let s = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    s.connect("192.0.2.1:9").ok()?;
    let ip = s.local_addr().ok()?.ip();
    (!ip.is_loopback() && !ip.is_unspecified()).then_some(ip)
}

#[tokio::test]
async fn sigterm_drains_then_exits_zero() {
    let infra = infra().await;
    let judge = FakeJudge::start(&[]).await;
    let mut e = spawn_engine(
        &infra,
        &EngineOpts {
            args: &["--role", "all"],
            judge_url: &judge.url,
            env: &[],
        },
    );
    e.wait_ready().await;
    let hold = judge.slow_gate.clone().write_owned().await;
    let v = execute(&e.url, &run_body("SLOW")).await;
    let id = v["job_id"].as_str().unwrap().to_owned();
    assert!(
        eventually(STARTUP, || async {
            judge.slow_started.load(Ordering::SeqCst) == 1
        })
        .await,
        "job never reached the sandbox"
    );
    e.signal("TERM");
    assert!(e.wait_log("shutting down", STARTUP).await);
    // The job is still in flight; the engine must wait for it.
    assert!(e.wait_exit(Duration::from_millis(500)).await.is_none());
    drop(hold);
    let status = e
        .wait_exit(Duration::from_secs(60))
        .await
        .expect("engine did not exit after the drain");
    assert!(status.success(), "{status:?}");
    let db = ae_db::connect(&infra.config()).await.unwrap();
    let sub = db.get_submission(&id).await.unwrap().unwrap();
    assert_eq!(sub.status, Status::Completed);
    assert!(
        e.has_log("4bf92f3577b34da6a3ce929d0e0e4736"),
        "traceparent not on the job span"
    );
}

/// Migrates, inserts a queued python submission and enqueues it on the submit stream
/// as the Go engine's gateway would. Returns (db, queue, job id).
async fn seed_job(
    infra: &common::Infra,
    expires_at: Option<i64>,
) -> (ae_db::Db, ae_queue::Queue, String) {
    let cfg = infra.config();
    let db = ae_db::connect(&cfg).await.unwrap();
    db.migrate().await.unwrap();
    let queue = ae_queue::Queue::connect(&cfg, Arc::new(db.clone()))
        .await
        .unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    db.insert_submission(&Submission {
        id: id.clone(),
        tenant_id: DEFAULT_TENANT_ID.into(),
        user_id: None,
        idempotency_key: None,
        language: "python".into(),
        source_code: "print('hello')".into(),
        status: Status::Queued,
        verdict: None,
        compile_stderr: String::new(),
        cpu_time_ns: 0,
        memory_bytes: 0,
        test_count: 0,
        tests_passed: 0,
        created_at: 0,
        updated_at: 0,
    })
    .await
    .unwrap();
    let job = Job {
        job_id: id.clone(),
        submission_id: id.clone(),
        language: "python".into(),
        source: "print('hello')".into(),
        limits: Limits::default(),
        checker: Checker::Exact,
        tests_key: format!("ac:tests:{id}"),
        expires_at,
    };
    let set = TestSet {
        tests: vec![TestData {
            input: String::new(),
            expected_output: "hello".into(),
            env: Vec::new(),
        }],
        callback_url: "http://127.0.0.1:9/hook".into(),
        ..TestSet::default()
    };
    queue
        .prepare_and_enqueue(&job, &set, "ac:submissions:submit")
        .await
        .unwrap();
    (db, queue, id)
}

/// Polls `ac:shadow:<id>` until it exists.
async fn shadow_record(r: &mut redis::aio::ConnectionManager, id: &str) -> Value {
    let key = format!("ac:shadow:{id}");
    let deadline = tokio::time::Instant::now() + STARTUP;
    loop {
        let got: Option<String> = redis::cmd("GET").arg(&key).query_async(r).await.unwrap();
        if let Some(raw) = got {
            return serde_json::from_str(&raw).unwrap();
        }
        assert!(tokio::time::Instant::now() < deadline, "no shadow verdict");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn shadow_expired_job_publishes_nothing() {
    let infra = infra().await;
    let judge = FakeJudge::start(&[]).await;
    let (db, queue, id) = seed_job(&infra, Some(1)).await;
    let mut sub = queue.subscribe(&id).await.unwrap();
    let _e = spawn_engine(
        &infra,
        &EngineOpts {
            args: &["--role", "worker", "--shadow"],
            judge_url: &judge.url,
            env: &[],
        },
    );
    let mut r = infra.redis().await;
    let v = shadow_record(&mut r, &id).await;
    assert_eq!(v["verdict"], "internal_error", "{v}");
    let log_len: u64 = redis::cmd("LLEN")
        .arg(format!("ac:events:{id}:log"))
        .query_async(&mut r)
        .await
        .unwrap();
    assert_eq!(log_len, 1, "only the enqueue's QUEUED");
    // Pub/sub: nothing after QUEUED (published before the subscription).
    let got = tokio::time::timeout(Duration::from_millis(500), sub.recv()).await;
    assert!(!matches!(got, Ok(Some(_))), "shadow published {got:?}");
    let row = db.get_submission(&id).await.unwrap().unwrap();
    assert_eq!((row.status, row.verdict), (Status::Queued, None));
    assert_eq!(
        v["results"].as_array().map(Vec::len),
        Some(0),
        "never ran: {v}"
    );
}

#[tokio::test]
async fn shadow_mode_writes_only_shadow_key() {
    let infra = infra().await;
    let judge = FakeJudge::start(&[]).await;
    let (db, queue, id) = seed_job(&infra, None).await;

    // The gateway cannot run in shadow mode.
    for role in ["all", "gateway"] {
        let mut bad = spawn_engine(
            &infra,
            &EngineOpts {
                args: &["--role", role, "--shadow"],
                judge_url: &judge.url,
                env: &[],
            },
        );
        let s = bad
            .wait_exit(STARTUP)
            .await
            .expect("shadow gateway kept running");
        assert!(!s.success(), "--shadow --role {role}");
    }

    let e = spawn_engine(
        &infra,
        &EngineOpts {
            args: &["--role", "worker", "--shadow"],
            judge_url: &judge.url,
            env: &[],
        },
    );
    let mut r = infra.redis().await;
    let key = format!("ac:shadow:{id}");
    let v = shadow_record(&mut r, &id).await;
    assert_eq!(v["verdict"], "accepted", "{v}");
    assert_eq!(v["results"].as_array().unwrap().len(), 1, "{v}");
    assert_eq!(v["results"][0]["verdict"], "accepted", "{v}");
    let ttl: i64 = redis::cmd("TTL")
        .arg(&key)
        .query_async(&mut r)
        .await
        .unwrap();
    assert!((1..=3600).contains(&ttl), "ttl {ttl}");

    // No Postgres writes, no events beyond the enqueue's QUEUED.
    let sub = db.get_submission(&id).await.unwrap().unwrap();
    assert_eq!(sub.status, Status::Queued);
    assert_eq!(sub.verdict, None);
    let events = queue.replay(&id).await.unwrap();
    assert_eq!(events.len(), 1, "{events:?}");

    // The Go group never saw the message; the shadow group acked it.
    let groups: Vec<HashMap<String, redis::Value>> = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg("ac:submissions:submit")
        .query_async(&mut r)
        .await
        .unwrap();
    let group = |name: &str| {
        groups
            .iter()
            .find(|g| field(g, "name") == name)
            .unwrap_or_else(|| panic!("group {name} missing: {groups:?}"))
            .clone()
    };
    assert_eq!(field(&group("workers"), "last-delivered-id"), "0-0");
    assert_eq!(field(&group("workers-rust"), "pending"), "0");
    assert!(
        !e.has_log("webhook already dispatched") && !e.has_log("webhook marker"),
        "shadow mode must not touch webhooks, even in logs"
    );
}

fn field(g: &HashMap<String, redis::Value>, k: &str) -> String {
    match &g[k] {
        redis::Value::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
        redis::Value::SimpleString(s) => s.clone(),
        redis::Value::Int(n) => n.to_string(),
        v => format!("{v:?}"),
    }
}
