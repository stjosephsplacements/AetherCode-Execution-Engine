//! The real `shadowdiff` binary against Redis and Postgres containers: summary line and
//! exit codes for match / diff / pending / unreadable / tests-gone records.

use ae_core::model::{DEFAULT_TENANT_ID, Status, Submission, TestResult, Verdict};
use ae_shadowdiff::tests_gone_message;
use serde_json::json;
use std::collections::HashMap;
use std::process::Command;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::Redis;
use uuid::Uuid;

struct Env {
    _pg: ContainerAsync<Postgres>,
    _redis: ContainerAsync<Redis>,
    pg_url: String,
    redis_url: String,
    db: ae_db::Db,
    r: redis::aio::ConnectionManager,
}

async fn env() -> Env {
    let pg = Postgres::default()
        .with_user("aethercode")
        .with_password("aethercode")
        .with_db_name("aethercode_test")
        .with_tag("18")
        .start()
        .await
        .expect("start postgres:18 (is docker running?)");
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let pg_url = format!("postgres://aethercode:aethercode@127.0.0.1:{port}/aethercode_test");
    let redis = Redis::default().with_tag("7").start().await.unwrap();
    let rport = redis.get_host_port_ipv4(6379).await.unwrap();
    let redis_url = format!("redis://127.0.0.1:{rport}/");
    let cfg = ae_core::Config::from_map(&HashMap::from([(
        "AC_DATABASE_URL".to_owned(),
        pg_url.clone(),
    )]))
    .unwrap();
    let db = ae_db::connect(&cfg).await.unwrap();
    db.migrate().await.unwrap();
    let r = redis::Client::open(redis_url.as_str())
        .unwrap()
        .get_connection_manager()
        .await
        .unwrap();
    Env {
        _pg: pg,
        _redis: redis,
        pg_url,
        redis_url,
        db,
        r,
    }
}

fn result(v: Verdict) -> TestResult {
    TestResult {
        test_index: 0,
        verdict: v,
        cpu_time_ns: 1,
        memory_bytes: 1,
        run_time_ns: 1,
        exit_status: 0,
        sandbox_status: String::new(),
        message: String::new(),
        stdout_preview: String::new(),
        stderr_preview: String::new(),
    }
}

/// A Go-engine row: queued (`None`) or finished with one test verdict.
async fn go_row(e: &Env, v: Option<Verdict>) -> String {
    let id = Uuid::new_v4().to_string();
    e.db.insert_submission(&Submission {
        id: id.clone(),
        tenant_id: DEFAULT_TENANT_ID.to_owned(),
        user_id: None,
        idempotency_key: None,
        language: "python".into(),
        source_code: "print(1)".into(),
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
    if let Some(v) = v {
        e.db.finish_with_results(&id, v, &[result(v)])
            .await
            .unwrap();
    }
    id
}

async fn shadow_at(
    e: &mut Env,
    id: &str,
    v: Verdict,
    results: &[TestResult],
    stderr: &str,
    recorded_at: i64,
) {
    let passed = results
        .iter()
        .filter(|r| r.verdict == Verdict::Accepted)
        .count();
    let body = json!({
        "job_id": id, "engine": "rust", "verdict": v, "tests_passed": passed,
        "test_count": results.len(), "results": results, "compile_stderr": stderr,
        "recorded_at": recorded_at,
    });
    set(e, &format!("ac:shadow:{id}"), &body.to_string()).await;
}

async fn shadow(e: &mut Env, id: &str, v: Verdict, results: &[TestResult], stderr: &str) {
    shadow_at(e, id, v, results, stderr, 0).await;
}

async fn set(e: &mut Env, key: &str, val: &str) {
    redis::cmd("SET")
        .arg(key)
        .arg(val)
        .query_async::<()>(&mut e.r)
        .await
        .unwrap();
}

async fn flush(e: &mut Env) {
    redis::cmd("FLUSHALL")
        .query_async::<()>(&mut e.r)
        .await
        .unwrap();
}

/// (exit code, stdout)
fn run(e: &Env, extra: &[&str]) -> (i32, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_shadowdiff"))
        .env_clear()
        .args(["--redis", &e.redis_url, "--pg", &e.pg_url])
        .args(extra)
        .output()
        .unwrap();
    (
        out.status.code().unwrap(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
    )
}

#[tokio::test]
async fn binary_summary_and_exit_codes() {
    let mut e = env().await;

    // Only pending records: nothing compared, so the run fails even without a flag.
    let p = go_row(&e, None).await;
    shadow(
        &mut e,
        &p,
        Verdict::Accepted,
        &[result(Verdict::Accepted)],
        "",
    )
    .await;
    let (code, out) = run(&e, &[]);
    assert_eq!(code, 1, "{out}");
    assert!(
        out.contains(
            "compared=0 matched=0 diff=0 pending=1 ignored=0 unreadable=0 skipped_since=0"
        ),
        "{out}"
    );
    assert!(out.contains(&format!("PENDING {p}")), "{out}");

    // One of each.
    let m = go_row(&e, Some(Verdict::Accepted)).await;
    shadow(
        &mut e,
        &m,
        Verdict::Accepted,
        &[result(Verdict::Accepted)],
        "",
    )
    .await;
    let d = go_row(&e, Some(Verdict::WrongAnswer)).await;
    shadow(
        &mut e,
        &d,
        Verdict::Accepted,
        &[result(Verdict::Accepted)],
        "",
    )
    .await;
    let g = go_row(&e, Some(Verdict::Accepted)).await;
    shadow(
        &mut e,
        &g,
        Verdict::InternalError,
        &[],
        &tests_gone_message(&g),
    )
    .await;
    set(&mut e, "ac:shadow:garbled", "{not json").await;
    let (code, out) = run(&e, &[]);
    assert_eq!(code, 1, "{out}");
    assert!(
        out.contains(
            "compared=2 matched=1 diff=1 pending=1 ignored=1 unreadable=1 skipped_since=0"
        ),
        "{out}"
    );
    assert!(
        out.contains(&format!("DIFF {d}")) && out.contains("UNREADABLE garbled"),
        "{out}"
    );

    // Without the diff and the garbled record only the pending one remains.
    redis::cmd("DEL")
        .arg(format!("ac:shadow:{d}"))
        .arg("ac:shadow:garbled")
        .query_async::<()>(&mut e.r)
        .await
        .unwrap();
    let (code, out) = run(&e, &[]);
    assert_eq!(code, 0, "{out}");
    assert!(
        out.contains(
            "compared=1 matched=1 diff=0 pending=1 ignored=1 unreadable=0 skipped_since=0"
        ),
        "{out}"
    );
    assert!(!out.contains("PENDING"), "{out}");
    let (code, out) = run(&e, &["--fail-on-pending"]);
    assert_eq!(code, 1, "{out}");
    assert!(out.contains(&format!("PENDING {p}")), "{out}");

    // Clean run.
    flush(&mut e).await;
    shadow(
        &mut e,
        &m,
        Verdict::Accepted,
        &[result(Verdict::Accepted)],
        "",
    )
    .await;
    let (code, out) = run(&e, &["--fail-on-pending"]);
    assert_eq!(code, 0, "{out}");
}

#[tokio::test]
async fn binary_refuses_empty_and_all_ignored_runs() {
    let mut e = env().await;
    let (code, out) = run(&e, &[]);
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("refusing to report parity"), "{out}");
    assert!(
        out.contains(
            "compared=0 matched=0 diff=0 pending=0 ignored=0 unreadable=0 skipped_since=0"
        ),
        "{out}"
    );
    let (code, out) = run(&e, &["--allow-empty"]);
    assert_eq!(code, 0, "{out}");

    let g = go_row(&e, Some(Verdict::Accepted)).await;
    shadow(
        &mut e,
        &g,
        Verdict::InternalError,
        &[],
        &tests_gone_message(&g),
    )
    .await;
    let (code, out) = run(&e, &[]);
    assert_eq!(code, 1, "{out}");
    assert!(
        out.contains("ignored=1") && out.contains("refusing"),
        "{out}"
    );
    let (code, _) = run(&e, &["--allow-empty"]);
    assert_eq!(code, 0);
}

#[tokio::test]
async fn binary_since_counts_skipped_and_flags_missing_timestamp() {
    let mut e = env().await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let ok = go_row(&e, Some(Verdict::Accepted)).await;
    shadow_at(
        &mut e,
        &ok,
        Verdict::Accepted,
        &[result(Verdict::Accepted)],
        "",
        now,
    )
    .await;
    let old = go_row(&e, Some(Verdict::Accepted)).await;
    shadow_at(
        &mut e,
        &old,
        Verdict::Accepted,
        &[result(Verdict::Accepted)],
        "",
        now - 7_200_000,
    )
    .await;
    let (code, out) = run(&e, &["--since", "1h"]);
    assert_eq!(code, 0, "{out}");
    assert!(
        out.contains(
            "compared=1 matched=1 diff=0 pending=0 ignored=0 unreadable=0 skipped_since=1"
        ),
        "{out}"
    );

    let none = go_row(&e, Some(Verdict::Accepted)).await;
    shadow_at(
        &mut e,
        &none,
        Verdict::Accepted,
        &[result(Verdict::Accepted)],
        "",
        0,
    )
    .await;
    let (code, out) = run(&e, &["--since", "1h"]);
    assert_eq!(code, 1, "{out}");
    assert!(
        out.contains("unreadable=1 skipped_since=1") && out.contains(&format!("UNREADABLE {none}")),
        "{out}"
    );
}
