//! Ports of `internal/gateway/handler_test.go` plus the ingest, idempotency, tenant and
//! language-availability rules, against the real server (Postgres and Redis containers,
//! fake sandbox).

mod common;

use ae_core::eventlog::EventType;
use ae_core::model::{DEFAULT_TENANT_ID, Status, Submission};
use common::{StackOpts, error_of, http, post_execute, run_body, stack};
use serde_json::Value;
use std::sync::atomic::Ordering;
use std::time::Duration;

const LEGACY: &str =
    "mode=submit and problem_version_id are no longer supported; send tests in the request body";

async fn xlen(st: &common::Stack, stream: &str) -> u64 {
    st.queue.depth(stream).await.unwrap()
}

fn submission(id: &str, user: Option<String>) -> Submission {
    Submission {
        id: id.to_owned(),
        tenant_id: DEFAULT_TENANT_ID.into(),
        user_id: user,
        idempotency_key: None,
        language: "python".into(),
        source_code: "print(1)".into(),
        status: Status::Queued,
        verdict: None,
        compile_stderr: String::new(),
        cpu_time_ns: 0,
        memory_bytes: 0,
        test_count: 3,
        tests_passed: 0,
        created_at: 0,
        updated_at: 0,
    }
}

#[tokio::test]
async fn health_reports_checks_versions_workers_and_queues() {
    let st = stack(StackOpts::default()).await;
    for i in 0..5 {
        st.queue
            .enqueue("ac:submissions:submit", &i.to_string())
            .await
            .unwrap();
    }
    for i in 0..3 {
        st.queue
            .enqueue("ac:submissions:run", &i.to_string())
            .await
            .unwrap();
    }
    let r = http()
        .get(format!("{}/api/v1/health", st.url))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["status"], "healthy");
    assert_eq!(v["checks"]["database"], "ok");
    assert_eq!(v["checks"]["sandbox"], "ok");
    assert_eq!(v["checks"]["redis"], "ok");
    assert_eq!(v["sandbox_version"], "1.13.0");
    assert_eq!(v["workers"]["configured"], 1);
    assert_eq!(v["workers"]["active"], 0);
    assert!(v["uptime_seconds"].as_u64().is_some());
    assert!(
        v["go_version"].as_str().unwrap().starts_with("rustc "),
        "the compiler version, not the MSRV: {}",
        v["go_version"]
    );
    assert_eq!(v["queues"]["ac:submissions:submit"], 5);
    assert_eq!(v["queues"]["ac:submissions:run"], 3);

    // Sandbox down: degraded, still 200, no sandbox_version.
    st.sb.version_fails.store(true, Ordering::SeqCst);
    let r = http()
        .get(format!("{}/api/v1/health", st.url))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["status"], "degraded");
    assert_eq!(v["checks"]["sandbox"], "error");
    assert!(v.get("sandbox_version").is_none());
}

#[tokio::test]
async fn health_is_unhealthy_503_when_database_down() {
    let st = stack(StackOpts::default()).await;
    st.pg.stop().await.unwrap();
    let start = std::time::Instant::now();
    let r = http()
        .get(format!("{}/api/v1/health", st.url))
        .send()
        .await
        .unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(7),
        "health must answer within 5 s"
    );
    assert_eq!(r.status(), 503);
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["status"], "unhealthy");
    assert_eq!(v["checks"]["database"], "error");
    assert_eq!(v["checks"]["sandbox"], "ok");
    assert_eq!(v["checks"]["redis"], "ok");
    assert_eq!(v["sandbox_version"], "1.13.0");
}

#[tokio::test]
async fn languages_lists_unavailable_flag() {
    let st = stack(StackOpts {
        missing: vec!["/usr/bin/rustc"],
        ..StackOpts::default()
    })
    .await;
    let r = http()
        .get(format!("{}/api/v1/languages", st.url))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let langs: Vec<Value> = r.json().await.unwrap();
    let ids: Vec<&str> = langs.iter().map(|l| l["id"].as_str().unwrap()).collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted, "sorted by id");
    for id in ["bash", "c", "cpp", "python", "rust", "sqlite"] {
        assert!(ids.contains(&id), "{id} missing");
    }
    let by = |id: &str| langs.iter().find(|l| l["id"] == id).unwrap().clone();
    let py = by("python");
    assert_eq!(py["compiled"], false);
    assert_eq!(py["default_cpu_limit_ms"], 2000);
    assert_eq!(py["default_memory_limit_kb"], 524_288);
    assert_eq!(py["available"], true);
    assert_eq!(by("cpp")["compiled"], true);
    assert_eq!(by("rust")["available"], false);

    let (code, msg) = error_of(
        post_execute(
            &st,
            r#"{"language":"rust","source_code":"fn main(){}","tests":[{"input":"","expected_output":""}]}"#,
            None,
        )
        .await,
    )
    .await;
    assert_eq!(
        (code, msg.as_str()),
        (400, "language rust is not installed on this engine")
    );
}

#[tokio::test]
async fn submission_ids_must_be_uuids_and_exist() {
    let st = stack(StackOpts::default()).await;
    for method in [reqwest::Method::GET, reqwest::Method::DELETE] {
        let r = http()
            .request(
                method.clone(),
                format!("{}/api/v1/submissions/not-a-uuid", st.url),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(error_of(r).await, (400, "invalid submission id".into()));
        let r = http()
            .request(
                method,
                format!(
                    "{}/api/v1/submissions/00000000-0000-0000-0000-000000000001",
                    st.url
                ),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(error_of(r).await, (404, "not found".into()));
    }
}

#[tokio::test]
async fn cancel_emits_verdict_event_and_is_then_conflict() {
    let st = stack(StackOpts::default()).await;
    let id = uuid::Uuid::new_v4().to_string();
    st.db
        .insert_submission(&submission(&id, None))
        .await
        .unwrap();
    let token = st.queue.issue_stream_token(&id).await.unwrap();
    let url = format!("{}/api/v1/submissions/{id}", st.url);

    let r = http().delete(&url).send().await.unwrap();
    assert_eq!(
        error_of(r).await,
        (401, "invalid or missing stream_token".into())
    );

    let r = http()
        .delete(format!("{url}?stream_token={token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "{\"cancelled\":true}\n");
    let evs = st.queue.replay(&id).await.unwrap();
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].kind, EventType::Verdict);
    assert_eq!(evs[0].data.as_ref().unwrap()["verdict"], "cancelled");

    let r = http()
        .delete(format!("{url}?stream_token={token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        error_of(r).await,
        (
            409,
            "submission is not cancellable (already terminal or not found)".into()
        )
    );
}

#[tokio::test]
async fn get_submission_requires_token_or_owner() {
    let st = stack(StackOpts {
        auth: true,
        ..StackOpts::default()
    })
    .await;
    let owner = st.token("owner");
    let r = post_execute(&st, &run_body("3", ""), Some(&owner)).await;
    assert_eq!(r.status(), 201);
    let v: Value = r.json().await.unwrap();
    let (id, tok) = (
        v["job_id"].as_str().unwrap(),
        v["stream_token"].as_str().unwrap(),
    );
    let url = format!("{}/api/v1/submissions/{id}", st.url);

    // Unauthenticated: refused without the stream token.
    let r = http().get(&url).send().await.unwrap();
    assert_eq!(
        error_of(r).await,
        (401, "invalid or missing stream_token".into())
    );
    let r = http()
        .get(format!("{url}?stream_token=wrong"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    let r = http()
        .get(format!("{url}?stream_token={tok}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    // Another user of the same tenant: not found (never 403, so ids do not leak).
    let other = st.token("intruder");
    let r = http().get(&url).bearer_auth(&other).send().await.unwrap();
    assert_eq!(error_of(r).await, (404, "not found".into()));
    let r = http()
        .delete(&url)
        .bearer_auth(&other)
        .send()
        .await
        .unwrap();
    assert_eq!(error_of(r).await, (404, "not found".into()));

    let r = http().get(&url).bearer_auth(&owner).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let v: Value = r.json().await.unwrap();
    assert_eq!(v["job_id"], id);
    assert_eq!(v["status"], "queued");
    assert_eq!(v["language"], "python");
    assert!(v.get("verdict").is_none());
    assert!(v.get("results").is_none());
    let created = v["created_at"].as_str().unwrap();
    assert!(created.ends_with('Z') && created.contains('T'), "{created}");
}

#[tokio::test]
async fn execute_validation_matches_go() {
    let st = stack(StackOpts::default()).await;
    let cases: &[(&str, u16, &str)] = &[
        (
            r#"{"language":"python","source_code":"print(1)","checker":"regex","tests":[{"input":"","expected_output":"1"}]}"#,
            400,
            "checker must be one of: exact, token, float, custom",
        ),
        (
            r#"{"language":"python","source_code":"print(1)","mode":"submit","problem_version_id":"0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"}"#,
            400,
            LEGACY,
        ),
        (
            r#"{"language":"python","source_code":"print(1)","mode":"submit","problem_version_id":"0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b","tests":[{"input":"","expected_output":"1"}]}"#,
            400,
            LEGACY,
        ),
        (
            r#"{"language":"python","source_code":"print(1)","problem_version_id":"0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b","tests":[{"input":"","expected_output":"1"}]}"#,
            400,
            LEGACY,
        ),
        (
            r#"{"language":"python","source_code":"print(1)","mode":"run"}"#,
            400,
            "at least one test is required",
        ),
        (
            r#"{"language":"python","source_code":"print(1)"}"#,
            400,
            "at least one test is required",
        ),
        (
            r#"{"language":"python","source_code":"print(1)","priority":"invalid","tests":[{"input":"","expected_output":"1"}]}"#,
            400,
            "priority must be one of: run, submit, bulk",
        ),
        (
            r#"{"language":"python","source_code":"print(1)","env":["LD_PRELOAD=/w/x.so"],"tests":[{"input":"","expected_output":"1"}]}"#,
            400,
            r#"env: key "LD_PRELOAD" is not permitted"#,
        ),
        (
            r#"{"language":"python","source_code":"print(1)","tests":[{"input":"","expected_output":"1"},{"input":"","expected_output":"1","env":["PATH=/w"]}]}"#,
            400,
            r#"tests[1].env: key "PATH" is not permitted"#,
        ),
        (
            r#"{"language":"python","source_code":"print(1)","tests":[{"input":"","expected_output":"1","env":["TZ"]}]}"#,
            400,
            r#"tests[0].env: entry "TZ" must be KEY=VALUE"#,
        ),
        (
            r#"{"language":"c","source_code":"int main(){}","stack_limit_kb":10,"tests":[{"input":"","expected_output":""}]}"#,
            400,
            "stack_limit_kb must be between 1024 and 1048576",
        ),
        (
            r#"{"language":"c","source_code":"int main(){}","max_processes":1000,"tests":[{"input":"","expected_output":""}]}"#,
            400,
            "max_processes must be between 1 and 512",
        ),
        (
            r#"{"language":"c","source_code":"int main(){}","max_processes":0,"tests":[{"input":"","expected_output":""}]}"#,
            400,
            "max_processes must be between 1 and 512",
        ),
        (
            r#"{"language":"c","source_code":"int main(){}","args":["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17"],"tests":[{"input":"","expected_output":""}]}"#,
            400,
            "args: at most 16 arguments",
        ),
        (
            r#"{"language":"c","source_code":"int main(){}","output":"everything","tests":[{"input":"","expected_output":""}]}"#,
            400,
            "output must be one of: full, preview, none",
        ),
        (
            r#"{"language":"sqlite","source_code":"select 1;","args":["x"],"tests":[{"input":"","expected_output":"1"}]}"#,
            400,
            "args are not supported for sqlite",
        ),
        (
            r#"{"language":"cobol","source_code":"x","tests":[{"input":"","expected_output":""}]}"#,
            400,
            "unsupported language",
        ),
        (
            r#"{"language":"python","source_code":"","tests":[{"input":"","expected_output":""}]}"#,
            400,
            "source_code is required",
        ),
        (
            r#"{"language":"python","source_code":"x","time_limit_ms":50,"tests":[{"input":"","expected_output":""}]}"#,
            400,
            "time_limit_ms must be between 100 and 20000",
        ),
        (
            r#"{"language":"python","source_code":"x","memory_limit_kb":1,"tests":[{"input":"","expected_output":""}]}"#,
            400,
            "memory_limit_kb must be between 16384 and 2097152",
        ),
        (
            r#"{"language":"python","source_code":"x","callback_url":"ftp://x/y","tests":[{"input":"","expected_output":""}]}"#,
            400,
            "invalid callback_url",
        ),
        (
            r#"{"language":"python","source_code":"x","checker":"custom","tests":[{"input":"","expected_output":""}]}"#,
            400,
            "checker_code is required when checker is custom",
        ),
        (
            r#"{"language":"python","source_code":"x","checker_code":"y","checker_language":"cobol","tests":[{"input":"","expected_output":""}]}"#,
            400,
            "unsupported checker_language",
        ),
        (
            r#"{"language":"cpp","source_code":"x","compiler_flags":["-fplugin=evil"],"tests":[{"input":"","expected_output":""}]}"#,
            400,
            r#"flag "-fplugin=evil" is not permitted for cpp"#,
        ),
        (r#"{"language":"#, 400, "invalid JSON"),
        (r#"[1,2]"#, 400, "invalid JSON"),
    ];
    for (body, code, msg) in cases {
        let got = error_of(post_execute(&st, body, None).await).await;
        assert_eq!(got, (*code, (*msg).to_owned()), "body {body}");
    }
    let r = http()
        .post(format!("{}/api/v1/execute", st.url))
        .header("content-type", "text/plain")
        .body(run_body("3", ""))
        .send()
        .await
        .unwrap();
    assert_eq!(
        error_of(r).await,
        (415, "Content-Type must be application/json".into())
    );
    // A body over the size limit is rejected without echoing anything back.
    let big = format!(
        r#"{{"language":"python","source_code":"{}"}}"#,
        "a".repeat(65 << 20)
    );
    let got = error_of(post_execute(&st, &big, None).await).await;
    assert_eq!(got, (400, "invalid JSON".into()));
    let src = "x".repeat(64 * 1024 + 1);
    let body = format!(
        r#"{{"language":"python","source_code":"{src}","tests":[{{"input":"","expected_output":""}}]}}"#
    );
    assert_eq!(
        error_of(post_execute(&st, &body, None).await).await,
        (400, "source_code exceeds 64KB".into())
    );
    let tests = vec![r#"{"input":"","expected_output":""}"#; 101].join(",");
    let body = format!(r#"{{"language":"python","source_code":"x","tests":[{tests}]}}"#);
    assert_eq!(
        error_of(post_execute(&st, &body, None).await).await,
        (400, "too many tests (max 100)".into())
    );
    for s in st.queue.streams() {
        assert_eq!(xlen(&st, s).await, 0, "nothing enqueued on {s}");
    }
}

#[tokio::test]
async fn execute_routes_priority_tiers_and_merges_env() {
    let st = stack(StackOpts::default()).await;
    let r = post_execute(
        &st,
        r#"{"language":"python","source_code":"print(1)","priority":"bulk","tests":[{"input":"","expected_output":"1"}]}"#,
        None,
    )
    .await;
    assert_eq!(r.status(), 201);
    assert_eq!(xlen(&st, "ac:submissions:bulk").await, 1);
    let r = post_execute(
        &st,
        r#"{"language":"python","source_code":"print(1)","priority":"submit","tests":[{"input":"","expected_output":"1"}]}"#,
        None,
    )
    .await;
    assert_eq!(r.status(), 201);
    assert_eq!(xlen(&st, "ac:submissions:submit").await, 1);

    let body = r#"{"language":"python","source_code":"print(1)","env":["TZ=UTC","LANG=C"],"tests":[{"input":"","expected_output":"1"},{"input":"","expected_output":"1","env":["TZ=Asia/Kolkata","AC_MODE=strict"]}]}"#;
    let r = post_execute(&st, body, None).await;
    assert_eq!(r.status(), 201);
    assert_eq!(xlen(&st, "ac:submissions:run").await, 1);
    let v: Value = r.json().await.unwrap();
    let id = v["job_id"].as_str().unwrap();
    assert_eq!(uuid::Uuid::parse_str(id).unwrap().get_version_num(), 7);
    let spec = st.queue.get_tests(id).await.unwrap();
    assert_eq!(spec.set.tests[0].env, ["TZ=UTC", "LANG=C"]);
    assert_eq!(
        spec.set.tests[1].env,
        ["TZ=Asia/Kolkata", "LANG=C", "AC_MODE=strict"]
    );
    let sub = st.db.get_submission(id).await.unwrap().unwrap();
    assert_eq!(sub.status, Status::Queued);
    assert_eq!(sub.tenant_id, DEFAULT_TENANT_ID);
    assert!(sub.user_id.is_none());
}

#[tokio::test]
async fn execute_is_idempotent_on_repeat_key() {
    let st = stack(StackOpts {
        auth: true,
        ..StackOpts::default()
    })
    .await;
    let tok = st.token("alice");
    let body = r#"{"language":"python","source_code":"print(1)","idempotency_key":"exam-1","tests":[{"input":"","expected_output":"1"}]}"#;
    let first = post_execute(&st, body, Some(&tok)).await;
    assert_eq!(first.status(), 201);
    let v: Value = first.json().await.unwrap();
    let id = v["job_id"].as_str().unwrap().to_owned();
    assert!(v["stream_token"].is_string());

    let again = post_execute(&st, body, Some(&tok)).await;
    assert_eq!(again.status(), 200);
    assert_eq!(
        again.headers()["content-type"].to_str().unwrap(),
        "application/json"
    );
    assert_eq!(
        again.text().await.unwrap(),
        format!("{{\"job_id\":\"{id}\"}}\n")
    );
    assert_eq!(xlen(&st, "ac:submissions:run").await, 1, "enqueued once");

    // Same key, other user: a separate job.
    let bob = post_execute(&st, body, Some(&st.token("bob"))).await;
    assert_eq!(bob.status(), 201);
    // Unauthenticated callers have no user, so the key never deduplicates.
    let anon1 = post_execute(&st, body, None).await;
    let anon2 = post_execute(&st, body, None).await;
    assert_eq!(
        (anon1.status().as_u16(), anon2.status().as_u16()),
        (201, 201)
    );

    let long = format!(
        r#"{{"language":"python","source_code":"print(1)","idempotency_key":"{}","tests":[{{"input":"","expected_output":"1"}}]}}"#,
        "k".repeat(256)
    );
    assert_eq!(
        error_of(post_execute(&st, &long, Some(&tok)).await).await,
        (400, "idempotency_key exceeds 255 bytes".into())
    );
}

#[tokio::test]
async fn ingest_concurrency_cap_512() {
    let st = stack(StackOpts::default()).await;
    let sem = st.state.ingest.clone();
    assert_eq!(sem.available_permits(), 512);
    let held = sem.clone().acquire_many_owned(512).await.unwrap();
    let r = post_execute(&st, &run_body("3", ""), None).await;
    assert_eq!(r.headers()["retry-after"], "1");
    assert_eq!(
        error_of(r).await,
        (429, "server is busy, try again later".into())
    );
    drop(held);
    let r = post_execute(&st, &run_body("3", ""), None).await;
    assert_eq!(r.status(), 201);
    assert_eq!(
        sem.available_permits(),
        512,
        "permit released after the request"
    );
}

#[tokio::test]
async fn admission_sheds_with_retry_after_from_drain_rate() {
    let st = stack(StackOpts {
        max_depth: 10,
        ..StackOpts::default()
    })
    .await;
    for i in 0..10 {
        st.queue
            .enqueue("ac:submissions:run", &i.to_string())
            .await
            .unwrap();
    }
    let r = post_execute(&st, &run_body("3", ""), None).await;
    assert_eq!(
        r.headers()["retry-after"],
        "10",
        "no measured drain rate yet"
    );
    assert_eq!(
        error_of(r).await,
        (429, "server is busy, try again later".into())
    );
    // The submit stream is admitted separately.
    let r = post_execute(
        &st,
        r#"{"language":"python","source_code":"print(1)","priority":"submit","tests":[{"input":"","expected_output":"1"}]}"#,
        None,
    )
    .await;
    assert_eq!(r.status(), 201);
}

#[tokio::test]
async fn execute_is_503_while_not_ready_but_other_routes_are_served() {
    let st = stack(StackOpts::default()).await;
    st.state.set_ready(false);
    let r = post_execute(&st, &run_body("3", ""), None).await;
    assert_eq!(r.status(), 503);
    assert_eq!(r.headers().get("retry-after").unwrap(), "5");
    assert_eq!(xlen(&st, "ac:submissions:run").await, 0, "nothing enqueued");
    for (p, want) in [
        ("/readyz", 503),
        ("/livez", 200),
        ("/api/v1/health", 200),
        ("/api/v1/languages", 200),
    ] {
        let r = http().get(format!("{}{p}", st.url)).send().await.unwrap();
        assert_eq!(r.status(), want, "{p}");
    }
    st.state.set_ready(true);
    assert_eq!(
        post_execute(&st, &run_body("3", ""), None).await.status(),
        201
    );
}

#[tokio::test]
async fn livez_readyz_and_metrics() {
    let st = stack(StackOpts {
        not_ready: true,
        ..Default::default()
    })
    .await;
    let r = http()
        .get(format!("{}/livez", st.url))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let r = http()
        .get(format!("{}/readyz", st.url))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503, "not ready until the flag is set");
    st.state.set_ready(true);
    let r = http()
        .get(format!("{}/readyz", st.url))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    // Not exposed on the public port.
    let r = http()
        .get(format!("{}/metrics", st.url))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);

    let _ = post_execute(&st, &run_body("3", ""), None).await;
    // The worker hook feeds the job metrics and the admission drain rate.
    let obs = st.state.worker_observer();
    obs.job_started();
    obs.compiled("cpp", Duration::from_millis(5), false);
    obs.stage("run", Duration::from_millis(5));
    obs.job_finished(
        "cpp",
        ae_core::model::Verdict::Accepted,
        Duration::from_millis(9),
    );
    obs.job_ended();
    assert_eq!(st.state.drain().rate(), 0.0);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(st.state.drain().rate() > 0.0, "job_ended feeds the drain");
    ae_gateway::metrics::sample_queues(&st.queue).await;
    let ready = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let m = ae_gateway::metrics_router(ready.clone());
    for (flag, want) in [(false, 503), (true, 200)] {
        ready.store(flag, std::sync::atomic::Ordering::SeqCst);
        for (path, code) in [("/livez", 200), ("/readyz", want)] {
            let r = tower::ServiceExt::oneshot(
                m.clone(),
                axum::http::Request::get(path)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(r.status(), code, "{path} ready={flag}");
        }
    }
    let resp = tower::ServiceExt::oneshot(
        m,
        axum::http::Request::get("/metrics")
            .body(axum::body::Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(resp.status(), 200);
    let text = common::body_string(resp).await;
    for name in [
        "ac_http_requests_total",
        "ac_http_request_duration_seconds",
        "ac_jobs_processed_total",
        "ac_job_processing_duration_seconds",
        "ac_compile_duration_seconds",
        "ac_active_workers",
        "ac_queue_depth",
        "ac_rate_limit_rejections_total",
        "ac_poison_messages_total",
        "ac_stage_duration_seconds",
        "ac_queue_age_seconds",
        "ac_compile_cache_hits_total",
        "ac_scheduler_budget_used",
    ] {
        assert!(
            text.contains(&format!("# TYPE {name} ")),
            "{name} missing from /metrics"
        );
    }
    assert!(
        text.contains(r#"ac_http_requests_total{code="201",method="POST",path="/api/v1/execute"}"#),
        "{text}"
    );
}

#[tokio::test]
async fn only_execute_is_rate_limited() {
    let st = stack(StackOpts {
        env: vec![
            ("AC_RATE_LIMIT_UNAUTH_PER_MINUTE", "2"),
            ("AC_RATE_LIMIT_UNAUTH_BURST", "2"),
        ],
        ..StackOpts::default()
    })
    .await;
    // Invalid requests are rejected before the limiter and spend no budget (Go order).
    for _ in 0..5 {
        let r = post_execute(&st, r#"{"language":"python","source_code":"x"}"#, None).await;
        assert_eq!(r.status(), 400);
    }
    let r = post_execute(&st, &run_body("3", ""), None).await;
    assert_eq!(r.status(), 201);
    let v: Value = r.json().await.unwrap();
    let (id, tok) = (
        v["job_id"].as_str().unwrap().to_owned(),
        v["stream_token"].as_str().unwrap().to_owned(),
    );
    st.queue
        .publish(&ae_core::eventlog::Event::new(
            &id,
            EventType::Verdict,
            Some(serde_json::json!({"verdict": "accepted"})),
        ))
        .await
        .unwrap();
    let paths = [
        format!("/api/v1/stream?job_id={id}&stream_token={tok}"),
        format!("/api/v1/submissions/{id}?stream_token={tok}"),
        "/api/v1/health".to_owned(),
        "/api/v1/languages".to_owned(),
        "/livez".to_owned(),
        "/readyz".to_owned(),
    ];
    for _ in 0..4 {
        for p in &paths {
            let r = http().get(format!("{}{p}", st.url)).send().await.unwrap();
            assert_ne!(r.status(), 429, "{p} must not be rate limited");
            let _ = r.bytes().await;
        }
    }
    assert_eq!(
        post_execute(&st, &run_body("3", ""), None).await.status(),
        201
    );
    let r = post_execute(&st, &run_body("3", ""), None).await;
    assert!(r.headers().contains_key("retry-after"));
    assert_eq!(error_of(r).await, (429, "rate limit exceeded".into()));
}

#[tokio::test]
async fn unknown_methods_are_labelled_other() {
    let st = stack(StackOpts::default()).await;
    let m = reqwest::Method::from_bytes(b"BREW").unwrap();
    let _ = http()
        .request(m, format!("{}/livez", st.url))
        .send()
        .await
        .unwrap();
    assert_eq!(
        ae_gateway::metrics::method_label(&axum::http::Method::from_bytes(b"BREW").unwrap()),
        "other"
    );
    let text = String::from_utf8(ae_gateway::metrics::metrics().render()).unwrap();
    assert!(!text.contains("BREW"), "raw method leaked into labels");
    assert!(text.contains(r#"method="other""#));
}
