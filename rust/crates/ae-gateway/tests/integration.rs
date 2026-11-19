//! Port of `internal/gateway/integration_test.go`: POST /execute -> Redis -> worker ->
//! fake sandbox -> SSE VERDICT, over the real server path (`serve` with connect info).

mod common;

use ae_core::model::{DEFAULT_TENANT_ID, Status, Submission};
use common::{StackOpts, http, post_execute, read_sse, run_body, stack};
use serde_json::Value;
use std::time::Duration;

/// Executes `body` (one python test; the sandbox prints "3"), streams it and returns
/// the VERDICT data after checking the event sequence. The judge is held until
/// QUEUED arrives, so the VERDICT can only come over the live subscription.
async fn execute_and_stream(st: &common::Stack, body: &str) -> Value {
    let gate = st.sb.gate.clone().write_owned().await;
    let r = post_execute(st, body, None).await;
    assert_eq!(r.status(), 201);
    let exec: Value = r.json().await.unwrap();
    let (id, tok) = (
        exec["job_id"].as_str().unwrap().to_owned(),
        exec["stream_token"].as_str().unwrap().to_owned(),
    );
    for bad in [String::new(), format!("{tok}x")] {
        let r = http()
            .get(format!(
                "{}/api/v1/stream?job_id={id}&stream_token={bad}",
                st.url
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 401);
    }
    let mut resp = http()
        .get(format!(
            "{}/api/v1/stream?job_id={id}&stream_token={tok}",
            st.url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let first = resp.chunk().await.unwrap().unwrap();
    let mut raw = String::from_utf8_lossy(&first).into_owned();
    assert!(raw.contains("event: QUEUED"), "{raw}");
    drop(gate);
    let (_, rest) = read_sse(resp, Duration::from_secs(15)).await;
    raw.push_str(&rest);
    let events = common::parse_sse(&raw);
    let kinds: Vec<&str> = events.iter().map(|e| e["type"].as_str().unwrap()).collect();
    assert_eq!(kinds.first(), Some(&"QUEUED"), "{raw}");
    assert_eq!(kinds.last(), Some(&"VERDICT"), "{raw}");
    assert!(kinds.contains(&"RUNNING"), "{raw}");
    assert!(kinds.contains(&"TEST_RESULT"), "{raw}");
    for e in &events {
        assert_eq!(e["job_id"], id.as_str());
    }
    let data = events.last().unwrap()["data"].clone();
    assert_eq!(data["test_count"], 1);
    assert_eq!(data["results"].as_array().unwrap().len(), 1);
    assert_eq!(data["results"][0]["stdout"], "3");
    data
}

#[tokio::test]
async fn execute_and_stream_verdicts() {
    let st = stack(StackOpts {
        worker: true,
        ..StackOpts::default()
    })
    .await;
    let v = execute_and_stream(&st, &run_body("3", "")).await;
    assert_eq!(
        (v["verdict"].as_str(), v["tests_passed"].as_u64()),
        (Some("accepted"), Some(1))
    );
    let v = execute_and_stream(&st, &run_body("4", "")).await;
    assert_eq!(
        (v["verdict"].as_str(), v["tests_passed"].as_u64()),
        (Some("wrong_answer"), Some(0))
    );
    let v = execute_and_stream(&st, &run_body("3.0000001", "float")).await;
    assert_eq!(v["verdict"], "accepted");
    let v = execute_and_stream(&st, &run_body("3.0000001", "")).await;
    assert_eq!(v["verdict"], "wrong_answer");
}

#[tokio::test]
async fn authenticated_caller_gets_stream_token() {
    let st = stack(StackOpts {
        auth: true,
        ..StackOpts::default()
    })
    .await;
    let r = post_execute(&st, &run_body("3", ""), Some(&st.token("u1"))).await;
    assert_eq!(r.status(), 201);
    let v: Value = r.json().await.unwrap();
    assert!(!v["stream_token"].as_str().unwrap().is_empty());
    assert!(st.queue.depth("ac:submissions:run").await.unwrap() > 0);
    let sub = st
        .db
        .get_submission(v["job_id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(sub.user_id.is_some(), "owned by the caller");

    // An invalid bearer token is 401, never anonymous.
    let r = post_execute(&st, &run_body("3", ""), Some("garbage")).await;
    assert_eq!(r.status(), 401);
}

#[tokio::test]
async fn cancel_unauth_requires_stream_token() {
    let st = stack(StackOpts {
        worker: true,
        ..StackOpts::default()
    })
    .await;
    let r = post_execute(&st, &run_body("3", ""), None).await;
    let v: Value = r.json().await.unwrap();
    let url = format!(
        "{}/api/v1/submissions/{}",
        st.url,
        v["job_id"].as_str().unwrap()
    );
    let r = http().delete(&url).send().await.unwrap();
    assert_eq!(r.status(), 401);
    assert!(
        r.text()
            .await
            .unwrap()
            .contains("invalid or missing stream_token")
    );
    let r = http()
        .delete(format!(
            "{url}?stream_token={}",
            v["stream_token"].as_str().unwrap()
        ))
        .send()
        .await
        .unwrap();
    // The worker may already have claimed the job.
    assert!(matches!(r.status().as_u16(), 200 | 409), "{}", r.status());
}

#[tokio::test]
async fn in_flight_job_without_tests_fails_cleanly() {
    let st = stack(StackOpts {
        worker: true,
        ..StackOpts::default()
    })
    .await;
    let id = uuid::Uuid::new_v4().to_string();
    st.db
        .insert_submission(&Submission {
            id: id.clone(),
            tenant_id: DEFAULT_TENANT_ID.into(),
            user_id: None,
            idempotency_key: None,
            language: "python".into(),
            source_code: "print(3)".into(),
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
    let tok = st.queue.issue_stream_token(&id).await.unwrap();
    st.queue
        .enqueue("ac:submissions:submit", &id)
        .await
        .unwrap();
    let resp = http()
        .get(format!(
            "{}/api/v1/stream?job_id={id}&stream_token={tok}",
            st.url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let (events, raw) = read_sse(resp, Duration::from_secs(15)).await;
    let verdict = events.iter().find(|e| e["type"] == "VERDICT").expect(&raw);
    assert_eq!(verdict["data"]["verdict"], "internal_error");
}
