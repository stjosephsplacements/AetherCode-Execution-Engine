//! Trace context on execute: the job carries the gateway's traceparent.

mod common;

use common::{StackOpts, http, run_body, stack};
use serde_json::Value;

fn valid(tp: &str) -> bool {
    let p: Vec<&str> = tp.split('-').collect();
    p.len() == 4
        && p[0] == "00"
        && p[1].len() == 32
        && p[2].len() == 16
        && p[1].chars().any(|c| c != '0')
        && p[2].chars().any(|c| c != '0')
}

async fn job_traceparent(st: &common::Stack, header: Option<&str>) -> String {
    let mut r = http()
        .post(format!("{}/api/v1/execute", st.url))
        .header("content-type", "application/json")
        .body(run_body("3", ""));
    if let Some(h) = header {
        r = r.header("traceparent", h);
    }
    let r = r.send().await.unwrap();
    assert_eq!(r.status(), 201);
    let v: Value = r.json().await.unwrap();
    let spec = st
        .queue
        .get_tests(v["job_id"].as_str().unwrap())
        .await
        .unwrap();
    spec.set.traceparent
}

#[tokio::test]
async fn job_carries_a_traceparent_without_an_incoming_one() {
    let st = stack(StackOpts::default()).await;
    let a = job_traceparent(&st, None).await;
    let b = job_traceparent(&st, None).await;
    assert!(valid(&a), "{a:?}");
    assert_ne!(
        a.split('-').nth(1),
        b.split('-').nth(1),
        "fresh trace per request"
    );
}

#[tokio::test]
async fn incoming_trace_id_is_preserved() {
    let st = stack(StackOpts::default()).await;
    let incoming = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let tp = job_traceparent(&st, Some(incoming)).await;
    assert!(valid(&tp), "{tp:?}");
    assert_eq!(
        tp.split('-').nth(1),
        Some("4bf92f3577b34da6a3ce929d0e0e4736")
    );
    // A malformed header starts a new trace instead of being forwarded.
    let tp = job_traceparent(&st, Some("garbage")).await;
    assert!(valid(&tp), "{tp:?}");
}
