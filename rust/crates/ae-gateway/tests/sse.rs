//! SSE: token gate, replay then live from the shared subscription, close on VERDICT,
//! Last-Event-ID, and releasing the subscription when the client goes away.

mod common;

use ae_core::eventlog::{Event, EventType};
use common::{StackOpts, http, read_sse, stack};
use serde_json::json;
use std::time::{Duration, Instant};

async fn issue(st: &common::Stack) -> (String, String) {
    let id = uuid::Uuid::now_v7().to_string();
    let tok = st.queue.issue_stream_token(&id).await.unwrap();
    (id, tok)
}

fn stream_url(st: &common::Stack, id: &str, tok: &str) -> String {
    format!("{}/api/v1/stream?job_id={id}&stream_token={tok}", st.url)
}

async fn wait_subscribers(st: &common::Stack, id: &str, want: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while st.queue.subscriber_count(id).await != want {
        assert!(
            Instant::now() < deadline,
            "subscribers never reached {want}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn sse_requires_stream_token_when_unauthenticated() {
    let st = stack(StackOpts::default()).await;
    let (id, tok) = issue(&st).await;
    for bad in ["", "nope", &format!("{tok}x")] {
        let r = http().get(stream_url(&st, &id, bad)).send().await.unwrap();
        assert_eq!(r.status(), 401, "token {bad:?}");
        assert_eq!(
            r.text().await.unwrap(),
            "{\"error\":\"invalid or missing stream_token\"}\n"
        );
    }
    // Another job's token does not open this job.
    let (_, other_tok) = issue(&st).await;
    let r = http()
        .get(stream_url(&st, &id, &other_tok))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);

    let r = http()
        .get(format!("{}/api/v1/stream", st.url))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert_eq!(
        r.text().await.unwrap(),
        "{\"error\":\"job_id is required\"}\n"
    );
    let r = http()
        .get(format!("{}/api/v1/stream?job_id=x", st.url))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert_eq!(r.text().await.unwrap(), "{\"error\":\"invalid job_id\"}\n");
}

#[tokio::test]
async fn sse_replays_then_streams_and_closes_on_verdict() {
    let st = stack(StackOpts::default()).await;
    let (id, tok) = issue(&st).await;
    let queued = Event::new(&id, EventType::Queued, None);
    st.queue.publish(&queued).await.unwrap();

    let resp = http().get(stream_url(&st, &id, &tok)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let h = resp.headers();
    assert_eq!(h["content-type"], "text/event-stream");
    assert_eq!(h["cache-control"], "no-cache");
    assert_eq!(h["x-accel-buffering"], "no");
    wait_subscribers(&st, &id, 1).await;

    // A second client shares the one hub subscription.
    let resp2 = http().get(stream_url(&st, &id, &tok)).send().await.unwrap();
    wait_subscribers(&st, &id, 2).await;

    let q = st.queue.clone();
    let id2 = id.clone();
    tokio::spawn(async move {
        q.publish(&Event::new(&id2, EventType::Running, None))
            .await
            .unwrap();
        q.publish(&Event::new(
            &id2,
            EventType::Verdict,
            Some(json!({"verdict": "accepted", "tests_passed": 1, "test_count": 1})),
        ))
        .await
        .unwrap();
        // Anything after VERDICT is never delivered: the stream has closed.
        q.publish(&Event::new(&id2, EventType::Running, None))
            .await
            .unwrap();
    });
    for resp in [resp, resp2] {
        let (events, raw) = read_sse(resp, Duration::from_secs(10)).await;
        let kinds: Vec<&str> = events.iter().map(|e| e["type"].as_str().unwrap()).collect();
        assert_eq!(kinds, ["QUEUED", "RUNNING", "VERDICT"], "{raw}");
        assert!(raw.starts_with(&format!("id: {}\nevent: QUEUED\ndata: ", queued.id)));
        assert_eq!(events[2]["data"]["verdict"], "accepted");
        assert_eq!(events[0]["job_id"], id.as_str());
    }
    wait_subscribers(&st, &id, 0).await;

    // A finished job replays to VERDICT and closes without subscribing live.
    let resp = http().get(stream_url(&st, &id, &tok)).send().await.unwrap();
    let (events, _) = read_sse(resp, Duration::from_secs(5)).await;
    assert_eq!(
        events.len(),
        4,
        "QUEUED, RUNNING, VERDICT and the late RUNNING in the log"
    );
    assert_eq!(events[2]["type"], "VERDICT");

    // Last-Event-ID skips what the client already has.
    let log = st.queue.replay(&id).await.unwrap();
    let resp = http()
        .get(stream_url(&st, &id, &tok))
        .header("last-event-id", &log[0].id)
        .send()
        .await
        .unwrap();
    let (events, _) = read_sse(resp, Duration::from_secs(5)).await;
    assert_eq!(events[0]["type"], "RUNNING");
    assert_eq!(events[1]["type"], "VERDICT");
}

#[tokio::test]
async fn sse_client_disconnect_releases_subscription() {
    let st = stack(StackOpts::default()).await;
    let (id, tok) = issue(&st).await;
    st.queue
        .publish(&Event::new(&id, EventType::Queued, None))
        .await
        .unwrap();
    let mut resp = http().get(stream_url(&st, &id, &tok)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let first = resp.chunk().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&first).contains("event: QUEUED"));
    wait_subscribers(&st, &id, 1).await;
    drop(resp);
    wait_subscribers(&st, &id, 0).await;
}

#[tokio::test]
async fn sse_authenticated_owner_only() {
    let st = stack(StackOpts {
        auth: true,
        ..StackOpts::default()
    })
    .await;
    let owner = st.token("owner");
    let r = common::post_execute(&st, &common::run_body("3", ""), Some(&owner)).await;
    let v: serde_json::Value = r.json().await.unwrap();
    let id = v["job_id"].as_str().unwrap().to_owned();
    let url = format!("{}/api/v1/stream?job_id={id}", st.url);

    let r = http()
        .get(&url)
        .bearer_auth(st.token("intruder"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    assert_eq!(r.text().await.unwrap(), "{\"error\":\"not found\"}\n");

    let mut r = http().get(&url).bearer_auth(&owner).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let first = r.chunk().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&first).contains("event: QUEUED"));
}
