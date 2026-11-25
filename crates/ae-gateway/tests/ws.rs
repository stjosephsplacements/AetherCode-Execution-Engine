//! WebSocket stream: same auth gates as SSE before upgrade.

mod common;

use common::{StackOpts, http, stack};

async fn issue(st: &common::Stack) -> (String, String) {
    let id = uuid::Uuid::now_v7().to_string();
    let tok = st.queue.issue_stream_token(&id).await.unwrap();
    (id, tok)
}

fn ws_url(st: &common::Stack, id: &str, tok: &str) -> String {
    format!("{}/api/v1/ws?job_id={id}&stream_token={tok}", st.url)
}

#[tokio::test]
async fn ws_requires_stream_token_when_unauthenticated() {
    let st = stack(StackOpts::default()).await;
    let (id, tok) = issue(&st).await;
    for bad in ["", "nope", &format!("{tok}x")] {
        let r = http().get(ws_url(&st, &id, bad)).send().await.unwrap();
        assert_eq!(r.status(), 401, "token {bad:?}");
        assert_eq!(
            r.text().await.unwrap(),
            "{\"error\":\"invalid or missing stream_token\"}\n"
        );
    }
    let r = http()
        .get(format!("{}/api/v1/ws", st.url))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert_eq!(
        r.text().await.unwrap(),
        "{\"error\":\"job_id is required\"}\n"
    );
    let r = http()
        .get(format!("{}/api/v1/ws?job_id=x", st.url))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert_eq!(r.text().await.unwrap(), "{\"error\":\"invalid job_id\"}\n");
}