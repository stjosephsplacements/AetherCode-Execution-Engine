//! Webhook logs never carry the callback URL's path, query or credentials.
//! Alone in its binary: a scoped tracing subscriber misses events whose
//! callsite interest another concurrent test already cached.

use ae_worker::webhook::{WebhookClient, WebhookPayload};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

fn payload() -> WebhookPayload {
    WebhookPayload {
        submission_id: "sub-1".into(),
        verdict: "accepted".into(),
        tests_passed: 1,
        test_count: 1,
        cpu_time_ns: 0,
        memory_bytes: 0,
        run_time_ns: 0,
        timestamp: "2026-11-09T00:00:00Z".into(),
        results: Vec::new(),
    }
}

#[derive(Clone, Default)]
struct Capture(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Capture {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn logs_never_contain_path_query_or_credentials() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let cap = Capture::default();
    let w = cap.clone();
    let sub = tracing_subscriber::fmt()
        .with_writer(move || w.clone())
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .finish();
    let _g = tracing::subscriber::set_default(sub);
    let addr = server.address();
    let url = format!("http://user:pass@{addr}/hook/private?token=SECRET");
    let client = WebhookClient::allowing_private_addresses();
    client.dispatch(&url, "s3cret", &payload()).await;
    // A successful delivery is logged too.
    let ok = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&ok)
        .await;
    let url = format!(
        "http://user:pass@{}/hook/private?token=SECRET",
        ok.address()
    );
    assert!(client.dispatch(&url, "s3cret", &payload()).await);
    // Transport error path: connection refused, reqwest's error text carries the URL.
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let refused = closed.local_addr().unwrap();
    drop(closed);
    let url = format!("http://user:pass@{refused}/hook/private?token=SECRET");
    assert!(!client.dispatch(&url, "s3cret", &payload()).await);
    let out = String::from_utf8(cap.0.lock().unwrap().clone()).unwrap();
    assert!(out.contains(&format!("http://{}", ok.address())), "{out}");
    assert!(out.contains("webhook attempt failed"), "{out}");
    for secret in [
        "SECRET",
        "token",
        "user:pass",
        "pass@",
        "/hook/private",
        "s3cret",
    ] {
        assert!(!out.contains(secret), "{secret} leaked: {out}");
    }
}
