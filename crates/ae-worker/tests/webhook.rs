//! Webhook delivery. Ports webhook_test.go: HMAC signing, the SSRF guard and
//! redirect refusal.

use ae_worker::webhook::{WebhookClient, WebhookError, WebhookPayload, is_blocked, sign};
use std::net::IpAddr;
use std::time::Duration;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn payload() -> WebhookPayload {
    WebhookPayload {
        submission_id: "sub-12345".into(),
        verdict: "accepted".into(),
        tests_passed: 5,
        test_count: 5,
        cpu_time_ns: 150_000_000,
        memory_bytes: 1_048_576,
        run_time_ns: 180_000_000,
        timestamp: "2026-11-09T00:00:00Z".into(),
        results: Vec::new(),
    }
}

#[tokio::test]
async fn delivers_signed_payload() {
    let server = MockServer::start().await;
    let p = payload();
    let body = serde_json::to_vec(&p).unwrap();
    let sig = sign("test-secret-key", &body);
    Mock::given(method("POST"))
        .and(path("/hook"))
        .and(header("content-type", "application/json"))
        .and(header("user-agent", "AetherCode-Webhook-Dispatcher/1.0"))
        .and(header("x-aethercode-signature", sig.as_str()))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    let ok = WebhookClient::allowing_private_addresses()
        .dispatch(&format!("{}/hook", server.uri()), "test-secret-key", &p)
        .await;
    assert!(ok);
    let got: serde_json::Value =
        serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
    assert_eq!(got["submission_id"], "sub-12345");
    assert!(got.get("results").is_none(), "empty results are omitted");
}

#[test]
fn signature_is_hex_hmac_sha256() {
    // RFC 4231 test case 2.
    assert_eq!(
        sign("Jefe", b"what do ya want for nothing?"),
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
}

#[tokio::test]
async fn no_secret_means_no_signature_header() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    assert!(
        WebhookClient::allowing_private_addresses()
            .dispatch(&server.uri(), "", &payload())
            .await
    );
    let req = &server.received_requests().await.unwrap()[0];
    assert!(req.headers.get("x-aethercode-signature").is_none());
}

#[tokio::test]
async fn retries_three_times_on_non_2xx() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .expect(3)
        .mount(&server)
        .await;
    let start = std::time::Instant::now();
    assert!(
        !WebhookClient::allowing_private_addresses()
            .dispatch(&server.uri(), "s", &payload())
            .await
    );
    assert!(
        start.elapsed() >= Duration::from_millis(1000),
        "500ms backoff"
    );
}

#[test]
fn ssrf_blocked_ranges() {
    for ip in [
        "127.0.0.1",
        "127.255.0.1",
        "10.0.0.1",
        "10.255.255.255",
        "172.16.0.1",
        "172.31.255.255",
        "192.168.1.1",
        "169.254.169.254",
        "169.254.0.1",
        "100.64.0.1",
        "192.0.0.8",
        "198.18.0.1",
        "198.51.100.7",
        "203.0.113.9",
        "240.0.0.1",
        "0.0.0.0",
        "::1",
        "::",
        "fc00::1",
        "fd12::1",
        "fe80::1",
        "64:ff9b::a00:1",
        "64:ff9b:1::1",
        "::ffff:127.0.0.1",
        "::ffff:10.0.0.1",
    ] {
        assert!(
            is_blocked(ip.parse::<IpAddr>().unwrap()),
            "{ip} not blocked"
        );
    }
    for ip in ["8.8.8.8", "1.1.1.1", "172.32.0.1", "2606:4700::1111"] {
        assert!(!is_blocked(ip.parse::<IpAddr>().unwrap()), "{ip} blocked");
    }
}

#[tokio::test]
async fn guarded_client_refuses_loopback_and_private_targets() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;
    let port = server.address().port();
    let client = WebhookClient::new();
    for url in [
        format!("http://127.0.0.1:{port}/"),
        format!("http://localhost:{port}/"),
        format!("http://[::1]:{port}/"),
        "http://169.254.169.254/latest/meta-data/".to_owned(),
        "http://10.0.0.1/".to_owned(),
    ] {
        let err = client.send_once(&url, "", b"{}").await.unwrap_err();
        assert!(
            matches!(err, WebhookError::Blocked),
            "{url}: want an SSRF block, got {err:?}"
        );
    }
}

#[tokio::test]
async fn redirects_are_not_followed() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/redirect"))
        .respond_with(ResponseTemplate::new(302).insert_header("location", "/target"))
        .mount(&server)
        .await;
    Mock::given(path("/target"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;
    let err = WebhookClient::allowing_private_addresses()
        .send_once(&format!("{}/redirect", server.uri()), "", b"{}")
        .await
        .unwrap_err();
    assert!(matches!(err, WebhookError::Redirect), "got {err:?}");
}

#[tokio::test]
async fn non_http_schemes_are_rejected() {
    let err = WebhookClient::new()
        .send_once("file:///etc/passwd", "", b"{}")
        .await
        .unwrap_err();
    assert!(matches!(err, WebhookError::Url(_)), "got {err:?}");
}
