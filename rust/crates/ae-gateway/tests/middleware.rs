//! Ports of internal/gateway/middleware_test.go plus CORS, request-id and layer order.

mod common;

use ae_gateway::auth::{AuthLayer, AuthSettings, Authenticator, JwksOptions};
use ae_gateway::middleware::{
    CorsLayer, RequestId, RequestLogLayer, normalize_path, parse_trusted_proxies, remote_ip,
    sanitize_log_value,
};
use axum::Router;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::routing::{get, post};
use common::*;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

fn xff(v: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert("x-forwarded-for", HeaderValue::from_str(v).unwrap());
    h
}

fn ip(s: &str) -> Option<IpAddr> {
    Some(s.parse().unwrap())
}

#[test]
fn remote_ip_no_trusted_proxies() {
    assert_eq!(
        remote_ip(ip("1.2.3.4"), &xff("10.0.0.1, 192.168.1.1"), &[]),
        "1.2.3.4"
    );
}

#[test]
fn remote_ip_trusted_proxy() {
    let trusted = parse_trusted_proxies(&["192.168.0.0/16".to_owned()]);
    assert_eq!(
        remote_ip(ip("192.168.1.1"), &xff("8.8.8.8, 192.168.1.1"), &trusted),
        "8.8.8.8"
    );
}

#[test]
fn remote_ip_untrusted_peer_ignores_xff() {
    let trusted = parse_trusted_proxies(&["10.0.0.0/8".to_owned()]);
    assert_eq!(
        remote_ip(ip("1.2.3.4"), &xff("spoofed-ip"), &trusted),
        "1.2.3.4"
    );
}

#[test]
fn remote_ip_rejects_unparseable_forwarded_values() {
    let trusted = parse_trusted_proxies(&["10.0.0.0/8".to_owned()]);
    assert_eq!(
        remote_ip(ip("10.0.0.2"), &xff("not-an-ip"), &trusted),
        "10.0.0.2"
    );
    assert_eq!(
        remote_ip(ip("10.0.0.2"), &xff("8.8.8.8, a:b:c:zz"), &trusted),
        "10.0.0.2"
    );
    let mut h = HeaderMap::new();
    h.insert("x-real-ip", HeaderValue::from_static("x; DEL 1.2.3.4"));
    assert_eq!(remote_ip(ip("10.0.0.2"), &h, &trusted), "10.0.0.2");
}

#[test]
fn remote_ip_rightmost_untrusted_wins_and_x_real_ip_fallback() {
    let trusted = parse_trusted_proxies(&["10.0.0.0/8".to_owned()]);
    // A client-supplied left-most entry cannot override the proxy-appended one.
    assert_eq!(
        remote_ip(ip("10.0.0.2"), &xff("6.6.6.6, 7.7.7.7, 10.0.0.9"), &trusted),
        "7.7.7.7"
    );
    let mut h = xff("10.0.0.3, 10.0.0.4");
    h.insert("x-real-ip", HeaderValue::from_static(" 4.4.4.4 "));
    assert_eq!(remote_ip(ip("10.0.0.2"), &h, &trusted), "4.4.4.4");
    // X-Real-Ip from an untrusted peer is ignored too.
    let mut h = HeaderMap::new();
    h.insert("x-real-ip", HeaderValue::from_static("4.4.4.4"));
    assert_eq!(remote_ip(ip("1.1.1.1"), &h, &trusted), "1.1.1.1");
}

#[test]
fn parse_trusted_proxies_skips_invalid() {
    let nets = parse_trusted_proxies(&[
        "10.0.0.0/8".to_owned(),
        "192.168.1.1".to_owned(),
        "invalid".to_owned(),
    ]);
    assert_eq!(nets.len(), 2);
    assert!(nets[1].contains(&"192.168.1.1".parse::<IpAddr>().unwrap()));
    assert!(!nets[1].contains(&"192.168.1.2".parse::<IpAddr>().unwrap()));
    let v6 = parse_trusted_proxies(&["::1".to_owned()]);
    assert!(v6[0].contains(&"::1".parse::<IpAddr>().unwrap()));
    assert!(!v6[0].contains(&"::2".parse::<IpAddr>().unwrap()));
}

#[test]
fn sanitize_strips_control_characters() {
    assert_eq!(sanitize_log_value("/a\nb\r\tc\u{7f}d"), "/abcd");
}

#[test]
fn normalize_path_known_and_other() {
    assert_eq!(normalize_path("/api/v1/execute"), "/api/v1/execute");
    assert_eq!(normalize_path("/api/v1/stream"), "/api/v1/stream");
    assert_eq!(normalize_path("/api/v1/health"), "/api/v1/health");
    assert_eq!(normalize_path("/api/v1/submissions/x"), "other");
}

// ── CORS ────────────────────────────────────────────────────────────────────

fn cors_app(origins: &str) -> Router {
    Router::new()
        .route("/", get(|| async { "ok" }).post(|| async { "posted" }))
        .layer(CorsLayer::new(origins))
}

#[tokio::test]
async fn cors_allowed_origin_gets_headers_and_preflight_204() {
    let app = cors_app("https://a.test, https://b.test");
    let resp = app
        .clone()
        .oneshot(
            req("OPTIONS", "/")
                .header("origin", "https://b.test")
                .body(empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let h = resp.headers();
    assert_eq!(h["access-control-allow-origin"], "https://b.test");
    assert_eq!(h["access-control-allow-methods"], "GET, POST, OPTIONS");
    assert_eq!(
        h["access-control-allow-headers"],
        "Content-Type, Authorization"
    );
    assert_eq!(h["access-control-max-age"], "86400");
    assert_eq!(h["vary"], "Origin");

    let resp = app
        .oneshot(
            req("GET", "/")
                .header("origin", "https://a.test")
                .body(empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()["access-control-allow-origin"],
        "https://a.test"
    );
}

#[tokio::test]
async fn cors_disallowed_origin_gets_no_headers() {
    let app = cors_app("https://a.test");
    let resp = app
        .oneshot(
            req("GET", "/")
                .header("origin", "https://evil.test")
                .body(empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().get("access-control-allow-origin").is_none());
}

#[tokio::test]
async fn cors_empty_config_is_passthrough() {
    let app = cors_app("");
    let resp = app
        .oneshot(
            req("GET", "/")
                .header("origin", "https://a.test")
                .body(empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(resp.headers().get("access-control-allow-origin").is_none());
}

// ── request id / logging ───────────────────────────────────────────────────

#[tokio::test]
async fn request_id_header_and_extension() {
    let app = Router::new()
        .route(
            "/",
            get(|req: axum::extract::Request| async move {
                req.extensions().get::<RequestId>().unwrap().0.clone()
            }),
        )
        .layer(RequestLogLayer::new());
    let resp = app
        .oneshot(req("GET", "/").body(empty()).unwrap())
        .await
        .unwrap();
    let hdr = resp.headers()["x-request-id"].to_str().unwrap().to_owned();
    let body = body_string(resp).await;
    assert_eq!(hdr, body);
    assert_eq!(uuid::Uuid::parse_str(&hdr).unwrap().get_version_num(), 7);
}

// ── layer order: logging, CORS, auth ───────────────────────────────────────

#[tokio::test]
async fn preflight_passes_without_token_and_401_carries_request_id() {
    let key = rsa_key("test-key");
    let jwks = JwksServer::start(vec![key.jwk.clone()]).await;
    let auth = Authenticator::new(
        AuthSettings {
            issuer: ISSUER.to_owned(),
            audience: AUDIENCE.to_owned(),
            jwks_url: jwks.url.clone(),
        },
        JwksOptions {
            timeout: Duration::from_secs(2),
            ..JwksOptions::default()
        },
        Arc::new(FakeStore::default()),
    )
    .await;
    let app = Router::new()
        .route("/api/v1/execute", post(|| async { "ran" }))
        .layer(AuthLayer::new(auth))
        .layer(CorsLayer::new("https://a.test"))
        .layer(RequestLogLayer::new());

    let resp = app
        .clone()
        .oneshot(
            req("OPTIONS", "/api/v1/execute")
                .header("origin", "https://a.test")
                .header("authorization", "Bearer bad")
                .body(empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let resp = app
        .oneshot(
            req("POST", "/api/v1/execute")
                .header("origin", "https://a.test")
                .header("authorization", "Bearer bad")
                .body(empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert!(resp.headers().get("x-request-id").is_some());
    assert_eq!(
        resp.headers()["access-control-allow-origin"],
        "https://a.test"
    );
}
