//! Ports of internal/ratelimit/limiter_test.go plus the layer behaviour from
//! internal/gateway/handler.go (per-user and per-IP limits, trusted proxies).

mod common;

use ae_core::model::DEFAULT_TENANT_ID;
use ae_gateway::auth::{Identity, Role};
use ae_gateway::middleware::parse_trusted_proxies;
use ae_gateway::ratelimit::{Limiter, RateLimitLayer};
use axum::Router;
use axum::extract::ConnectInfo;
use axum::http::StatusCode;
use axum::routing::get;
use common::*;
use std::net::SocketAddr;
use std::time::Duration;
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn first_request_allowed() {
    let (_c, addr) = redis().await;
    let l = Limiter::new(redis_conn(&addr).await, 10, 5);
    assert!(l.check(Uuid::new_v4()).await.allowed);
}

#[tokio::test]
async fn burst_limit() {
    let (_c, addr) = redis().await;
    let l = Limiter::new(redis_conn(&addr).await, 100, 2);
    let user = Uuid::new_v4();
    for i in 0..2 {
        assert!(l.check(user).await.allowed, "request {i}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let d = l.check(user).await;
    assert!(!d.allowed);
    assert_eq!(d.retry_after, Duration::from_secs(10));
    // Another user is unaffected.
    assert!(l.check(Uuid::new_v4()).await.allowed);
}

#[tokio::test]
async fn minute_limit_retry_after_from_oldest_entry() {
    let (_c, addr) = redis().await;
    let l = Limiter::new(redis_conn(&addr).await, 3, 100);
    let user = Uuid::new_v4();
    for _ in 0..3 {
        assert!(l.check(user).await.allowed);
    }
    let d = l.check(user).await;
    assert!(!d.allowed);
    assert!(
        d.retry_after > Duration::from_secs(58) && d.retry_after <= Duration::from_secs(60),
        "{:?}",
        d.retry_after
    );
}

#[tokio::test]
async fn keys_match_go() {
    let (_c, addr) = redis().await;
    let conn = redis_conn(&addr).await;
    let l = Limiter::new(conn.clone(), 10, 5);
    let user = Uuid::new_v4();
    l.check(user).await;
    l.check_ip("9.9.9.9").await;
    let mut c = conn;
    let n: i64 = redis::cmd("ZCARD")
        .arg(format!("ac:ratelimit:{user}"))
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(n, 1);
    let n: i64 = redis::cmd("ZCARD")
        .arg("ac:ratelimit:ip:9.9.9.9")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(n, 1);
    let ttl: i64 = redis::cmd("TTL")
        .arg(format!("ac:ratelimit:{user}"))
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(ttl, 61);
}

#[tokio::test]
async fn ip_limit_disabled_when_per_minute_zero() {
    let (_c, addr) = redis().await;
    let l = Limiter::new(redis_conn(&addr).await, 0, 0);
    for _ in 0..5 {
        assert!(l.check_ip("1.1.1.1").await.allowed);
    }
}

#[tokio::test]
async fn fails_open_when_redis_is_down() {
    let (c, addr) = redis().await;
    let l = Limiter::new(redis_conn(&addr).await, 1, 1);
    let user = Uuid::new_v4();
    assert!(l.check(user).await.allowed);
    assert!(!l.check(user).await.allowed);
    c.stop().await.unwrap();
    assert!(l.check(user).await.allowed);
}

// ── layer ───────────────────────────────────────────────────────────────────

fn app(layer: RateLimitLayer) -> Router {
    Router::new()
        .route("/", get(|| async { "ok" }))
        .layer(layer)
}

async fn hit(
    app: &Router,
    peer: &str,
    xff: Option<&str>,
    identity: Option<&Identity>,
) -> (StatusCode, Option<String>, String) {
    let mut b = req("GET", "/");
    if let Some(x) = xff {
        b = b.header("x-forwarded-for", x);
    }
    let mut r = b.body(empty()).unwrap();
    let addr: SocketAddr = peer.parse().unwrap();
    r.extensions_mut().insert(ConnectInfo(addr));
    if let Some(id) = identity {
        r.extensions_mut().insert(id.clone());
    }
    let resp = app.clone().oneshot(r).await.unwrap();
    let status = resp.status();
    let ra = resp
        .headers()
        .get("retry-after")
        .map(|v| v.to_str().unwrap().to_owned());
    (status, ra, body_string(resp).await)
}

fn identity() -> Identity {
    Identity {
        tenant_id: Uuid::parse_str(DEFAULT_TENANT_ID).unwrap(),
        user_id: Uuid::new_v4(),
        external_sub: "u".to_owned(),
        email: String::new(),
        role: Role::Student,
    }
}

#[tokio::test]
async fn user_limit_applies_to_authenticated_callers() {
    let (_c, addr) = redis().await;
    let conn = redis_conn(&addr).await;
    let app = app(RateLimitLayer::new(
        Limiter::new(conn, 100, 1),
        None,
        vec![],
    ));
    let id = identity();
    assert_eq!(
        hit(&app, "1.2.3.4:1", None, Some(&id)).await.0,
        StatusCode::OK
    );
    let (status, ra, body) = hit(&app, "1.2.3.4:1", None, Some(&id)).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(ra.as_deref(), Some("10"));
    assert_eq!(body, "{\"error\":\"rate limit exceeded\"}\n");
    // Unauthenticated callers are not limited when the IP limiter is off.
    for _ in 0..3 {
        assert_eq!(hit(&app, "1.2.3.4:1", None, None).await.0, StatusCode::OK);
    }
}

#[tokio::test]
async fn unauth_ip_limit_applies() {
    let (_c, addr) = redis().await;
    let conn = redis_conn(&addr).await;
    let app = app(RateLimitLayer::new(
        Limiter::new(conn.clone(), 100, 100),
        Some(Limiter::new(conn, 100, 2)),
        vec![],
    ));
    for _ in 0..2 {
        assert_eq!(
            hit(&app, "5.6.7.8:40000", None, None).await.0,
            StatusCode::OK
        );
    }
    let (status, ra, _) = hit(&app, "5.6.7.8:40001", None, None).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(ra.as_deref(), Some("10"));
    // A different client IP has its own bucket.
    assert_eq!(
        hit(&app, "5.6.7.9:40000", None, None).await.0,
        StatusCode::OK
    );
    // Authenticated callers use the per-user bucket, not the IP one.
    assert_eq!(
        hit(&app, "5.6.7.8:40000", None, Some(&identity())).await.0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn xff_from_untrusted_peer_is_ignored() {
    let (_c, addr) = redis().await;
    let conn = redis_conn(&addr).await;
    let app = app(RateLimitLayer::new(
        Limiter::new(conn.clone(), 100, 100),
        Some(Limiter::new(conn, 100, 1)),
        parse_trusted_proxies(&["10.0.0.0/8".to_owned()]),
    ));
    // Untrusted peer rotating a spoofed XFF still shares one bucket (its own address).
    assert_eq!(
        hit(&app, "1.2.3.4:1", Some("8.8.8.1"), None).await.0,
        StatusCode::OK
    );
    assert_eq!(
        hit(&app, "1.2.3.4:1", Some("8.8.8.2"), None).await.0,
        StatusCode::TOO_MANY_REQUESTS
    );
    // Behind a trusted proxy, distinct forwarded clients get distinct buckets.
    assert_eq!(
        hit(&app, "10.0.0.5:1", Some("9.9.9.1"), None).await.0,
        StatusCode::OK
    );
    assert_eq!(
        hit(&app, "10.0.0.5:1", Some("9.9.9.2"), None).await.0,
        StatusCode::OK
    );
    assert_eq!(
        hit(&app, "10.0.0.6:1", Some("9.9.9.1"), None).await.0,
        StatusCode::TOO_MANY_REQUESTS
    );
}
