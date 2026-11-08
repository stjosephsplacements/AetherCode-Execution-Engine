//! Port of TestAdmissionCheck plus drain-rate Retry-After and cost-based shedding.

mod common;

use ae_core::Config;
use ae_core::model::Verdict;
use ae_gateway::admission::{Admission, AdmissionLayer, Drain, Shed};
use ae_queue::{BoxFuture, ExpiryStore, Queue};
use axum::Router;
use axum::http::StatusCode;
use axum::routing::post;
use common::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tower::ServiceExt;

struct NoStore;
impl ExpiryStore for NoStore {
    fn fail_expired<'a>(
        &'a self,
        _job_id: &'a str,
        _verdict: Verdict,
        _message: &'a str,
    ) -> BoxFuture<'a, ae_queue::Result<bool>> {
        Box::pin(async { Ok(true) })
    }
}

const STREAM: &str = "ac:submissions:run";

async fn queue(addr: &str) -> Queue {
    let m = HashMap::from([("AC_REDIS_ADDR".to_owned(), addr.to_owned())]);
    Queue::connect(&Config::from_map(&m).unwrap(), Arc::new(NoStore))
        .await
        .unwrap()
}

async fn fill(q: &Queue, n: usize) {
    for i in 0..n {
        q.enqueue(STREAM, &format!("job-{i}")).await.unwrap();
    }
}

/// Clock that only moves when the test says so.
fn manual_clock() -> (Arc<Mutex<Instant>>, Arc<Drain>) {
    let t = Arc::new(Mutex::new(Instant::now()));
    let tc = t.clone();
    let drain = Arc::new(Drain::with_clock(Arc::new(move || *tc.lock().unwrap())));
    (t, drain)
}

fn advance(t: &Mutex<Instant>, d: Duration) {
    let mut g = t.lock().unwrap();
    *g += d;
}

#[test]
fn drain_rate_is_smoothed_per_second() {
    let (t, d) = manual_clock();
    assert!(d.rate() <= 0.0);
    for _ in 0..20 {
        d.complete();
    }
    assert!(d.rate() <= 0.0, "no full window yet");
    advance(&t, Duration::from_secs(1));
    assert!((d.rate() - 6.0).abs() < 1e-9, "{}", d.rate());
    // Idle windows decay toward zero.
    advance(&t, Duration::from_secs(1));
    assert!((d.rate() - 4.2).abs() < 1e-9, "{}", d.rate());
}

#[tokio::test]
async fn admission_check_depth() {
    let (_c, addr) = redis().await;
    let q = queue(&addr).await;
    let a = Admission::new(q.clone(), 100, Arc::new(Drain::new()));
    assert!(a.check(STREAM, 1.0).await.is_ok());
    fill(&q, 150).await;
    assert!(a.check(STREAM, 1.0).await.is_err());
}

#[tokio::test]
async fn admits_up_to_max_depth_like_go() {
    let (_c, addr) = redis().await;
    let q = queue(&addr).await;
    let a = Admission::new(q.clone(), 10, Arc::new(Drain::new()));
    fill(&q, 9).await;
    assert!(a.check(STREAM, 1.0).await.is_ok());
    fill(&q, 1).await;
    assert!(a.check(STREAM, 1.0).await.is_err());
}

#[tokio::test]
async fn sheds_by_estimated_cost() {
    let (_c, addr) = redis().await;
    let q = queue(&addr).await;
    let a = Admission::new(q.clone(), 10, Arc::new(Drain::new()));
    fill(&q, 5).await;
    assert!(a.check(STREAM, 1.0).await.is_ok());
    assert!(a.check(STREAM, 5.0).await.is_ok());
    assert!(a.check(STREAM, 6.0).await.is_err());
}

#[tokio::test]
async fn retry_after_without_drain_rate_is_ten_seconds() {
    let (_c, addr) = redis().await;
    let q = queue(&addr).await;
    let a = Admission::new(q.clone(), 1, Arc::new(Drain::new()));
    fill(&q, 5).await;
    let Shed { retry_after } = a.check(STREAM, 1.0).await.unwrap_err();
    assert_eq!(retry_after, Duration::from_secs(10));
}

#[tokio::test]
async fn shed_returns_429_with_retry_after_from_drain_rate() {
    let (_c, addr) = redis().await;
    let q = queue(&addr).await;
    let (t, drain) = manual_clock();
    for _ in 0..20 {
        drain.complete();
    }
    advance(&t, Duration::from_secs(1)); // rate = 6/s
    let a = Arc::new(Admission::new(q.clone(), 10, drain.clone()));
    fill(&q, 30).await;
    // Excess = 30 - 10 + 1 = 21 jobs at 6/s -> ceil(3.5) = 4 s.
    let app = Router::new()
        .route("/api/v1/execute", post(|| async { "queued" }))
        .layer(AdmissionLayer::new(a.clone(), STREAM, 1.0));
    let resp = app
        .clone()
        .oneshot(req("POST", "/api/v1/execute").body(empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(resp.headers()["retry-after"], "4");
    assert_eq!(
        body_string(resp).await,
        "{\"error\":\"server is busy, try again later\"}\n"
    );

    // Clamped to [1, 60].
    fill(&q, 1000).await;
    assert_eq!(
        a.check(STREAM, 1.0).await.unwrap_err().retry_after,
        Duration::from_secs(60)
    );
}

#[tokio::test]
async fn retry_after_clamped_to_one_second_minimum() {
    let (_c, addr) = redis().await;
    let q = queue(&addr).await;
    let (t, drain) = manual_clock();
    for _ in 0..1000 {
        drain.complete();
    }
    advance(&t, Duration::from_secs(1)); // rate = 300/s
    let a = Admission::new(q.clone(), 1, drain);
    fill(&q, 2).await;
    assert_eq!(
        a.check(STREAM, 1.0).await.unwrap_err().retry_after,
        Duration::from_secs(1)
    );
}

#[tokio::test]
async fn admits_under_limit_through_layer() {
    let (_c, addr) = redis().await;
    let q = queue(&addr).await;
    let a = Arc::new(Admission::new(q, 10, Arc::new(Drain::new())));
    let app = Router::new()
        .route("/", post(|| async { "queued" }))
        .layer(AdmissionLayer::new(a, STREAM, 1.0));
    let resp = app
        .oneshot(req("POST", "/").body(empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn fails_open_when_redis_is_down() {
    let (c, addr) = redis().await;
    let q = queue(&addr).await;
    let a = Admission::new(q, 1, Arc::new(Drain::new()));
    c.stop().await.unwrap();
    assert!(a.check(STREAM, 1.0).await.is_ok());
}

#[tokio::test]
async fn queue_age_reports_oldest_entry() {
    let (_c, addr) = redis().await;
    let q = queue(&addr).await;
    let a = Admission::new(q.clone(), 10, Arc::new(Drain::new()));
    assert_eq!(a.queue_age(STREAM).await.unwrap(), Duration::ZERO);
    fill(&q, 1).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(a.queue_age(STREAM).await.unwrap() >= Duration::from_millis(40));
}
