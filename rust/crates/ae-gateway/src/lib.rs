//! HTTP gateway building blocks: OIDC auth, rate limiting, admission control, CORS and
//! request logging, each as a tower layer. Layer order, outermost first: logging, CORS,
//! auth, rate limit, admission (see the Go `cmd/stj-exec/main.go` wiring).

pub mod admission;
pub mod auth;
pub mod middleware;
pub mod ratelimit;

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;

/// Future returned by every middleware service in this crate.
pub type ResponseFuture = Pin<Box<dyn Future<Output = Result<Response, Infallible>> + Send>>;

/// `{"error": msg}` with a trailing newline, byte-identical to Go's `jsonError`.
pub(crate) fn json_error(status: StatusCode, msg: &str) -> Response {
    let mut body = serde_json::json!({ "error": msg }).to_string();
    body.push('\n');
    (
        status,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        body,
    )
        .into_response()
}

/// `Retry-After` in whole seconds, rounded up (Go: `math.Ceil(d.Seconds())`).
pub(crate) fn retry_after_header(d: std::time::Duration) -> HeaderValue {
    let secs = d.as_millis().div_ceil(1000);
    HeaderValue::from_str(&secs.to_string()).unwrap_or(HeaderValue::from_static("1"))
}
