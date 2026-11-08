//! Request logging with request ids, CORS, and client-IP resolution behind trusted proxies
//! (port of `internal/gateway/{middleware,cors}.go`).

use crate::ResponseFuture;
use axum::extract::{ConnectInfo, Request};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use ipnet::IpNet;
use std::collections::HashSet;
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use tower::{Layer, Service};

/// Parses `AC_TRUSTED_PROXIES` entries. A bare address is a single host (`/32`, or `/128`
/// for IPv6); invalid entries are skipped, as in Go.
#[must_use]
pub fn parse_trusted_proxies(cidrs: &[String]) -> Vec<IpNet> {
    cidrs
        .iter()
        .filter_map(|c| {
            if c.contains('/') {
                c.parse::<IpNet>().ok().map(|n| n.trunc())
            } else {
                c.parse::<IpAddr>().ok().map(IpNet::from)
            }
        })
        .collect()
}

fn is_trusted(ip: IpAddr, nets: &[IpNet]) -> bool {
    let ip = ip.to_canonical();
    nets.iter().any(|n| n.contains(&ip))
}

/// Client IP for rate limiting. `X-Forwarded-For` (right-most entry that is not itself a
/// trusted proxy) and then `X-Real-Ip` are honoured only when the direct peer is a trusted
/// proxy; otherwise the peer address is used, so untrusted callers cannot pick their key.
#[must_use]
pub fn remote_ip(peer: Option<IpAddr>, headers: &HeaderMap, trusted: &[IpNet]) -> String {
    let peer = peer.map(|p| p.to_canonical());
    if let Some(p) = peer
        && !trusted.is_empty()
        && is_trusted(p, trusted)
    {
        if let Some(xff) = header_str(headers, "x-forwarded-for") {
            for part in xff.split(',').rev() {
                let ip = part.trim();
                if ip.is_empty() {
                    continue;
                }
                if !ip.parse::<IpAddr>().is_ok_and(|a| is_trusted(a, trusted)) {
                    return ip.to_owned();
                }
            }
        }
        if let Some(xri) = header_str(headers, "x-real-ip") {
            return xri.trim().to_owned();
        }
    }
    peer.map_or_else(|| "unknown".to_owned(), |p| p.to_string())
}

fn header_str<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
    h.get(name)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
}

/// Peer address recorded by `into_make_service_with_connect_info`.
pub(crate) fn peer_ip(req: &Request) -> Option<IpAddr> {
    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip())
}

/// Drops ASCII control characters (newlines included) so a crafted path cannot forge log
/// lines.
#[must_use]
pub fn sanitize_log_value(s: &str) -> String {
    s.chars().filter(|&c| c >= ' ' && c != '\u{7f}').collect()
}

/// Route label for metrics: known paths verbatim, everything else `other`.
#[must_use]
pub fn normalize_path(p: &str) -> &'static str {
    match p {
        "/api/v1/execute" => "/api/v1/execute",
        "/api/v1/stream" => "/api/v1/stream",
        "/api/v1/health" => "/api/v1/health",
        _ => "other",
    }
}

/// Request id (UUIDv7) stored in request extensions and echoed as `X-Request-Id`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestId(pub String);

/// Outermost layer: assigns the request id and logs every request with its status and
/// duration (warn for 4xx, error for 5xx).
#[derive(Clone, Copy, Default)]
pub struct RequestLogLayer;

impl RequestLogLayer {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for RequestLogLayer {
    type Service = RequestLog<S>;
    fn layer(&self, inner: S) -> Self::Service {
        RequestLog { inner }
    }
}

#[derive(Clone)]
pub struct RequestLog<S> {
    inner: S,
}

impl<S> Service<Request> for RequestLog<S>
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = Response;
    type Error = Infallible;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            let id = ae_core::requestid::new_request_id();
            req.extensions_mut().insert(RequestId(id.clone()));
            let method = req.method().clone();
            let path = sanitize_log_value(req.uri().path());
            let ip = peer_ip(&req).map(|p| p.to_string()).unwrap_or_default();
            let start = Instant::now();
            let mut resp = inner.call(req).await?;
            if let Ok(v) = HeaderValue::from_str(&id) {
                resp.headers_mut().insert("x-request-id", v);
            }
            let status = resp.status().as_u16();
            let duration_ms = start.elapsed().as_millis();
            macro_rules! log {
                ($lvl:ident) => {
                    tracing::$lvl!(
                        method = %method, path, status, duration_ms, ip, request_id = id, "http"
                    )
                };
            }
            match status {
                500.. => log!(error),
                400..=499 => log!(warn),
                _ => log!(info),
            }
            Ok(resp)
        })
    }
}

/// CORS for `AC_CORS_ALLOWED_ORIGINS` (comma-separated exact origins). Allowed preflights
/// get 204 here so they never reach auth; with no origins configured it is a no-op.
#[derive(Clone)]
pub struct CorsLayer {
    origins: Arc<HashSet<String>>,
}

impl CorsLayer {
    #[must_use]
    pub fn new(origins: &str) -> Self {
        Self {
            origins: Arc::new(
                origins
                    .split(',')
                    .map(str::trim)
                    .filter(|o| !o.is_empty())
                    .map(str::to_owned)
                    .collect(),
            ),
        }
    }
}

impl<S> Layer<S> for CorsLayer {
    type Service = Cors<S>;
    fn layer(&self, inner: S) -> Self::Service {
        Cors {
            inner,
            origins: self.origins.clone(),
        }
    }
}

#[derive(Clone)]
pub struct Cors<S> {
    inner: S,
    origins: Arc<HashSet<String>>,
}

impl<S> Service<Request> for Cors<S>
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = Response;
    type Error = Infallible;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let allowed = (!self.origins.is_empty())
            .then(|| header_str(req.headers(), "origin"))
            .flatten()
            .filter(|o| self.origins.contains(*o))
            .and_then(|o| HeaderValue::from_str(o).ok());
        Box::pin(async move {
            let Some(origin) = allowed else {
                return inner.call(req).await;
            };
            let mut resp = if req.method() == Method::OPTIONS {
                StatusCode::NO_CONTENT.into_response()
            } else {
                inner.call(req).await?
            };
            let h = resp.headers_mut();
            h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
            h.insert(
                header::ACCESS_CONTROL_ALLOW_METHODS,
                HeaderValue::from_static("GET, POST, OPTIONS"),
            );
            h.insert(
                header::ACCESS_CONTROL_ALLOW_HEADERS,
                HeaderValue::from_static("Content-Type, Authorization"),
            );
            h.insert(
                header::ACCESS_CONTROL_MAX_AGE,
                HeaderValue::from_static("86400"),
            );
            h.insert(header::VARY, HeaderValue::from_static("Origin"));
            Ok(resp)
        })
    }
}
