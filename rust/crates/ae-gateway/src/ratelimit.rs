//! Redis sliding-window rate limiting (port of `internal/ratelimit/limiter.go`).

use crate::auth::Identity;
use crate::middleware::{peer_ip, remote_ip};
use crate::{ResponseFuture, json_error, retry_after_header};
use ae_core::Config;
use axum::extract::Request;
use axum::http::{StatusCode, header};
use axum::response::Response;
use ipnet::IpNet;
use redis::aio::ConnectionManager;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tower::{Layer, Service};
use uuid::Uuid;

/// Atomic check-and-record, byte-identical to Go's `rateLimitScript`.
/// Returns {1,0} allow, {0,0} minute limit, {0,1} burst limit.
const SCRIPT: &str = r"
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local burstWindow = tonumber(ARGV[3])
local perMinute = tonumber(ARGV[4])
local burst = tonumber(ARGV[5])
local member = ARGV[6]
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local total = redis.call('ZCARD', key)
if total >= perMinute then return {0, 0} end
local burstCount = redis.call('ZCOUNT', key, now - burstWindow, '+inf')
if burstCount >= burst then return {0, 1} end
redis.call('ZADD', key, now, member)
redis.call('EXPIRE', key, math.ceil(window/1000) + 1)
return {1, 0}
";

/// Set once the missing-`ConnectInfo` misconfiguration has been logged.
static MISSING_PEER_LOGGED: AtomicBool = AtomicBool::new(false);

const WINDOW_MS: i64 = 60_000;
const BURST_WINDOW: Duration = Duration::from_secs(10);
/// Bound on a Redis round trip; on timeout the limiter fails open like on any error.
const REDIS_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Decision {
    pub allowed: bool,
    pub retry_after: Duration,
}

const ALLOW: Decision = Decision {
    allowed: true,
    retry_after: Duration::ZERO,
};

#[derive(Clone)]
pub struct Limiter {
    conn: ConnectionManager,
    script: Arc<redis::Script>,
    per_minute: u32,
    burst: u32,
}

impl Limiter {
    /// `per_minute` per 60 s window, `burst` per 10 s window.
    #[must_use]
    pub fn new(conn: ConnectionManager, per_minute: u32, burst: u32) -> Self {
        Self {
            conn,
            script: Arc::new(redis::Script::new(SCRIPT)),
            per_minute,
            burst,
        }
    }

    /// Authenticated user, key `ac:ratelimit:<user>`.
    pub async fn check(&self, user: Uuid) -> Decision {
        self.run(&format!("ac:ratelimit:{user}")).await
    }

    /// Unauthenticated client IP, key `ac:ratelimit:ip:<ip>`; always allowed when
    /// `per_minute` is 0 (disabled).
    pub async fn check_ip(&self, ip: &str) -> Decision {
        if self.per_minute == 0 {
            return ALLOW;
        }
        self.run(&format!("ac:ratelimit:ip:{ip}")).await
    }

    async fn run(&self, key: &str) -> Decision {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
        let member = format!("{now_ms}-{:016x}", rand::random::<u64>());
        let mut conn = self.conn.clone();
        let mut inv = self.script.key(key);
        inv.arg(now_ms)
            .arg(WINDOW_MS)
            .arg(i64::try_from(BURST_WINDOW.as_millis()).unwrap_or(10_000))
            .arg(self.per_minute)
            .arg(self.burst)
            .arg(member);
        let fut = inv.invoke_async::<Vec<i64>>(&mut conn);
        let res = match tokio::time::timeout(REDIS_TIMEOUT, fut).await {
            Ok(Ok(r)) if r.len() == 2 => r,
            Ok(Ok(_)) => {
                tracing::error!("ratelimit: unexpected script reply, failing open");
                return ALLOW;
            }
            Ok(Err(e)) => {
                tracing::error!(error = %e, "ratelimit: lua script error, failing open");
                return ALLOW;
            }
            Err(_) => {
                tracing::error!("ratelimit: redis timeout, failing open");
                return ALLOW;
            }
        };
        if res[0] == 1 {
            return ALLOW;
        }
        let retry_after = if res[1] == 1 {
            BURST_WINDOW
        } else {
            self.minute_retry_after(key, now_ms).await
        };
        Decision {
            allowed: false,
            retry_after,
        }
    }

    /// Until the oldest entry leaves the window, at least 1 s; 10 s when unknown.
    async fn minute_retry_after(&self, key: &str, now_ms: i64) -> Duration {
        let mut conn = self.conn.clone();
        let mut cmd = redis::cmd("ZRANGE");
        cmd.arg(key).arg(0).arg(0).arg("WITHSCORES");
        let fut = cmd.query_async::<Vec<(String, f64)>>(&mut conn);
        match tokio::time::timeout(REDIS_TIMEOUT, fut).await {
            Ok(Ok(v)) if !v.is_empty() => {
                #[allow(clippy::cast_possible_truncation)]
                let oldest = v[0].1 as i64;
                let ms = (oldest + WINDOW_MS - now_ms).max(1000);
                Duration::from_millis(u64::try_from(ms).unwrap_or(1000))
            }
            _ => Duration::from_secs(10),
        }
    }
}

/// Per-user limit for requests carrying an [`Identity`]; per-client-IP limit (when
/// configured) for the rest. The IP honours `X-Forwarded-For` only from trusted proxies.
///
/// The server must be started with
/// `into_make_service_with_connect_info::<SocketAddr>()`. Without the peer address the IP
/// limiter cannot key callers, so such requests fail with 500 and the misconfiguration is
/// logged (once) instead of throttling every anonymous caller through one shared bucket.
#[derive(Clone)]
pub struct RateLimitLayer {
    user: Limiter,
    ip: Option<Limiter>,
    trusted: Arc<Vec<IpNet>>,
}

impl RateLimitLayer {
    #[must_use]
    pub fn new(user: Limiter, ip: Option<Limiter>, trusted: Vec<IpNet>) -> Self {
        Self {
            user,
            ip,
            trusted: Arc::new(trusted),
        }
    }

    /// `AC_RATE_LIMIT_*` and `AC_TRUSTED_PROXIES`; the IP limiter only when
    /// `AC_RATE_LIMIT_UNAUTH_PER_MINUTE > 0`.
    #[must_use]
    pub fn from_config(cfg: &Config, conn: ConnectionManager) -> Self {
        let user = Limiter::new(
            conn.clone(),
            cfg.rate_limit_per_minute,
            cfg.rate_limit_burst,
        );
        let ip = (cfg.rate_limit_unauth_per_minute > 0).then(|| {
            Limiter::new(
                conn,
                cfg.rate_limit_unauth_per_minute,
                cfg.rate_limit_unauth_burst,
            )
        });
        Self::new(
            user,
            ip,
            crate::middleware::parse_trusted_proxies(&cfg.trusted_proxies),
        )
    }
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimit<S>;
    fn layer(&self, inner: S) -> Self::Service {
        RateLimit {
            inner,
            cfg: self.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RateLimit<S> {
    inner: S,
    cfg: RateLimitLayer,
}

impl<S> Service<Request> for RateLimit<S>
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
        let cfg = self.cfg.clone();
        Box::pin(async move {
            let decision = if let Some(id) = req.extensions().get::<Identity>() {
                cfg.user.check(id.user_id).await
            } else if let Some(ip) = &cfg.ip {
                let Some(peer) = peer_ip(&req) else {
                    if !MISSING_PEER_LOGGED.swap(true, Ordering::Relaxed) {
                        tracing::error!(
                            "ratelimit: request has no peer address; serve with into_make_service_with_connect_info::<SocketAddr>()"
                        );
                    }
                    return Ok(json_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal error",
                    ));
                };
                let client = remote_ip(Some(peer), req.headers(), &cfg.trusted);
                ip.check_ip(&client).await
            } else {
                ALLOW
            };
            if decision.allowed {
                return inner.call(req).await;
            }
            let mut resp = json_error(StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded");
            resp.headers_mut().insert(
                header::RETRY_AFTER,
                retry_after_header(decision.retry_after),
            );
            Ok(resp)
        })
    }
}
