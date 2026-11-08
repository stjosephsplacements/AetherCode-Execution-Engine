//! Verdict webhooks (Go: `webhook.go`): HMAC-SHA256 signed POST, three attempts
//! 500 ms apart, 5 s per attempt, no redirects, and an SSRF guard that refuses
//! loopback, private, link-local and other internal targets.
//!
//! The callback secret is only ever used to sign; it is never logged.

use hmac::{Hmac, Mac};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use serde::Serialize;
use sha2::Sha256;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

const USER_AGENT: &str = "AetherCode-Webhook-Dispatcher/1.0";
const SIGNATURE_HEADER: &str = "X-AetherCode-Signature";
const ATTEMPTS: u32 = 3;
const BACKOFF: Duration = Duration::from_millis(500);
const TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Serialize)]
pub struct WebhookPayload {
    pub submission_id: String,
    pub verdict: String,
    pub tests_passed: i64,
    pub test_count: i64,
    pub cpu_time_ns: i64,
    pub memory_bytes: i64,
    pub run_time_ns: i64,
    /// RFC 3339, UTC.
    pub timestamp: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub results: Vec<serde_json::Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum WebhookError {
    #[error("webhook: target address is in a blocked IP range (SSRF protection)")]
    Blocked,
    #[error("webhook: redirects are not followed (SSRF protection)")]
    Redirect,
    #[error("webhook: invalid callback URL: {0}")]
    Url(String),
    #[error("webhook: HTTP {0}")]
    Status(u16),
    #[error("webhook: {0}")]
    Http(String),
}

/// Hex HMAC-SHA256 of `body` keyed by `secret`.
pub fn sign(secret: &str, body: &[u8]) -> String {
    // HMAC accepts keys of any length.
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret.as_bytes())
        .unwrap_or_else(|_| unreachable!("HMAC takes any key length"));
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

/// Address ranges a webhook must never reach (Go's `ssrfBlockedPrefixes`, plus
/// the unspecified addresses, which connect to the local host).
const BLOCKED_V4: &[(Ipv4Addr, u8)] = &[
    (Ipv4Addr::new(0, 0, 0, 0), 8),
    (Ipv4Addr::new(127, 0, 0, 0), 8),
    (Ipv4Addr::new(10, 0, 0, 0), 8),
    (Ipv4Addr::new(172, 16, 0, 0), 12),
    (Ipv4Addr::new(192, 168, 0, 0), 16),
    (Ipv4Addr::new(169, 254, 0, 0), 16),
    (Ipv4Addr::new(100, 64, 0, 0), 10),
    (Ipv4Addr::new(192, 0, 0, 0), 24),
    (Ipv4Addr::new(198, 18, 0, 0), 15),
    (Ipv4Addr::new(198, 51, 100, 0), 24),
    (Ipv4Addr::new(203, 0, 113, 0), 24),
    (Ipv4Addr::new(240, 0, 0, 0), 4),
];

const BLOCKED_V6: &[(Ipv6Addr, u8)] = &[
    (Ipv6Addr::UNSPECIFIED, 128),
    (Ipv6Addr::LOCALHOST, 128),
    (Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 0), 7),
    (Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 0), 10),
    (Ipv6Addr::new(0x64, 0xff9b, 0, 0, 0, 0, 0, 0), 96),
    (Ipv6Addr::new(0x64, 0xff9b, 1, 0, 0, 0, 0, 0), 48),
];

/// True when `ip` is in a blocked range. IPv4-mapped IPv6 addresses are checked as IPv4.
pub fn is_blocked(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => blocked_v4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => blocked_v4(v4),
            None => {
                let a = u128::from(v6);
                BLOCKED_V6.iter().any(|(net, bits)| {
                    let mask = u128::MAX.checked_shl(128 - u32::from(*bits)).unwrap_or(0);
                    a & mask == u128::from(*net) & mask
                })
            }
        },
    }
}

fn blocked_v4(ip: Ipv4Addr) -> bool {
    let a = u32::from(ip);
    BLOCKED_V4.iter().any(|(net, bits)| {
        let mask = u32::MAX.checked_shl(32 - u32::from(*bits)).unwrap_or(0);
        a & mask == u32::from(*net) & mask
    })
}

/// Resolves host names and refuses the connection when any address is blocked;
/// connects to the first address only (Go's `ssrfSafeDialContext`). This runs for
/// every connection, so a DNS answer cannot smuggle in an internal address.
struct GuardedResolver;

impl Resolve for GuardedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let addrs: Vec<SocketAddr> =
                tokio::net::lookup_host((name.as_str(), 0)).await?.collect();
            if addrs.is_empty() {
                return Err(
                    format!("webhook: no addresses resolved for {:?}", name.as_str()).into(),
                );
            }
            if addrs.iter().any(|a| is_blocked(a.ip())) {
                return Err(
                    Box::new(WebhookError::Blocked) as Box<dyn std::error::Error + Send + Sync>
                );
            }
            Ok(Box::new(addrs.into_iter().take(1)) as Addrs)
        })
    }
}

/// HTTP client for webhooks.
#[derive(Clone)]
pub struct WebhookClient {
    http: reqwest::Client,
    guarded: bool,
}

impl Default for WebhookClient {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for WebhookClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookClient")
            .field("guarded", &self.guarded)
            .finish_non_exhaustive()
    }
}

fn builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .timeout(TIMEOUT)
        .connect_timeout(CONNECT_TIMEOUT)
        // One-shot calls: keep no idle connections.
        .pool_max_idle_per_host(0)
        .redirect(reqwest::redirect::Policy::custom(|a| {
            a.error(WebhookError::Redirect)
        }))
}

impl WebhookClient {
    /// The production client, with the SSRF guard.
    ///
    /// # Panics
    /// When the TLS backend cannot initialise (a broken build, not a runtime condition).
    pub fn new() -> Self {
        let http = builder()
            .dns_resolver(Arc::new(GuardedResolver))
            .no_proxy()
            .build()
            .unwrap_or_else(|e| panic!("webhook client: {e}"));
        Self {
            http,
            guarded: true,
        }
    }

    /// The same client without the address guard, so tests can reach a local
    /// server. Never use it for caller-supplied URLs in production.
    ///
    /// # Panics
    /// See [`WebhookClient::new`].
    pub fn allowing_private_addresses() -> Self {
        let http = builder()
            .no_proxy()
            .build()
            .unwrap_or_else(|e| panic!("webhook client: {e}"));
        Self {
            http,
            guarded: false,
        }
    }

    /// One signed POST.
    ///
    /// # Errors
    /// A blocked or malformed target, a refused redirect, a transport error or a non-2xx status.
    pub async fn send_once(
        &self,
        url: &str,
        secret: &str,
        body: &[u8],
    ) -> Result<u16, WebhookError> {
        let parsed = reqwest::Url::parse(url).map_err(|e| WebhookError::Url(e.to_string()))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(WebhookError::Url(format!(
                "unsupported scheme {:?}",
                parsed.scheme()
            )));
        }
        // IP literals never reach the resolver, so check them here.
        if self.guarded {
            let literal = match parsed.host() {
                Some(url::Host::Ipv4(ip)) => Some(IpAddr::V4(ip)),
                Some(url::Host::Ipv6(ip)) => Some(IpAddr::V6(ip)),
                Some(url::Host::Domain(_)) => None,
                None => return Err(WebhookError::Url("no host".into())),
            };
            if literal.is_some_and(is_blocked) {
                return Err(WebhookError::Blocked);
            }
        }
        let mut req = self
            .http
            .post(parsed)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::USER_AGENT, USER_AGENT)
            .body(body.to_vec());
        if !secret.is_empty() {
            req = req.header(SIGNATURE_HEADER, sign(secret, body));
        }
        let resp = req.send().await.map_err(classify)?;
        let status = resp.status().as_u16();
        if resp.status().is_success() {
            Ok(status)
        } else {
            Err(WebhookError::Status(status))
        }
    }

    /// Delivers `payload` with up to three attempts; true once a 2xx arrives.
    pub async fn dispatch(&self, url: &str, secret: &str, payload: &WebhookPayload) -> bool {
        let body = match serde_json::to_vec(payload) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "failed to marshal webhook payload");
                return false;
            }
        };
        for attempt in 1..=ATTEMPTS {
            match self.send_once(url, secret, &body).await {
                Ok(status) => {
                    tracing::info!(url, status, "webhook delivered successfully");
                    return true;
                }
                Err(WebhookError::Url(e)) => {
                    tracing::warn!(error = %e, "failed to build webhook request");
                    return false;
                }
                Err(WebhookError::Status(status)) => {
                    tracing::warn!(attempt, status, "webhook returned non-2xx status");
                }
                Err(e) => tracing::warn!(attempt, error = %e, "webhook attempt failed"),
            }
            if attempt < ATTEMPTS {
                tokio::time::sleep(BACKOFF).await;
            }
        }
        false
    }
}

/// Maps a reqwest error back to the guard that caused it, if any.
fn classify(e: reqwest::Error) -> WebhookError {
    let mut src: Option<&(dyn std::error::Error + 'static)> = Some(&e);
    while let Some(s) = src {
        if let Some(w) = s.downcast_ref::<WebhookError>() {
            return match w {
                WebhookError::Blocked => WebhookError::Blocked,
                WebhookError::Redirect => WebhookError::Redirect,
                other => WebhookError::Http(other.to_string()),
            };
        }
        src = s.source();
    }
    WebhookError::Http(e.to_string())
}
