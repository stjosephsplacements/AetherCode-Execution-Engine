//! OIDC bearer-token authentication (port of `internal/auth`).
//!
//! Tokens are verified against the issuer's JWKS with a pinned set of asymmetric
//! algorithms, then mapped to a tenant (Zitadel org id claim, by `tenants.slug`), an
//! auto-provisioned user and a role. Requests without a token pass through without an
//! [`Identity`]; route handlers decide whether that is allowed, as in Go.

use crate::{ResponseFuture, json_error};
use ae_core::Config;
use ae_core::model::DEFAULT_TENANT_ID;
use ae_queue::BoxFuture;
use axum::extract::Request;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use jsonwebtoken::jwk::{AlgorithmParameters, Jwk, KeyAlgorithm, PublicKeyUse};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tower::{Layer, Service};
use uuid::Uuid;

pub const ORG_ID_CLAIM: &str = "urn:zitadel:iam:org:id";
pub const ROLES_CLAIM: &str = "urn:zitadel:iam:org:project:roles";

/// Tenant and user lookups are cached this long.
pub const CACHE_TTL: Duration = Duration::from_secs(300);

/// Signature algorithms accepted from the JWKS. Symmetric (HS*) and `none` are never
/// accepted, so a public key cannot be abused as an HMAC secret.
const ALLOWED_ALGS: [Algorithm; 9] = [
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::PS256,
    Algorithm::PS384,
    Algorithm::PS512,
    Algorithm::ES256,
    Algorithm::ES384,
    Algorithm::EdDSA,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Admin,
    Faculty,
    Student,
}

impl Role {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Admin => "admin",
            Role::Faculty => "faculty",
            Role::Student => "student",
        }
    }

    /// Zitadel project roles claim (`{"admin": {...}, ...}`): admin, then faculty, else
    /// student (also for a missing or malformed claim).
    fn from_claims(claims: &Map<String, Value>) -> Self {
        match claims.get(ROLES_CLAIM).and_then(Value::as_object) {
            Some(r) if r.contains_key("admin") => Role::Admin,
            Some(r) if r.contains_key("faculty") => Role::Faculty,
            _ => Role::Student,
        }
    }
}

/// Authenticated caller, stored in request extensions by [`AuthLayer`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub tenant_id: Uuid,
    pub user_id: Uuid,
    pub external_sub: String,
    pub email: String,
    pub role: Role,
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("auth: config: {0}")]
    Config(String),
    #[error("auth: jwt: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
    #[error("auth: algorithm {0:?} not accepted")]
    Algorithm(Algorithm),
    #[error("auth: malformed Authorization header")]
    MalformedHeader,
    #[error("auth: token has no kid")]
    MissingKid,
    #[error("auth: no signing key for kid")]
    UnknownKey,
    #[error("auth: missing sub claim")]
    MissingSub,
    #[error("auth: unknown or inactive organisation: {0}")]
    UnknownOrganisation(StoreError),
    #[error("auth: resolve user: {0}")]
    ResolveUser(StoreError),
    #[error("auth: jwks: {0}")]
    Jwks(String),
}

pub type StoreError = Box<dyn std::error::Error + Send + Sync>;

/// Bootstrap identity lookups. They run before the tenant is known, so the production
/// implementation ([`ae_db::Db`]) uses the owner pool: the RLS (`AC_DATABASE_APP_URL`)
/// pool would filter every row without `app.tenant_id` set, exactly as Go documents.
pub trait IdentityStore: Send + Sync + 'static {
    /// Active tenant whose slug is the Zitadel org id; an error when none.
    fn tenant_by_org<'a>(&'a self, org_id: &'a str) -> BoxFuture<'a, Result<Uuid, StoreError>>;
    fn find_user<'a>(
        &'a self,
        tenant_id: Uuid,
        sub: &'a str,
    ) -> BoxFuture<'a, Result<Option<Uuid>, StoreError>>;
    /// Upserts the user (email refreshed on conflict) and returns its id.
    fn provision_user<'a>(
        &'a self,
        tenant_id: Uuid,
        sub: &'a str,
        email: &'a str,
        name: &'a str,
    ) -> BoxFuture<'a, Result<Uuid, StoreError>>;
}

impl IdentityStore for ae_db::Db {
    fn tenant_by_org<'a>(&'a self, org_id: &'a str) -> BoxFuture<'a, Result<Uuid, StoreError>> {
        Box::pin(async move {
            Ok(
                sqlx::query_scalar("SELECT id FROM tenants WHERE slug = $1 AND active = true")
                    .bind(org_id)
                    .fetch_one(self.pool())
                    .await?,
            )
        })
    }

    fn find_user<'a>(
        &'a self,
        tenant_id: Uuid,
        sub: &'a str,
    ) -> BoxFuture<'a, Result<Option<Uuid>, StoreError>> {
        Box::pin(async move {
            Ok(sqlx::query_scalar(
                "SELECT id FROM users WHERE tenant_id = $1 AND external_subject_id = $2",
            )
            .bind(tenant_id)
            .bind(sub)
            .fetch_optional(self.pool())
            .await?)
        })
    }

    fn provision_user<'a>(
        &'a self,
        tenant_id: Uuid,
        sub: &'a str,
        email: &'a str,
        name: &'a str,
    ) -> BoxFuture<'a, Result<Uuid, StoreError>> {
        Box::pin(async move {
            Ok(sqlx::query_scalar(
                "INSERT INTO users (tenant_id, external_subject_id, email, display_name)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (tenant_id, external_subject_id) DO UPDATE SET email = EXCLUDED.email
                 RETURNING id",
            )
            .bind(tenant_id)
            .bind(sub)
            .bind(email)
            .bind(name)
            .fetch_one(self.pool())
            .await?)
        })
    }
}

type Entries<V> = HashMap<String, (V, Instant)>;

/// Map with per-entry expiry. Expired entries are swept on insert at most once per TTL.
pub struct TtlCache<V> {
    ttl: Duration,
    /// Entries with their expiry, and the time of the last sweep.
    inner: Mutex<(Entries<V>, Instant)>,
}

impl<V: Clone> TtlCache<V> {
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            inner: Mutex::new((HashMap::new(), Instant::now())),
        }
    }

    pub fn get(&self, key: &str) -> Option<V> {
        let g = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        g.0.get(key)
            .filter(|(_, exp)| Instant::now() < *exp)
            .map(|(v, _)| v.clone())
    }

    pub fn set(&self, key: String, value: V) {
        let now = Instant::now();
        let mut g = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if now.duration_since(g.1) >= self.ttl {
            g.0.retain(|_, (_, exp)| now < *exp);
            g.1 = now;
        }
        g.0.insert(key, (value, now + self.ttl));
    }
}

/// Go's `extractBearerToken`: the text after the first space when the scheme is `Bearer`
/// (case-insensitive), else empty.
#[must_use]
pub fn extract_bearer_token(headers: &HeaderMap) -> String {
    let Some(v) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return String::new();
    };
    match v.split_once(' ') {
        Some((scheme, rest)) if scheme.eq_ignore_ascii_case("Bearer") => rest.to_owned(),
        _ => String::new(),
    }
}

/// String claim, empty when absent or not a string.
#[must_use]
pub fn claim_string(claims: &Map<String, Value>, key: &str) -> String {
    claims
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

#[derive(Clone, Debug)]
pub struct JwksOptions {
    /// Per-request timeout for JWKS fetches.
    pub timeout: Duration,
    /// Minimum gap between refetches triggered by an unknown `kid` (keyfunc's default
    /// limiter: one per five minutes). Bounds the fetches forged kids can cause.
    pub refetch_min_interval: Duration,
    /// Background refresh period.
    pub refresh_interval: Duration,
}

impl Default for JwksOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
            refetch_min_interval: Duration::from_secs(300),
            refresh_interval: Duration::from_secs(3600),
        }
    }
}

struct SigningKey {
    key: DecodingKey,
    alg: Option<Algorithm>,
}

struct Jwks {
    url: String,
    host: String,
    client: reqwest::Client,
    opts: JwksOptions,
    keys: RwLock<Arc<HashMap<String, Arc<SigningKey>>>>,
    /// When the last unknown-kid refetch happened; the lock also serialises refetches.
    refetch: tokio::sync::Mutex<Option<Instant>>,
}

impl Jwks {
    fn get(&self, kid: &str) -> Option<Arc<SigningKey>> {
        self.keys
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(kid)
            .cloned()
    }

    async fn fetch(&self) -> Result<(), AuthError> {
        let mut req = self.client.get(&self.url);
        if let Ok(h) = HeaderValue::from_str(&self.host)
            && !self.host.is_empty()
        {
            // Zitadel routes on Host; the JWKS URL is usually an internal address.
            req = req.header(header::HOST, h);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| AuthError::Jwks(e.without_url().to_string()))?;
        if !resp.status().is_success() {
            return Err(AuthError::Jwks(format!("status {}", resp.status())));
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| AuthError::Jwks(e.without_url().to_string()))?;
        let entries = body
            .get("keys")
            .and_then(Value::as_array)
            .ok_or_else(|| AuthError::Jwks("no keys array".to_owned()))?;
        let mut keys = HashMap::new();
        for raw in entries {
            // Unparseable or unsupported entries are skipped, not fatal.
            let Ok(jwk) = serde_json::from_value::<Jwk>(raw.clone()) else {
                continue;
            };
            if let Some((kid, k)) = signing_key(&jwk) {
                keys.insert(kid, Arc::new(k));
            }
        }
        *self
            .keys
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(keys);
        Ok(())
    }

    /// Key for `kid`, refetching the set once per `refetch_min_interval` when unknown.
    ///
    /// The limit bounds the JWKS fetches forged kids can cause. Trade-off (same as
    /// keyfunc): a forged kid that consumes the refetch can delay pickup of a genuinely
    /// rotated key by up to `refetch_min_interval`; the periodic refresh still applies.
    async fn key(&self, kid: &str) -> Option<Arc<SigningKey>> {
        if let Some(k) = self.get(kid) {
            return Some(k);
        }
        let mut last = self.refetch.lock().await;
        if let Some(k) = self.get(kid) {
            return Some(k);
        }
        if last.is_some_and(|t| t.elapsed() < self.opts.refetch_min_interval) {
            return None;
        }
        *last = Some(Instant::now());
        if let Err(e) = self.fetch().await {
            tracing::warn!(error = %e, "auth: JWKS refetch failed");
        }
        self.get(kid)
    }
}

/// Asymmetric signing keys only: HMAC (`oct`) and unknown key types are ignored, as are
/// keys marked for encryption.
fn signing_key(jwk: &Jwk) -> Option<(String, SigningKey)> {
    let kid = jwk.common.key_id.clone()?;
    if matches!(
        jwk.common.public_key_use,
        Some(ref u) if *u != PublicKeyUse::Signature
    ) {
        return None;
    }
    if !matches!(
        jwk.algorithm,
        AlgorithmParameters::RSA(_)
            | AlgorithmParameters::EllipticCurve(_)
            | AlgorithmParameters::OctetKeyPair(_)
    ) {
        return None;
    }
    let alg = match jwk.common.key_algorithm {
        None => None,
        Some(a) => Some(asymmetric_alg(a)?),
    };
    let key = DecodingKey::from_jwk(jwk).ok()?;
    Some((kid, SigningKey { key, alg }))
}

fn asymmetric_alg(a: KeyAlgorithm) -> Option<Algorithm> {
    Some(match a {
        KeyAlgorithm::RS256 => Algorithm::RS256,
        KeyAlgorithm::RS384 => Algorithm::RS384,
        KeyAlgorithm::RS512 => Algorithm::RS512,
        KeyAlgorithm::PS256 => Algorithm::PS256,
        KeyAlgorithm::PS384 => Algorithm::PS384,
        KeyAlgorithm::PS512 => Algorithm::PS512,
        KeyAlgorithm::ES256 => Algorithm::ES256,
        KeyAlgorithm::ES384 => Algorithm::ES384,
        KeyAlgorithm::EdDSA => Algorithm::EdDSA,
        _ => return None,
    })
}

#[derive(Clone, Debug)]
pub struct AuthSettings {
    pub issuer: String,
    pub audience: String,
    pub jwks_url: String,
}

/// Validates tokens and resolves them to an [`Identity`].
pub struct Authenticator {
    settings: AuthSettings,
    jwks: Arc<Jwks>,
    store: Arc<dyn IdentityStore>,
    tenants: TtlCache<Uuid>,
    users: TtlCache<Uuid>,
    refresher: tokio::task::JoinHandle<()>,
}

impl Drop for Authenticator {
    fn drop(&mut self) {
        self.refresher.abort();
    }
}

impl Authenticator {
    /// Fetches the JWKS once (a failure is logged; tokens are then rejected until a
    /// refetch succeeds, so auth fails closed) and starts the periodic refresh.
    ///
    /// # Panics
    /// Panics outside a Tokio runtime, or if the TLS backend cannot initialise.
    pub async fn new(
        settings: AuthSettings,
        opts: JwksOptions,
        store: Arc<dyn IdentityStore>,
    ) -> Arc<Self> {
        let host = settings
            .issuer
            .strip_prefix("https://")
            .or_else(|| settings.issuer.strip_prefix("http://"))
            .unwrap_or(&settings.issuer)
            .split('/')
            .next()
            .unwrap_or_default()
            .to_owned();
        let client = reqwest::Client::builder()
            .timeout(opts.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("auth: build JWKS HTTP client");
        let jwks = Arc::new(Jwks {
            url: settings.jwks_url.clone(),
            host,
            client,
            opts: opts.clone(),
            keys: RwLock::default(),
            refetch: tokio::sync::Mutex::new(None),
        });
        if let Err(e) = jwks.fetch().await {
            tracing::warn!(error = %e, "auth: initial JWKS fetch failed; tokens are rejected until it succeeds");
        }
        let refresher = tokio::spawn(refresh_loop(Arc::downgrade(&jwks), opts.refresh_interval));
        Arc::new(Self {
            settings,
            jwks,
            store,
            tenants: TtlCache::new(CACHE_TTL),
            users: TtlCache::new(CACHE_TTL),
            refresher,
        })
    }

    /// Verifies signature, `iss`, `aud`, `exp` (required, no leeway), `nbf` (when present)
    /// and `sub`, then resolves tenant, user and role.
    ///
    /// # Errors
    /// Any verification or lookup failure; a token carrying an org id that does not map
    /// to an active tenant is rejected rather than downgraded to the default tenant.
    pub async fn validate(&self, token: &str) -> Result<Identity, AuthError> {
        let header = jsonwebtoken::decode_header(token)?;
        if !ALLOWED_ALGS.contains(&header.alg) {
            return Err(AuthError::Algorithm(header.alg));
        }
        let kid = header.kid.ok_or(AuthError::MissingKid)?;
        let key = self.jwks.key(&kid).await.ok_or(AuthError::UnknownKey)?;
        if key.key.family() != header.alg.family() || key.alg.is_some_and(|a| a != header.alg) {
            return Err(AuthError::Algorithm(header.alg));
        }
        let mut v = Validation::new(header.alg);
        v.leeway = 0;
        v.validate_nbf = true;
        v.set_issuer(&[&self.settings.issuer]);
        v.set_audience(&[&self.settings.audience]);
        v.set_required_spec_claims(&["exp", "iss", "aud"]);
        let claims = jsonwebtoken::decode::<Map<String, Value>>(token, &key.key, &v)?.claims;

        let sub = claim_string(&claims, "sub");
        if sub.is_empty() {
            return Err(AuthError::MissingSub);
        }
        let email = claim_string(&claims, "email");
        let mut name = claim_string(&claims, "name");
        if name.is_empty() {
            name = claim_string(&claims, "preferred_username");
        }

        let org = claim_string(&claims, ORG_ID_CLAIM);
        let tenant_id = if org.is_empty() {
            Uuid::parse_str(DEFAULT_TENANT_ID).expect("default tenant id is a UUID")
        } else {
            self.resolve_tenant(&org).await?
        };
        let user_id = self.resolve_user(tenant_id, &sub, &email, &name).await?;
        Ok(Identity {
            tenant_id,
            user_id,
            external_sub: sub,
            email,
            role: Role::from_claims(&claims),
        })
    }

    async fn resolve_tenant(&self, org: &str) -> Result<Uuid, AuthError> {
        if let Some(id) = self.tenants.get(org) {
            return Ok(id);
        }
        let id = self
            .store
            .tenant_by_org(org)
            .await
            .map_err(AuthError::UnknownOrganisation)?;
        self.tenants.set(org.to_owned(), id);
        Ok(id)
    }

    async fn resolve_user(
        &self,
        tenant_id: Uuid,
        sub: &str,
        email: &str,
        name: &str,
    ) -> Result<Uuid, AuthError> {
        let key = format!("{tenant_id}:{sub}");
        if let Some(id) = self.users.get(&key) {
            return Ok(id);
        }
        // As in Go, any lookup miss or error falls through to the idempotent upsert.
        if let Ok(Some(id)) = self.store.find_user(tenant_id, sub).await {
            self.users.set(key, id);
            return Ok(id);
        }
        let id = self
            .store
            .provision_user(tenant_id, sub, email, name)
            .await
            .map_err(AuthError::ResolveUser)?;
        self.users.set(key, id);
        tracing::info!(user_id = %id, "user auto-provisioned");
        Ok(id)
    }
}

async fn refresh_loop(jwks: Weak<Jwks>, every: Duration) {
    let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    loop {
        tick.tick().await;
        let Some(j) = jwks.upgrade() else { return };
        if let Err(e) = j.fetch().await {
            tracing::warn!(error = %e, "auth: JWKS refresh failed; keeping previous keys");
        }
    }
}

/// Validates bearer tokens. No `Authorization` header passes through without an
/// [`Identity`]; any present but invalid credential is a 401, never anonymous.
#[derive(Clone)]
pub struct AuthLayer {
    auth: Option<Arc<Authenticator>>,
}

impl AuthLayer {
    #[must_use]
    pub fn new(auth: Arc<Authenticator>) -> Self {
        Self { auth: Some(auth) }
    }

    /// From `AC_AUTH_ENABLED`, `AC_ALLOW_UNAUTH` and the `AC_OIDC_*` settings. Auth
    /// disabled is only accepted with `AC_ALLOW_UNAUTH=true`, and then the layer is a
    /// pass-through (Go's behaviour).
    ///
    /// # Errors
    /// Auth disabled without `AC_ALLOW_UNAUTH`, or enabled without an audience.
    pub async fn from_config(
        cfg: &Config,
        store: Arc<dyn IdentityStore>,
    ) -> Result<Self, AuthError> {
        if !cfg.auth_enabled {
            if !cfg.allow_unauth {
                return Err(AuthError::Config(
                    "AC_AUTH_ENABLED=false requires AC_ALLOW_UNAUTH=true to confirm this is intentional".to_owned(),
                ));
            }
            tracing::warn!("AC_AUTH_ENABLED=false: authentication is disabled");
            return Ok(Self { auth: None });
        }
        if cfg.oidc_issuer.is_empty() {
            return Err(AuthError::Config(
                "OIDCIssuer (AC_OIDC_ISSUER) is required when auth is enabled".to_owned(),
            ));
        }
        if cfg.oidc_jwks_url.is_empty() {
            return Err(AuthError::Config(
                "OIDCJWKSURL (AC_OIDC_JWKS_URL) is required when auth is enabled".to_owned(),
            ));
        }
        if cfg.oidc_audience.is_empty() {
            return Err(AuthError::Config(
                "OIDCAudience (AC_OIDC_AUDIENCE) is required when auth is enabled".to_owned(),
            ));
        }
        if cfg.database_app_url.is_empty() {
            tracing::warn!(
                "AC_DATABASE_APP_URL not set: auth queries run as DB owner, bypassing Row-Level Security"
            );
        }
        let auth = Authenticator::new(
            AuthSettings {
                issuer: cfg.oidc_issuer.clone(),
                audience: cfg.oidc_audience.clone(),
                jwks_url: cfg.oidc_jwks_url.clone(),
            },
            JwksOptions::default(),
            store,
        )
        .await;
        tracing::info!(issuer = %cfg.oidc_issuer, "auth enabled");
        Ok(Self::new(auth))
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = Auth<S>;
    fn layer(&self, inner: S) -> Self::Service {
        Auth {
            inner,
            auth: self.auth.clone(),
        }
    }
}

#[derive(Clone)]
pub struct Auth<S> {
    inner: S,
    auth: Option<Arc<Authenticator>>,
}

impl<S> Service<Request> for Auth<S>
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
        let auth = self.auth.clone();
        Box::pin(async move {
            let Some(auth) = auth else {
                return inner.call(req).await;
            };
            if !req.headers().contains_key(header::AUTHORIZATION) {
                return inner.call(req).await;
            }
            let token = extract_bearer_token(req.headers());
            let result = if token.is_empty() {
                Err(AuthError::MalformedHeader)
            } else {
                auth.validate(&token).await
            };
            match result {
                Ok(id) => {
                    req.extensions_mut().insert(id);
                    inner.call(req).await
                }
                Err(e) => {
                    tracing::debug!(error = %e, "auth: invalid token");
                    Ok(json_error(
                        StatusCode::UNAUTHORIZED,
                        "invalid or expired token",
                    ))
                }
            }
        })
    }
}
