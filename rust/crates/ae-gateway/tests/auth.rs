//! Ports of internal/auth/middleware_test.go plus the JWT hardening cases.

mod common;

use ae_core::Config;
use ae_core::model::DEFAULT_TENANT_ID;
use ae_gateway::auth::{
    AuthLayer, AuthSettings, Authenticator, Identity, IdentityStore, JwksOptions, Role, TtlCache,
    claim_string, extract_bearer_token,
};
use axum::Router;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::routing::get;
use common::*;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tower::ServiceExt;
use uuid::Uuid;

fn default_tenant() -> Uuid {
    Uuid::parse_str(DEFAULT_TENANT_ID).unwrap()
}

fn fast_opts() -> JwksOptions {
    JwksOptions {
        timeout: Duration::from_secs(2),
        refetch_min_interval: Duration::from_millis(300),
        refresh_interval: Duration::from_secs(3600),
    }
}

struct Fixture {
    key: common::Key,
    jwks: JwksServer,
    store: Arc<FakeStore>,
    auth: Arc<Authenticator>,
}

async fn fixture_with(issuer: &str, audience: &str, opts: JwksOptions) -> Fixture {
    let key = rsa_key("test-key");
    let jwks = JwksServer::start(vec![key.jwk.clone()]).await;
    let store = Arc::new(FakeStore::default());
    let auth = Authenticator::new(
        AuthSettings {
            issuer: issuer.to_owned(),
            audience: audience.to_owned(),
            jwks_url: jwks.url.clone(),
        },
        opts,
        store.clone() as Arc<dyn IdentityStore>,
    )
    .await;
    Fixture {
        key,
        jwks,
        store,
        auth,
    }
}

async fn fixture() -> Fixture {
    fixture_with(ISSUER, AUDIENCE, fast_opts()).await
}

fn app(layer: AuthLayer) -> Router {
    Router::new()
        .route(
            "/",
            get(|req: axum::extract::Request| async move {
                match req.extensions().get::<Identity>() {
                    Some(id) => format!("{}:{}", id.external_sub, id.role.as_str()),
                    None => "anon".to_owned(),
                }
            }),
        )
        .layer(layer)
}

async fn call(app: Router, auth: Option<&str>) -> (StatusCode, String) {
    let mut b = req("GET", "/");
    if let Some(a) = auth {
        b = b.header("authorization", a);
    }
    let resp = app.oneshot(b.body(empty()).unwrap()).await.unwrap();
    let status = resp.status();
    (status, body_string(resp).await)
}

// ── extract_bearer_token ────────────────────────────────────────────────────

#[test]
fn extract_bearer_token_cases() {
    let cases = [
        ("present", Some("Bearer my-token"), "my-token"),
        ("absent", None, ""),
        ("no space", Some("Bearermy-token"), ""),
        ("wrong scheme", Some("Basic dXNlcjpwYXNz"), ""),
        ("bearer lowercase", Some("bearer my-token"), "my-token"),
        ("multiple spaces", Some("Bearer  my-token"), " my-token"),
    ];
    for (name, header, want) in cases {
        let mut h = HeaderMap::new();
        if let Some(v) = header {
            h.insert("authorization", HeaderValue::from_static(v));
        }
        assert_eq!(extract_bearer_token(&h), want, "{name}");
    }
}

// ── claim_string ────────────────────────────────────────────────────────────

#[test]
fn claim_string_cases() {
    let v = json!({"present": "hello", "wrong": 42, "nilvalue": null});
    let claims = v.as_object().unwrap();
    assert_eq!(claim_string(claims, "present"), "hello");
    assert_eq!(claim_string(claims, "absent"), "");
    assert_eq!(claim_string(claims, "wrong"), "");
    assert_eq!(claim_string(claims, "nilvalue"), "");
}

// ── TtlCache ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn ttl_cache_hit_miss_expiry() {
    let c = TtlCache::new(Duration::from_millis(100));
    let id = Uuid::new_v4();
    assert!(c.get("k1").is_none());
    c.set("k1".to_owned(), id);
    assert_eq!(c.get("k1"), Some(id));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(c.get("k1").is_none());
}

#[test]
fn ttl_cache_overwrite() {
    let c = TtlCache::new(Duration::from_secs(60));
    let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
    c.set("k".to_owned(), a);
    c.set("k".to_owned(), b);
    assert_eq!(c.get("k"), Some(b));
}

// ── middleware ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn no_token_passes_through_without_identity() {
    let f = fixture().await;
    let (status, body) = call(app(AuthLayer::new(f.auth.clone())), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "anon");
}

#[tokio::test]
async fn invalid_token_returns_401() {
    let f = fixture().await;
    let (status, body) = call(
        app(AuthLayer::new(f.auth.clone())),
        Some("Bearer this.is.not.a.valid.jwt"),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, "{\"error\":\"invalid or expired token\"}\n");
}

#[tokio::test]
async fn non_bearer_authorization_is_rejected() {
    let f = fixture().await;
    let (status, _) = call(
        app(AuthLayer::new(f.auth.clone())),
        Some("Basic dXNlcjpwYXNz"),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn valid_token_sets_identity() {
    let f = fixture().await;
    let tok = sign(&f.key, &valid_claims(ISSUER, AUDIENCE, "alice"));
    let (status, body) = call(
        app(AuthLayer::new(f.auth.clone())),
        Some(&format!("Bearer {tok}")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "alice:student");
}

// ── validate: role mapping and claims ───────────────────────────────────────

#[tokio::test]
async fn role_mapping() {
    let f = fixture().await;
    let cases = [
        ("no roles claim", None, Role::Student),
        ("empty roles", Some(json!({})), Role::Student),
        ("faculty", Some(json!({"faculty": {}})), Role::Faculty),
        ("admin", Some(json!({"admin": {}})), Role::Admin),
        (
            "admin wins over faculty",
            Some(json!({"faculty": {}, "admin": {}})),
            Role::Admin,
        ),
        ("unknown role", Some(json!({"owner": {}})), Role::Student),
        ("wrong shape", Some(json!(["admin"])), Role::Student),
    ];
    for (name, roles, want) in cases {
        let mut c = valid_claims(ISSUER, AUDIENCE, "test-sub");
        if let Some(r) = roles {
            c.insert("urn:zitadel:iam:org:project:roles".to_owned(), r);
        }
        let id = f.auth.validate(&sign(&f.key, &c)).await.unwrap();
        assert_eq!(id.role, want, "{name}");
    }
}

#[tokio::test]
async fn claims_extraction_and_auto_provision_is_cached() {
    let f = fixture().await;
    let mut c = valid_claims(ISSUER, AUDIENCE, "user-sub-123");
    c.insert("email".to_owned(), json!("user@example.com"));
    c.insert("name".to_owned(), json!("Alice"));
    let tok = sign(&f.key, &c);
    let id = f.auth.validate(&tok).await.unwrap();
    assert_eq!(id.external_sub, "user-sub-123");
    assert_eq!(id.email, "user@example.com");
    assert_eq!(id.tenant_id, default_tenant());
    let again = f.auth.validate(&tok).await.unwrap();
    assert_eq!(again.user_id, id.user_id);
    assert_eq!(f.store.provisioned.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn expired_token_rejected() {
    let f = fixture().await;
    let mut c = valid_claims(ISSUER, AUDIENCE, "sub");
    c.insert("exp".to_owned(), json!(now() - 3600));
    assert!(f.auth.validate(&sign(&f.key, &c)).await.is_err());
    // No leeway, as in Go's jwt/v5 default.
    c.insert("exp".to_owned(), json!(now() - 5));
    assert!(f.auth.validate(&sign(&f.key, &c)).await.is_err());
}

#[tokio::test]
async fn missing_exp_rejected() {
    let f = fixture().await;
    let mut c = valid_claims(ISSUER, AUDIENCE, "sub");
    c.remove("exp");
    assert!(f.auth.validate(&sign(&f.key, &c)).await.is_err());
}

#[tokio::test]
async fn not_yet_valid_token_rejected() {
    let f = fixture().await;
    let mut c = valid_claims(ISSUER, AUDIENCE, "sub");
    c.insert("nbf".to_owned(), json!(now() + 3600));
    assert!(f.auth.validate(&sign(&f.key, &c)).await.is_err());
}

#[tokio::test]
async fn wrong_issuer_rejected() {
    let f = fixture_with("https://correct.issuer", AUDIENCE, fast_opts()).await;
    let c = valid_claims("https://wrong.issuer", AUDIENCE, "sub");
    assert!(f.auth.validate(&sign(&f.key, &c)).await.is_err());
    let mut c = valid_claims("https://correct.issuer", AUDIENCE, "sub");
    c.remove("iss");
    assert!(f.auth.validate(&sign(&f.key, &c)).await.is_err());
}

#[tokio::test]
async fn wrong_audience_rejected() {
    let f = fixture_with(ISSUER, "correct-audience", fast_opts()).await;
    let c = valid_claims(ISSUER, "wrong-audience", "sub");
    assert!(f.auth.validate(&sign(&f.key, &c)).await.is_err());
    let mut c = valid_claims(ISSUER, "correct-audience", "sub");
    c.remove("aud");
    assert!(f.auth.validate(&sign(&f.key, &c)).await.is_err());
    // A string audience is accepted, as in Go.
    let mut c = valid_claims(ISSUER, "correct-audience", "sub");
    c.insert("aud".to_owned(), json!("correct-audience"));
    assert!(f.auth.validate(&sign(&f.key, &c)).await.is_ok());
}

#[tokio::test]
async fn missing_sub_rejected() {
    let f = fixture().await;
    let mut c = valid_claims(ISSUER, AUDIENCE, "");
    assert!(f.auth.validate(&sign(&f.key, &c)).await.is_err());
    c.remove("sub");
    assert!(f.auth.validate(&sign(&f.key, &c)).await.is_err());
}

#[tokio::test]
async fn unregistered_org_id_rejected() {
    let f = fixture().await;
    let mut c = valid_claims(ISSUER, AUDIENCE, "attacker-sub");
    c.insert(
        "urn:zitadel:iam:org:id".to_owned(),
        json!("unregistered-org-xyz"),
    );
    let err = f.auth.validate(&sign(&f.key, &c)).await.unwrap_err();
    assert!(
        err.to_string().contains("unknown or inactive organisation"),
        "{err}"
    );
    assert_eq!(f.store.provisioned.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn org_id_resolves_tenant_and_is_cached() {
    let f = fixture().await;
    let tenant = Uuid::new_v4();
    f.store
        .tenants
        .lock()
        .unwrap()
        .insert("org-1".to_owned(), tenant);
    let mut c = valid_claims(ISSUER, AUDIENCE, "bob");
    c.insert("urn:zitadel:iam:org:id".to_owned(), json!("org-1"));
    let tok = sign(&f.key, &c);
    assert_eq!(f.auth.validate(&tok).await.unwrap().tenant_id, tenant);
    assert_eq!(f.auth.validate(&tok).await.unwrap().tenant_id, tenant);
    assert_eq!(f.store.tenant_lookups.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn orgless_token_uses_default_tenant() {
    let f = fixture().await;
    let c = valid_claims(ISSUER, AUDIENCE, "orgless-sub");
    let id = f.auth.validate(&sign(&f.key, &c)).await.unwrap();
    assert_eq!(id.tenant_id, default_tenant());
}

// ── algorithm pinning ──────────────────────────────────────────────────────

#[tokio::test]
async fn alg_none_and_hs256_confusion_rejected() {
    let f = fixture().await;
    let claims = serde_json::Value::Object(valid_claims(ISSUER, AUDIENCE, "mallory"));

    // alg=none, empty signature.
    let none = format!(
        "{}.{}.",
        b64(&json!({"alg": "none", "typ": "JWT", "kid": "test-key"})),
        b64(&claims)
    );
    assert!(f.auth.validate(&none).await.is_err());

    // HS256 signed with the RSA public key as the HMAC secret.
    for secret in [
        f.key.public_pem.as_bytes().to_vec(),
        serde_json::to_vec(&f.key.jwk).unwrap(),
    ] {
        let mut h = Header::new(Algorithm::HS256);
        h.kid = Some("test-key".to_owned());
        let tok = jsonwebtoken::encode(&h, &claims, &EncodingKey::from_secret(&secret)).unwrap();
        assert!(f.auth.validate(&tok).await.is_err());
    }

    // A token without a kid is rejected.
    let tok = jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &f.key.enc).unwrap();
    assert!(f.auth.validate(&tok).await.is_err());
}

#[tokio::test]
async fn symmetric_jwks_keys_are_never_used() {
    // A JWKS that (wrongly) publishes an oct key must not enable HS256 tokens.
    let secret = b"0123456789abcdef0123456789abcdef";
    let jwk = json!({"kty": "oct", "kid": "hmac", "alg": "HS256",
        "k": base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, secret)});
    let jwks = JwksServer::start(vec![jwk]).await;
    let auth = Authenticator::new(
        AuthSettings {
            issuer: ISSUER.to_owned(),
            audience: AUDIENCE.to_owned(),
            jwks_url: jwks.url.clone(),
        },
        fast_opts(),
        Arc::new(FakeStore::default()),
    )
    .await;
    let mut h = Header::new(Algorithm::HS256);
    h.kid = Some("hmac".to_owned());
    let claims = valid_claims(ISSUER, AUDIENCE, "mallory");
    let tok = jsonwebtoken::encode(&h, &claims, &EncodingKey::from_secret(secret)).unwrap();
    assert!(auth.validate(&tok).await.is_err());
}

// ── JWKS lifecycle ─────────────────────────────────────────────────────────

#[tokio::test]
async fn jwks_request_carries_issuer_host() {
    let f = fixture().await;
    assert!(f.jwks.hits() >= 1);
    assert_eq!(*f.jwks.last_host.lock().unwrap(), "sso.test");
}

#[tokio::test]
async fn jwks_rotation_picks_up_new_key() {
    let f = fixture().await;
    let next = rsa_key("next-key");
    *f.jwks.keys.lock().unwrap() = vec![next.jwk.clone()];
    let tok = sign(&next, &valid_claims(ISSUER, AUDIENCE, "rotated"));
    let id = f.auth.validate(&tok).await.unwrap();
    assert_eq!(id.external_sub, "rotated");
}

async fn forged_burst(f: &Fixture, n: usize) {
    let mut handles = Vec::new();
    for i in 0..n {
        let auth = f.auth.clone();
        let tok = sign_kid(
            &f.key,
            &format!("forged-{i}"),
            &valid_claims(ISSUER, AUDIENCE, "x"),
        );
        handles.push(tokio::spawn(async move { auth.validate(&tok).await }));
    }
    for h in handles {
        assert!(h.await.unwrap().is_err());
    }
}

#[tokio::test]
async fn forged_kid_refetch_is_rate_limited() {
    let f = fixture_with(
        ISSUER,
        AUDIENCE,
        JwksOptions {
            refetch_min_interval: Duration::from_secs(60),
            ..fast_opts()
        },
    )
    .await;
    let start = f.jwks.hits();
    // Fifty concurrent forged kids cause a single refetch, and later ones none.
    forged_burst(&f, 50).await;
    assert_eq!(f.jwks.hits(), start + 1);
    forged_burst(&f, 50).await;
    assert_eq!(f.jwks.hits(), start + 1);
    // The known key still works without a fetch.
    let ok = sign(&f.key, &valid_claims(ISSUER, AUDIENCE, "ok"));
    assert!(f.auth.validate(&ok).await.is_ok());
    assert_eq!(f.jwks.hits(), start + 1);
}

#[tokio::test]
async fn forged_kids_refetch_again_only_after_interval() {
    let f = fixture().await;
    let start = f.jwks.hits();
    forged_burst(&f, 30).await;
    assert_eq!(f.jwks.hits(), start + 1);
    tokio::time::sleep(Duration::from_millis(400)).await;
    forged_burst(&f, 30).await;
    assert_eq!(f.jwks.hits(), start + 2);
}

#[tokio::test]
async fn jwks_down_fails_closed() {
    let key = rsa_key("test-key");
    let jwks = JwksServer::start(vec![key.jwk.clone()]).await;
    jwks.fail.store(true, Ordering::SeqCst);
    let auth = Authenticator::new(
        AuthSettings {
            issuer: ISSUER.to_owned(),
            audience: AUDIENCE.to_owned(),
            jwks_url: jwks.url.clone(),
        },
        fast_opts(),
        Arc::new(FakeStore::default()),
    )
    .await;
    let tok = sign(&key, &valid_claims(ISSUER, AUDIENCE, "alice"));
    let (status, _) = call(
        app(AuthLayer::new(auth.clone())),
        Some(&format!("Bearer {tok}")),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Recovers once the JWKS endpoint is back.
    jwks.fail.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(auth.validate(&tok).await.is_ok());
}

#[tokio::test]
async fn jwks_unreachable_fails_closed() {
    let key = rsa_key("test-key");
    let auth = Authenticator::new(
        AuthSettings {
            issuer: ISSUER.to_owned(),
            audience: AUDIENCE.to_owned(),
            jwks_url: "http://127.0.0.1:1/oauth/v2/keys".to_owned(),
        },
        fast_opts(),
        Arc::new(FakeStore::default()),
    )
    .await;
    let tok = sign(&key, &valid_claims(ISSUER, AUDIENCE, "alice"));
    assert!(auth.validate(&tok).await.is_err());
}

// ── configuration: AC_AUTH_ENABLED / AC_ALLOW_UNAUTH ───────────────────────

fn cfg(pairs: &[(&str, &str)]) -> Config {
    let m: HashMap<String, String> = pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    Config::from_map(&m).unwrap()
}

#[tokio::test]
async fn invalid_token_with_allow_unauth_is_rejected() {
    let key = rsa_key("test-key");
    let jwks = JwksServer::start(vec![key.jwk.clone()]).await;
    let c = cfg(&[
        ("AC_AUTH_ENABLED", "true"),
        ("AC_ALLOW_UNAUTH", "true"),
        ("AC_OIDC_ISSUER", ISSUER),
        ("AC_OIDC_AUDIENCE", AUDIENCE),
        ("AC_OIDC_JWKS_URL", &jwks.url),
    ]);
    let layer = AuthLayer::from_config(&c, Arc::new(FakeStore::default()))
        .await
        .unwrap();
    let (status, _) = call(app(layer.clone()), Some("Bearer garbage.token.here")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let mut bad = valid_claims(ISSUER, AUDIENCE, "x");
    bad.insert("exp".to_owned(), json!(now() - 10));
    let (status, _) = call(
        app(layer.clone()),
        Some(&format!("Bearer {}", sign(&key, &bad))),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, body) = call(app(layer), None).await;
    assert_eq!((status, body.as_str()), (StatusCode::OK, "anon"));
}

#[tokio::test]
async fn auth_disabled_requires_allow_unauth() {
    let c = cfg(&[("AC_AUTH_ENABLED", "false")]);
    assert!(
        AuthLayer::from_config(&c, Arc::new(FakeStore::default()))
            .await
            .is_err()
    );
    let c = cfg(&[("AC_AUTH_ENABLED", "false"), ("AC_ALLOW_UNAUTH", "true")]);
    let layer = AuthLayer::from_config(&c, Arc::new(FakeStore::default()))
        .await
        .unwrap();
    let (status, body) = call(app(layer), Some("Bearer whatever")).await;
    assert_eq!((status, body.as_str()), (StatusCode::OK, "anon"));
}

#[tokio::test]
async fn auth_enabled_requires_issuer_and_jwks_url() {
    let base = cfg(&[("AC_AUTH_ENABLED", "true"), ("AC_OIDC_AUDIENCE", AUDIENCE)]);
    let mut no_issuer = base.clone();
    no_issuer.oidc_issuer.clear();
    let mut no_jwks = base;
    no_jwks.oidc_jwks_url.clear();
    for (c, label) in [(no_issuer, "AC_OIDC_ISSUER"), (no_jwks, "AC_OIDC_JWKS_URL")] {
        let err = AuthLayer::from_config(&c, Arc::new(FakeStore::default()))
            .await
            .err()
            .expect("must fail");
        assert!(err.to_string().contains(label), "{err}");
    }
}

#[tokio::test]
async fn auth_enabled_requires_audience() {
    let c = cfg(&[("AC_AUTH_ENABLED", "true")]);
    assert!(
        AuthLayer::from_config(&c, Arc::new(FakeStore::default()))
            .await
            .is_err()
    );
}
