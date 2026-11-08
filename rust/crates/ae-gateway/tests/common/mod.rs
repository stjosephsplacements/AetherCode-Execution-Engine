#![allow(dead_code)]

use ae_gateway::auth::{IdentityStore, StoreError};
use ae_queue::BoxFuture;
use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::traits::PublicKeyParts;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::redis::Redis;
use uuid::Uuid;

pub const ISSUER: &str = "https://sso.test";
pub const AUDIENCE: &str = "myapp";

pub struct Key {
    pub kid: String,
    pub enc: EncodingKey,
    pub jwk: Value,
    /// PKCS#1 PEM of the public key, used as the HMAC secret in alg-confusion tests.
    pub public_pem: String,
}

pub fn rsa_key(kid: &str) -> Key {
    let k = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let der = k.to_pkcs1_der().unwrap();
    let enc = EncodingKey::from_rsa_der(der.as_bytes());
    let public = k.to_public_key();
    let jwk = json!({
        "kty": "RSA",
        "use": "sig",
        "alg": "RS256",
        "kid": kid,
        "n": URL_SAFE_NO_PAD.encode(public.n().to_bytes_be()),
        "e": URL_SAFE_NO_PAD.encode(public.e().to_bytes_be()),
    });
    let public_pem =
        rsa::pkcs1::EncodeRsaPublicKey::to_pkcs1_pem(&public, Default::default()).unwrap();
    Key {
        kid: kid.to_owned(),
        enc,
        jwk,
        public_pem,
    }
}

pub fn now() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
    .unwrap()
}

pub fn valid_claims(issuer: &str, audience: &str, sub: &str) -> serde_json::Map<String, Value> {
    let v = json!({
        "iss": issuer,
        "aud": [audience],
        "sub": sub,
        "email": "test@example.com",
        "name": "Test User",
        "exp": now() + 3600,
        "iat": now(),
    });
    v.as_object().unwrap().clone()
}

pub fn sign(key: &Key, claims: &serde_json::Map<String, Value>) -> String {
    let mut h = Header::new(Algorithm::RS256);
    h.kid = Some(key.kid.clone());
    jsonwebtoken::encode(&h, claims, &key.enc).unwrap()
}

pub fn sign_kid(key: &Key, kid: &str, claims: &serde_json::Map<String, Value>) -> String {
    let mut h = Header::new(Algorithm::RS256);
    h.kid = Some(kid.to_owned());
    jsonwebtoken::encode(&h, claims, &key.enc).unwrap()
}

pub fn b64(v: &Value) -> String {
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(v).unwrap())
}

/// Local JWKS endpoint: serves `keys`, counts hits, can be switched to fail with 500.
#[derive(Clone)]
pub struct JwksServer {
    pub url: String,
    pub hits: Arc<AtomicUsize>,
    pub keys: Arc<Mutex<Vec<Value>>>,
    pub fail: Arc<AtomicBool>,
    pub last_host: Arc<Mutex<String>>,
}

impl JwksServer {
    pub async fn start(keys: Vec<Value>) -> Self {
        let s = JwksServer {
            url: String::new(),
            hits: Arc::default(),
            keys: Arc::new(Mutex::new(keys)),
            fail: Arc::default(),
            last_host: Arc::default(),
        };
        let st = s.clone();
        let app = Router::new().route(
            "/oauth/v2/keys",
            get(move |headers: HeaderMap| {
                let st = st.clone();
                async move {
                    st.hits.fetch_add(1, Ordering::SeqCst);
                    *st.last_host.lock().unwrap() = headers
                        .get("host")
                        .and_then(|h| h.to_str().ok())
                        .unwrap_or_default()
                        .to_owned();
                    if st.fail.load(Ordering::SeqCst) {
                        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                    }
                    let keys = st.keys.lock().unwrap().clone();
                    axum::Json(json!({ "keys": keys })).into_response()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        JwksServer {
            url: format!("http://{addr}/oauth/v2/keys"),
            ..s
        }
    }

    pub fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }
}

/// In-memory identity store: org slug to tenant, and provisioned users.
#[derive(Default)]
pub struct FakeStore {
    pub tenants: Mutex<HashMap<String, Uuid>>,
    pub users: Mutex<HashMap<(Uuid, String), Uuid>>,
    pub provisioned: AtomicUsize,
    pub tenant_lookups: AtomicUsize,
}

impl IdentityStore for FakeStore {
    fn tenant_by_org<'a>(&'a self, org_id: &'a str) -> BoxFuture<'a, Result<Uuid, StoreError>> {
        self.tenant_lookups.fetch_add(1, Ordering::SeqCst);
        let r = self
            .tenants
            .lock()
            .unwrap()
            .get(org_id)
            .copied()
            .ok_or_else(|| StoreError::from("no rows"));
        Box::pin(async move { r })
    }

    fn find_user<'a>(
        &'a self,
        tenant_id: Uuid,
        sub: &'a str,
    ) -> BoxFuture<'a, Result<Option<Uuid>, StoreError>> {
        let r = self
            .users
            .lock()
            .unwrap()
            .get(&(tenant_id, sub.to_owned()))
            .copied();
        Box::pin(async move { Ok(r) })
    }

    fn provision_user<'a>(
        &'a self,
        tenant_id: Uuid,
        sub: &'a str,
        _email: &'a str,
        _name: &'a str,
    ) -> BoxFuture<'a, Result<Uuid, StoreError>> {
        self.provisioned.fetch_add(1, Ordering::SeqCst);
        let id = *self
            .users
            .lock()
            .unwrap()
            .entry((tenant_id, sub.to_owned()))
            .or_insert_with(Uuid::new_v4);
        Box::pin(async move { Ok(id) })
    }
}

pub async fn redis() -> (ContainerAsync<Redis>, String) {
    let mut attempt = 0;
    loop {
        attempt += 1;
        let c = Redis::default()
            .with_tag("7")
            .start()
            .await
            .expect("start redis:7 (is docker running?)");
        match c.get_host_port_ipv4(6379).await {
            Ok(p) => return (c, format!("127.0.0.1:{p}")),
            Err(e) if attempt < 5 => eprintln!("retrying redis container: {e}"),
            Err(e) => panic!("redis port: {e}"),
        }
    }
}

pub async fn redis_conn(addr: &str) -> redis::aio::ConnectionManager {
    redis::Client::open(format!("redis://{addr}/"))
        .unwrap()
        .get_connection_manager()
        .await
        .unwrap()
}

pub async fn body_string(resp: axum::response::Response) -> String {
    let b = http_body_util::BodyExt::collect(resp.into_body())
        .await
        .unwrap()
        .to_bytes();
    String::from_utf8(b.to_vec()).unwrap()
}

pub fn req(method: &str, uri: &str) -> axum::http::request::Builder {
    Request::builder().method(method).uri(uri)
}

pub fn empty() -> Body {
    Body::empty()
}
