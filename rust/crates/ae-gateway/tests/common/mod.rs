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

// ---- Full gateway stack: Postgres + Redis containers, fake sandbox, real server ----

use ae_gateway::AppState;
use ae_gateway::auth::{AuthLayer, Authenticator, JwksOptions};
use ae_sandbox::{Cmd, FileId, Registry, RunResult, STATUS_ACCEPTED, STATUS_NONZERO_EXIT, Sandbox};
use std::collections::HashSet;
use std::time::Duration;
use testcontainers_modules::postgres::Postgres;

/// Scriptable go-judge stand-in. Test runs print `stdout`; the registry self-test
/// needs "hello", so `stdout` starts as "hello" and tests switch it after loading.
pub struct FakeSandbox {
    pub missing: HashSet<String>,
    pub stdout: Mutex<String>,
    pub version_fails: AtomicBool,
    /// Test runs wait for a read lock: hold the write lock to stall the judge.
    pub gate: Arc<tokio::sync::RwLock<()>>,
    next: AtomicUsize,
}

impl FakeSandbox {
    pub fn new(missing: &[&str]) -> Self {
        Self {
            missing: missing.iter().map(|s| (*s).to_owned()).collect(),
            stdout: Mutex::new("hello".into()),
            version_fails: AtomicBool::new(false),
            gate: Arc::default(),
            next: AtomicUsize::new(0),
        }
    }
}

fn accepted() -> RunResult {
    RunResult {
        status: STATUS_ACCEPTED.into(),
        ..RunResult::default()
    }
}

#[async_trait::async_trait]
impl Sandbox for FakeSandbox {
    async fn run(&self, cmds: Vec<Cmd>) -> Result<Vec<RunResult>, ae_sandbox::Error> {
        if cmds[0].args[0] == "/usr/bin/test" {
            return Ok(cmds
                .iter()
                .map(|c| {
                    if self.missing.contains(&c.args[2]) {
                        RunResult {
                            status: STATUS_NONZERO_EXIT.into(),
                            exit_status: 1,
                            ..RunResult::default()
                        }
                    } else {
                        accepted()
                    }
                })
                .collect());
        }
        let _g = self.gate.read().await;
        Ok(cmds
            .iter()
            .map(|c| {
                let mut r = accepted();
                if let Some(bin) = c.copy_out_cached.first() {
                    let n = self.next.fetch_add(1, Ordering::SeqCst);
                    r.file_ids.insert(bin.clone(), format!("bin-{n}"));
                } else {
                    r.files
                        .insert("stdout".into(), self.stdout.lock().unwrap().clone());
                    r.time = 100_000;
                    r.memory = 4096;
                }
                r
            })
            .collect())
    }
    async fn cache_file(&self, _: &str, _: &[u8]) -> Result<FileId, ae_sandbox::Error> {
        let n = self.next.fetch_add(1, Ordering::SeqCst);
        Ok(FileId(format!("src-{n}")))
    }
    async fn delete_file(&self, _: FileId) -> Result<(), ae_sandbox::Error> {
        Ok(())
    }
    async fn version(&self) -> Result<String, ae_sandbox::Error> {
        if self.version_fails.load(Ordering::SeqCst) {
            return Err(ae_sandbox::Error::Config("down".into()));
        }
        Ok("1.13.0".into())
    }
}

pub async fn postgres() -> (ContainerAsync<Postgres>, String) {
    let c = Postgres::default()
        .with_user("aethercode")
        .with_password("aethercode")
        .with_db_name("aethercode_test")
        .with_tag("18")
        .start()
        .await
        .expect("start postgres:18 (is docker running?)");
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    (
        c,
        format!("postgres://aethercode:aethercode@127.0.0.1:{port}/aethercode_test"),
    )
}

#[derive(Default)]
pub struct StackOpts {
    /// Executables the sandbox lacks (their languages are unavailable).
    pub missing: Vec<&'static str>,
    /// Validate bearer tokens against a local JWKS; otherwise auth is disabled.
    pub auth: bool,
    /// `AC_ADMISSION_MAX_QUEUE_DEPTH`; 0 keeps the default.
    pub max_depth: usize,
    /// Start a worker consuming the streams.
    pub worker: bool,
    /// Leave the readiness flag cleared (stacks are ready by default, as after startup).
    pub not_ready: bool,
    /// Extra `AC_*` settings.
    pub env: Vec<(&'static str, &'static str)>,
}

pub struct Stack {
    pub pg: ContainerAsync<Postgres>,
    pub _redis: ContainerAsync<Redis>,
    pub redis_addr: String,
    pub cfg: ae_core::Config,
    pub db: ae_db::Db,
    pub queue: ae_queue::Queue,
    pub sb: Arc<FakeSandbox>,
    pub state: AppState,
    pub url: String,
    pub key: Option<Key>,
    pub jwks: Option<JwksServer>,
    pub shutdown: tokio_util::sync::CancellationToken,
}

impl Drop for Stack {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

pub async fn stack(opts: StackOpts) -> Stack {
    let ((pg, db_url), (rc, redis_addr)) = tokio::join!(postgres(), redis());
    let mut env = HashMap::from([
        ("AC_DATABASE_URL".to_owned(), db_url),
        ("AC_REDIS_ADDR".to_owned(), redis_addr.clone()),
        ("AC_BLOCK_READ_TIMEOUT".to_owned(), "100ms".to_owned()),
        ("AC_WORKER_COUNT".to_owned(), "1".to_owned()),
    ]);
    for (k, v) in &opts.env {
        env.insert((*k).to_owned(), (*v).to_owned());
    }
    if opts.max_depth > 0 {
        env.insert(
            "AC_ADMISSION_MAX_QUEUE_DEPTH".to_owned(),
            opts.max_depth.to_string(),
        );
    }
    let cfg = ae_core::Config::from_map(&env).unwrap();
    let db = ae_db::connect(&cfg).await.unwrap();
    db.migrate().await.unwrap();
    let queue = ae_queue::Queue::connect(&cfg, Arc::new(db.clone()))
        .await
        .unwrap();
    let sb = Arc::new(FakeSandbox::new(&opts.missing));
    let registry = Arc::new(
        Registry::load_with_budget(&cfg, sb.as_ref(), Duration::from_secs(5))
            .await
            .unwrap(),
    );
    *sb.stdout.lock().unwrap() = "3".into();

    let (auth, key, jwks) = if opts.auth {
        let key = rsa_key("k1");
        let jwks = JwksServer::start(vec![key.jwk.clone()]).await;
        let a = Authenticator::new(
            ae_gateway::auth::AuthSettings {
                issuer: ISSUER.into(),
                audience: AUDIENCE.into(),
                jwks_url: jwks.url.clone(),
            },
            JwksOptions::default(),
            Arc::new(db.clone()),
        )
        .await;
        (AuthLayer::new(a), Some(key), Some(jwks))
    } else {
        let mut c = cfg.clone();
        c.auth_enabled = false;
        c.allow_unauth = true;
        (
            AuthLayer::from_config(&c, Arc::new(db.clone()))
                .await
                .unwrap(),
            None,
            None,
        )
    };
    let state = AppState::new(
        &cfg,
        db.clone(),
        queue.clone(),
        sb.clone() as Arc<dyn Sandbox>,
        registry.clone(),
        auth,
        redis_conn(&redis_addr).await,
    );
    state.set_ready(!opts.not_ready);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = ae_gateway::router(state.clone());
    let s = shutdown.clone();
    tokio::spawn(async move {
        ae_gateway::serve(listener, app, async move { s.cancelled().await })
            .await
            .unwrap();
    });
    if opts.worker {
        let mut wo = ae_worker::WorkerOptions::from_config(&cfg);
        wo.observer = state.worker_observer();
        let w = ae_worker::Worker::new(
            Arc::new(db.clone()),
            Arc::new(queue.clone()),
            sb.clone() as Arc<dyn Sandbox>,
            registry,
            wo,
        );
        tokio::spawn(w.run(shutdown.clone()));
    }
    Stack {
        pg,
        _redis: rc,
        redis_addr,
        cfg,
        db,
        queue,
        sb,
        state,
        url,
        key,
        jwks,
        shutdown,
    }
}

impl Stack {
    /// Bearer token for `sub` in the default tenant.
    pub fn token(&self, sub: &str) -> String {
        sign(
            self.key.as_ref().expect("stack started with auth"),
            &valid_claims(ISSUER, AUDIENCE, sub),
        )
    }
}

pub fn http() -> reqwest::Client {
    reqwest::Client::builder().build().unwrap()
}

/// POSTs `body` to /api/v1/execute with an optional bearer token.
pub async fn post_execute(st: &Stack, body: &str, token: Option<&str>) -> reqwest::Response {
    let mut r = http()
        .post(format!("{}/api/v1/execute", st.url))
        .header("content-type", "application/json")
        .body(body.to_owned());
    if let Some(t) = token {
        r = r.bearer_auth(t);
    }
    r.send().await.unwrap()
}

pub async fn error_of(resp: reqwest::Response) -> (u16, String) {
    let code = resp.status().as_u16();
    let v: Value = resp.json().await.unwrap();
    (code, v["error"].as_str().unwrap_or_default().to_owned())
}

pub fn run_body(expected: &str, checker: &str) -> String {
    let c = if checker.is_empty() {
        String::new()
    } else {
        format!(r#""checker":"{checker}","#)
    };
    format!(
        r#"{{"language":"python","source_code":"print(1+2)",{c}"tests":[{{"input":"","expected_output":{}}}]}}"#,
        serde_json::to_string(expected).unwrap()
    )
}

/// Reads SSE frames from `resp` into events until the body ends or `limit` passes.
pub async fn read_sse(mut resp: reqwest::Response, limit: Duration) -> (Vec<Value>, String) {
    let mut raw = String::new();
    let _ = tokio::time::timeout(limit, async {
        while let Ok(Some(chunk)) = resp.chunk().await {
            raw.push_str(&String::from_utf8_lossy(&chunk));
        }
    })
    .await;
    (parse_sse(&raw), raw)
}

pub fn parse_sse(raw: &str) -> Vec<Value> {
    raw.split("\n\n")
        .filter_map(|frame| {
            frame
                .lines()
                .find_map(|l| l.strip_prefix("data: "))
                .and_then(|d| serde_json::from_str(d).ok())
        })
        .collect()
}
