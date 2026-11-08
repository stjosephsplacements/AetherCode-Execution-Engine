//! JSON handlers: execute, get and cancel a submission, health, languages, readiness
//! (port of `internal/gateway/handler.go`; status codes, messages and bodies match Go).

use crate::admission::{Admission, Drain};
use crate::auth::{AuthLayer, Identity};
use crate::middleware::CorsLayer;
use crate::ratelimit::RateLimitLayer;
use crate::{json_error, metrics};
use ae_core::Config;
use ae_core::eventlog::{Event, EventType};
use ae_core::model::{Checker, DEFAULT_TENANT_ID, Job, Limits, Status, Submission};
use ae_core::observe::Observer;
use ae_db::Db;
use ae_queue::{Queue, TestData, TestSet};
use ae_sandbox::{
    MAX_RUN_CPU, MAX_RUN_MEM, MAX_RUN_PROCS, MAX_RUN_STACK, MIN_RUN_CPU, MIN_RUN_MEM,
    MIN_RUN_PROCS, MIN_RUN_STACK, Registry, Sandbox,
};
use axum::Extension;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use redis::aio::ConnectionManager;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;

pub const MAX_SOURCE_SIZE: usize = 64 * 1024;
pub const MAX_TESTS: usize = 100;
pub const MAX_TEST_DATA_SIZE: usize = 8 << 20;
pub const MAX_BODY_SIZE: usize = 64 << 20;
pub const MAX_IDEMPOTENCY_KEY_LEN: usize = 255;
pub const MAX_CALLBACK_SECRET_LEN: usize = 256;
/// Concurrent execute handlers (Go's `ingestSem`), so a burst cannot exhaust the PG pool.
pub const INGEST_CONCURRENCY: usize = 512;
const MAX_FILES: usize = 10;
const MAX_FILES_BYTES: usize = 128 << 10;
/// Tests per sandbox `/run` batch; one batch is one unit of admission cost.
const TESTS_PER_BATCH: usize = 32;
const HEALTH_TIMEOUT: Duration = Duration::from_secs(5);
const TOKEN_TIMEOUT: Duration = Duration::from_secs(2);
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(3);

/// Everything the handlers share. Cheap to clone.
#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub queue: Queue,
    pub sandbox: Arc<dyn Sandbox>,
    pub registry: Arc<Registry>,
    pub admission: Arc<Admission>,
    pub auth: AuthLayer,
    pub rate_limit: RateLimitLayer,
    pub cors: CorsLayer,
    /// Capacity [`INGEST_CONCURRENCY`]; an execute that cannot take a permit gets 429.
    pub ingest: Arc<Semaphore>,
    pub ready: Arc<AtomicBool>,
    pub started: Instant,
    /// `workers.configured` in the health response.
    pub worker_count: usize,
    /// Jobs expire `job_timeout` after enqueue (`expires_at` on the stream entry).
    pub job_timeout: Duration,
}

impl AppState {
    /// Wires the layers from `cfg`: CORS origins, rate limits (on `redis`), admission
    /// at `cfg.max_queue_depth()` with a fresh [`Drain`].
    #[must_use]
    pub fn new(
        cfg: &Config,
        db: Db,
        queue: Queue,
        sandbox: Arc<dyn Sandbox>,
        registry: Arc<Registry>,
        auth: AuthLayer,
        redis: ConnectionManager,
    ) -> Self {
        let admission = Arc::new(Admission::new(
            queue.clone(),
            cfg.max_queue_depth(),
            Arc::new(Drain::new()),
        ));
        Self {
            db,
            queue,
            sandbox,
            registry,
            admission,
            auth,
            rate_limit: RateLimitLayer::from_config(cfg, redis),
            cors: CorsLayer::new(&cfg.cors_allowed_origins),
            ingest: Arc::new(Semaphore::new(INGEST_CONCURRENCY)),
            ready: Arc::new(AtomicBool::new(false)),
            started: Instant::now(),
            worker_count: cfg.effective_workers(),
            job_timeout: cfg.job_timeout,
        }
    }

    /// Job completions per second, read by admission control.
    #[must_use]
    pub fn drain(&self) -> &Arc<Drain> {
        self.admission.drain()
    }

    /// The hook the engine must put in `ae_worker::WorkerOptions::observer`: it records
    /// the job metrics and calls [`Drain::complete`] for every job a worker finishes, so
    /// the 503 `Retry-After` follows the real drain rate.
    #[must_use]
    pub fn worker_observer(&self) -> Observer {
        Observer(Arc::new(metrics::WorkerMetrics::new(self.drain().clone())))
    }

    /// `/readyz` answers 200 only after this is set (startup finished) and 503 again
    /// once it is cleared (draining for shutdown).
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
    }
}

/// JSON body with Go's `json.Encoder` trailing newline.
pub(crate) fn json_response<T: Serialize>(status: StatusCode, v: &T) -> Response {
    match serde_json::to_vec(v) {
        Ok(mut body) => {
            body.push(b'\n');
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
        Err(_) => json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error"),
    }
}

fn bad(msg: &str) -> Response {
    json_error(StatusCode::BAD_REQUEST, msg)
}

/// Go decodes JSON `null` as the zero value; serde would reject it for non-`Option`s.
fn nullable<'de, D: Deserializer<'de>, T: Default + Deserialize<'de>>(d: D) -> Result<T, D::Error> {
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ExecuteRequest {
    #[serde(deserialize_with = "nullable")]
    language: String,
    #[serde(deserialize_with = "nullable")]
    source_code: String,
    idempotency_key: Option<String>,
    #[serde(deserialize_with = "nullable")]
    tests: Vec<TestInput>,
    time_limit_ms: Option<u64>,
    memory_limit_kb: Option<u64>,
    stack_limit_kb: Option<u64>,
    max_processes: Option<u64>,
    #[serde(deserialize_with = "nullable")]
    args: Vec<String>,
    #[serde(deserialize_with = "nullable")]
    checker: String,
    #[serde(deserialize_with = "nullable")]
    checker_code: String,
    #[serde(deserialize_with = "nullable")]
    checker_language: String,
    #[serde(deserialize_with = "nullable")]
    interactor_code: String,
    #[serde(deserialize_with = "nullable")]
    interactor_language: String,
    #[serde(deserialize_with = "nullable")]
    files: BTreeMap<String, String>,
    #[serde(deserialize_with = "nullable")]
    compiler_flags: Vec<String>,
    #[serde(deserialize_with = "nullable")]
    env: Vec<String>,
    #[serde(deserialize_with = "nullable")]
    callback_url: String,
    #[serde(deserialize_with = "nullable")]
    callback_secret: String,
    #[serde(deserialize_with = "nullable")]
    priority: String,
    #[serde(deserialize_with = "nullable")]
    output: String,
    /// Only to reject legacy submit-mode requests.
    #[serde(deserialize_with = "nullable")]
    mode: String,
    problem_version_id: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct TestInput {
    #[serde(deserialize_with = "nullable")]
    input: String,
    #[serde(deserialize_with = "nullable")]
    expected_output: String,
    #[serde(deserialize_with = "nullable")]
    env: Vec<String>,
}

#[derive(Serialize)]
struct ExecuteResponse {
    job_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    stream_token: String,
}

/// Like Go's `json.Decoder.Decode`: the first JSON value in the body, trailing bytes ignored.
fn decode_first(body: &[u8]) -> Option<ExecuteRequest> {
    serde_json::Deserializer::from_slice(body)
        .into_iter::<ExecuteRequest>()
        .next()?
        .ok()
}

fn valid_checker(s: &str) -> bool {
    matches!(s, "" | "exact" | "token" | "float" | "custom")
}

fn valid_output(s: &str) -> bool {
    matches!(s, "" | "full" | "preview" | "none")
}

fn valid_priority(s: &str) -> bool {
    matches!(s, "" | "run" | "submit" | "bulk")
}

/// Go's `url.ParseRequestURI` followed by the http(s) scheme check: the scheme is
/// matched case-insensitively and ASCII control characters are rejected. The SSRF guard
/// runs at dispatch in the worker, as in Go.
fn valid_callback_url(s: &str) -> bool {
    let Some((scheme, _)) = s.split_once(':') else {
        return false;
    };
    let scheme = scheme.to_ascii_lowercase();
    (scheme == "http" || scheme == "https") && !s.chars().any(|c| c.is_ascii_control())
}

fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// Checks a limit given in `unit`s against the byte or nanosecond range.
fn scaled(v: Option<u64>, unit: u64, min: u64, max: u64) -> Result<u64, ()> {
    match v {
        None => Ok(0),
        Some(v) => match v.checked_mul(unit) {
            Some(x) if (min..=max).contains(&x) => Ok(x),
            _ => Err(()),
        },
    }
}

/// `POST /api/v1/execute`.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    State(st): State<AppState>,
    identity: Option<Extension<Identity>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if !ct.starts_with("application/json") {
        return json_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/json",
        );
    }
    let Ok(_permit) = st.ingest.clone().try_acquire_owned() else {
        let mut r = json_error(
            StatusCode::TOO_MANY_REQUESTS,
            "server is busy, try again later",
        );
        r.headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        return r;
    };
    let Ok(bytes) = axum::body::to_bytes(body, MAX_BODY_SIZE).await else {
        return bad("invalid JSON");
    };
    let Some(mut req) = decode_first(&bytes) else {
        return bad("invalid JSON");
    };
    drop(bytes);

    if (!req.mode.is_empty() && req.mode != "run")
        || req
            .problem_version_id
            .as_deref()
            .is_some_and(|p| !p.is_empty())
    {
        return bad(
            "mode=submit and problem_version_id are no longer supported; send tests in the request body",
        );
    }
    let langs = st.registry.languages();
    let Some(lang) = langs.get(&req.language) else {
        return bad("unsupported language");
    };
    if st.registry.get(&req.language).is_none() {
        return bad(&format!(
            "language {} is not installed on this engine",
            req.language
        ));
    }
    if req.source_code.is_empty() {
        return bad("source_code is required");
    }
    if req.source_code.len() > MAX_SOURCE_SIZE {
        return bad("source_code exceeds 64KB");
    }
    let identity = identity.map(|Extension(i)| i);
    if req.tests.is_empty() {
        return bad("at least one test is required");
    }
    if req.tests.len() > MAX_TESTS {
        return bad("too many tests (max 100)");
    }
    if req
        .tests
        .iter()
        .any(|t| t.input.len() > MAX_TEST_DATA_SIZE || t.expected_output.len() > MAX_TEST_DATA_SIZE)
    {
        return bad("test input/output exceeds 8MB");
    }
    if let Err(e) = ae_sandbox::validate_env(&req.env, langs) {
        return bad(&format!("env: {e}"));
    }
    // The submission-level env is merged into each test here, once, and re-validated:
    // the caps apply to what actually reaches the sandbox.
    let mut test_envs = Vec::with_capacity(req.tests.len());
    for (i, t) in req.tests.iter().enumerate() {
        if let Err(e) = ae_sandbox::validate_env(&t.env, langs) {
            return bad(&format!("tests[{i}].env: {e}"));
        }
        let merged = ae_sandbox::merge_env(&req.env, &t.env);
        if let Err(e) = ae_sandbox::validate_env(&merged, langs) {
            return bad(&format!("tests[{i}].env: {e}"));
        }
        test_envs.push(merged);
    }
    if !valid_checker(&req.checker) {
        return bad("checker must be one of: exact, token, float, custom");
    }
    if req.checker == "custom" || !req.checker_code.is_empty() {
        if req.checker_code.is_empty() {
            return bad("checker_code is required when checker is custom");
        }
        if req.checker_code.len() > MAX_SOURCE_SIZE {
            return bad("checker_code exceeds 64KB");
        }
        if req.checker_language.is_empty() {
            "python".clone_into(&mut req.checker_language);
        }
        if langs.get(&req.checker_language).is_none() {
            return bad("unsupported checker_language");
        }
        if st.registry.get(&req.checker_language).is_none() {
            return bad(&format!(
                "checker_language {} is not installed on this engine",
                req.checker_language
            ));
        }
        "custom".clone_into(&mut req.checker);
    }
    if !req.interactor_code.is_empty() {
        if req.interactor_code.len() > MAX_SOURCE_SIZE {
            return bad("interactor_code exceeds 64KB");
        }
        if req.interactor_language.is_empty() {
            "python".clone_into(&mut req.interactor_language);
        }
        if langs.get(&req.interactor_language).is_none() {
            return bad("unsupported interactor_language");
        }
        if st.registry.get(&req.interactor_language).is_none() {
            return bad(&format!(
                "interactor_language {} is not installed on this engine",
                req.interactor_language
            ));
        }
    }
    if !req.files.is_empty()
        && let Err(e) = ae_sandbox::validate_files(&req.files, MAX_FILES, MAX_FILES_BYTES)
    {
        return bad(&e);
    }
    if !req.compiler_flags.is_empty()
        && let Err(e) = ae_sandbox::validate_compiler_flags(lang, &req.compiler_flags)
    {
        return bad(&e);
    }
    if !req.callback_url.is_empty() {
        if !valid_callback_url(&req.callback_url) {
            return bad("invalid callback_url");
        }
        if req.callback_secret.is_empty() {
            tracing::warn!(
                job_hint = %req.language,
                "callback_url set without callback_secret; webhook deliveries will be unsigned"
            );
        }
    }
    if req.callback_secret.len() > MAX_CALLBACK_SECRET_LEN {
        return bad("callback_secret exceeds 256 bytes");
    }
    if !valid_output(&req.output) {
        return bad("output must be one of: full, preview, none");
    }
    if !valid_priority(&req.priority) {
        return bad("priority must be one of: run, submit, bulk");
    }
    let Ok(cpu_ns) = scaled(req.time_limit_ms, 1_000_000, MIN_RUN_CPU, MAX_RUN_CPU) else {
        return bad("time_limit_ms must be between 100 and 20000");
    };
    let Ok(mem_bytes) = scaled(req.memory_limit_kb, 1024, MIN_RUN_MEM, MAX_RUN_MEM) else {
        return bad("memory_limit_kb must be between 16384 and 2097152");
    };
    let Ok(stack_bytes) = scaled(req.stack_limit_kb, 1024, MIN_RUN_STACK, MAX_RUN_STACK) else {
        return bad("stack_limit_kb must be between 1024 and 1048576");
    };
    let Ok(procs) = scaled(req.max_processes, 1, MIN_RUN_PROCS, MAX_RUN_PROCS) else {
        return bad("max_processes must be between 1 and 512");
    };
    if let Err(e) = ae_sandbox::validate_args(&req.args) {
        return bad(&format!("args: {e}"));
    }
    if !req.args.is_empty() && lang.placement.stdin_is_source {
        return bad(&format!("args are not supported for {}", req.language));
    }

    let stream = match req.priority.as_str() {
        "submit" if !st.queue.submit_stream().is_empty() => st.queue.submit_stream(),
        "bulk" if !st.queue.bulk_stream().is_empty() => st.queue.bulk_stream(),
        _ => st.queue.run_stream(),
    }
    .to_owned();
    // Cost in typical-job units: one per sandbox batch of tests.
    #[allow(clippy::cast_precision_loss)]
    let est_cost = req.tests.len().div_ceil(TESTS_PER_BATCH).max(1) as f64;
    if let Err(shed) = st.admission.check(&stream, est_cost).await {
        return shed.into_response();
    }

    let id = uuid::Uuid::now_v7().to_string();
    let mut sub = Submission {
        id: id.clone(),
        tenant_id: DEFAULT_TENANT_ID.to_owned(),
        user_id: None,
        idempotency_key: None,
        language: req.language.clone(),
        source_code: std::mem::take(&mut req.source_code),
        status: Status::Queued,
        verdict: None,
        compile_stderr: String::new(),
        cpu_time_ns: 0,
        memory_bytes: 0,
        test_count: 0,
        tests_passed: 0,
        created_at: 0,
        updated_at: 0,
    };
    if let Some(i) = &identity {
        sub.tenant_id = i.tenant_id.to_string();
        sub.user_id = Some(i.user_id.to_string());
    }
    if let Some(k) = req.idempotency_key.take().filter(|k| !k.is_empty()) {
        if k.len() > MAX_IDEMPOTENCY_KEY_LEN {
            return bad("idempotency_key exceeds 255 bytes");
        }
        sub.idempotency_key = Some(k);
    }

    let job_id = if sub.idempotency_key.is_some() && sub.user_id.is_some() {
        match st.db.insert_submission_idempotent(&sub).await {
            Ok((existing, false)) => {
                return json_response(
                    StatusCode::OK,
                    &ExecuteResponse {
                        job_id: existing,
                        stream_token: String::new(),
                    },
                );
            }
            Ok((new_id, true)) => new_id,
            Err(e) => {
                tracing::error!(error = %e, "idempotent insert failed");
                return json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
            }
        }
    } else {
        if let Err(e) = st.db.insert_submission(&sub).await {
            tracing::error!(error = %e, "insert submission failed");
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
        }
        id
    };

    let set = TestSet {
        tests: req
            .tests
            .into_iter()
            .zip(test_envs)
            .map(|(t, env)| TestData {
                input: t.input,
                expected_output: t.expected_output,
                env,
            })
            .collect(),
        checker_code: req.checker_code,
        checker_language: req.checker_language,
        interactor_code: req.interactor_code,
        interactor_language: req.interactor_language,
        files: req.files,
        compiler_flags: req.compiler_flags,
        callback_url: req.callback_url,
        callback_secret: req.callback_secret,
        output_mode: req.output,
    };
    let timeout_ms = i64::try_from(st.job_timeout.as_millis()).unwrap_or(i64::MAX);
    let job = Job {
        job_id: job_id.clone(),
        submission_id: job_id.clone(),
        language: sub.language,
        source: sub.source_code,
        limits: Limits {
            cpu_ns,
            mem_bytes,
            stack_bytes,
            procs,
            args: req.args,
        },
        checker: Checker::from_name(&req.checker),
        tests_key: format!("ac:tests:{job_id}"),
        expires_at: Some(unix_ms().saturating_add(timeout_ms)),
    };
    if let Err(e) = st.queue.prepare_and_enqueue(&job, &set, &stream).await {
        tracing::error!(submission_id = %job_id, error = %e, "enqueue failed");
        return json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
    }

    // Every new job gets a stream token: unauthenticated callers need it to stream,
    // read or cancel the job. A failure is not fatal; the job is already queued.
    let stream_token = match st.queue.issue_stream_token(&job_id).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(job_id = %job_id, error = %e, "failed to generate stream token");
            String::new()
        }
    };
    json_response(
        StatusCode::CREATED,
        &ExecuteResponse {
            job_id,
            stream_token,
        },
    )
}

/// Constant-time token check with a bound on the Redis round trip (fails closed).
pub(crate) async fn verify_token(q: &Queue, job_id: &str, token: &str) -> bool {
    tokio::time::timeout(TOKEN_TIMEOUT, q.verify_stream_token(job_id, token))
        .await
        .unwrap_or(false)
}

/// Same tenant and user for authenticated callers (404 otherwise, so ids do not leak);
/// the job's stream token for everyone else (401).
async fn check_ownership(
    st: &AppState,
    sub: &Submission,
    identity: Option<&Identity>,
    token: &str,
) -> Result<(), Box<Response>> {
    if let Some(i) = identity {
        let owner = sub.tenant_id == i.tenant_id.to_string()
            && sub.user_id.as_deref() == Some(i.user_id.to_string().as_str());
        return if owner {
            Ok(())
        } else {
            Err(Box::new(json_error(StatusCode::NOT_FOUND, "not found")))
        };
    }
    if verify_token(&st.queue, &sub.id, token).await {
        Ok(())
    } else {
        Err(Box::new(json_error(
            StatusCode::UNAUTHORIZED,
            "invalid or missing stream_token",
        )))
    }
}

/// Parses the path id, loads the submission and checks the caller may see it.
async fn owned_submission(
    st: &AppState,
    raw_id: &str,
    identity: Option<&Identity>,
    query: &HashMap<String, String>,
) -> Result<Submission, Box<Response>> {
    let Ok(id) = uuid::Uuid::parse_str(raw_id) else {
        return Err(Box::new(bad("invalid submission id")));
    };
    let Ok(Some(sub)) = st.db.get_submission(&id.to_string()).await else {
        return Err(Box::new(json_error(StatusCode::NOT_FOUND, "not found")));
    };
    let token = query.get("stream_token").map_or("", String::as_str);
    check_ownership(st, &sub, identity, token).await?;
    Ok(sub)
}

/// `DELETE /api/v1/submissions/{id}`: cancels a queued submission.
pub async fn cancel(
    State(st): State<AppState>,
    identity: Option<Extension<Identity>>,
    Path(raw_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let identity = identity.map(|Extension(i)| i);
    let sub = match owned_submission(&st, &raw_id, identity.as_ref(), &query).await {
        Ok(s) => s,
        Err(r) => return *r,
    };
    if !matches!(st.db.cancel_submission(&sub.id).await, Ok(true)) {
        return json_error(
            StatusCode::CONFLICT,
            "submission is not cancellable (already terminal or not found)",
        );
    }
    let ev = Event::new(
        &sub.id,
        EventType::Verdict,
        Some(serde_json::json!({ "verdict": "cancelled" })),
    );
    match tokio::time::timeout(PUBLISH_TIMEOUT, st.queue.publish(&ev)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(submission_id = %sub.id, error = %e,
            "failed to publish VERDICT event for cancelled submission"),
        Err(_) => tracing::warn!(submission_id = %sub.id,
            "failed to publish VERDICT event for cancelled submission: timeout"),
    }
    metrics::metrics()
        .jobs_processed
        .with_label_values(&["cancelled"])
        .inc();
    json_response(StatusCode::OK, &serde_json::json!({ "cancelled": true }))
}

#[derive(Serialize)]
struct SubmissionResponse {
    job_id: String,
    status: Status,
    #[serde(skip_serializing_if = "String::is_empty")]
    verdict: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    compile_stderr: String,
    language: String,
    cpu_time_ns: i64,
    memory_bytes: i64,
    test_count: i32,
    tests_passed: i32,
    created_at: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    results: Vec<TestResultOut>,
}

#[derive(Serialize)]
struct TestResultOut {
    test_index: i32,
    verdict: String,
    cpu_time_ns: i64,
    memory_bytes: i64,
    stdout_preview: String,
    stderr_preview: String,
}

/// Go's `time.Time` JSON form (RFC 3339 with trailing fractional zeros trimmed), in UTC.
fn rfc3339_ms(ms: i64) -> String {
    let Some(t) = chrono::DateTime::from_timestamp_millis(ms) else {
        return "0001-01-01T00:00:00Z".to_owned();
    };
    let s = t.format("%Y-%m-%dT%H:%M:%S%.3f").to_string();
    format!("{}Z", s.trim_end_matches('0').trim_end_matches('.'))
}

/// `GET /api/v1/submissions/{id}`.
pub async fn get_submission(
    State(st): State<AppState>,
    identity: Option<Extension<Identity>>,
    Path(raw_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let identity = identity.map(|Extension(i)| i);
    let sub = match owned_submission(&st, &raw_id, identity.as_ref(), &query).await {
        Ok(s) => s,
        Err(r) => return *r,
    };
    let results = st
        .db
        .get_test_results(&sub.id)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|r| TestResultOut {
            test_index: r.test_index,
            verdict: metrics::verdict_label(r.verdict),
            cpu_time_ns: r.cpu_time_ns,
            memory_bytes: r.memory_bytes,
            stdout_preview: r.stdout_preview,
            stderr_preview: r.stderr_preview,
        })
        .collect();
    json_response(
        StatusCode::OK,
        &SubmissionResponse {
            job_id: sub.id,
            status: sub.status,
            verdict: sub.verdict.map(metrics::verdict_label).unwrap_or_default(),
            compile_stderr: sub.compile_stderr,
            language: sub.language,
            cpu_time_ns: sub.cpu_time_ns,
            memory_bytes: sub.memory_bytes,
            test_count: sub.test_count,
            tests_passed: sub.tests_passed,
            created_at: rfc3339_ms(sub.created_at),
            results,
        },
    )
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    /// Kept for contract compatibility with the Go engine: the runtime version.
    go_version: &'static str,
    uptime_seconds: u64,
    checks: BTreeMap<&'static str, &'static str>,
    #[serde(skip_serializing_if = "String::is_empty")]
    sandbox_version: String,
    workers: Workers,
    queues: BTreeMap<String, u64>,
}

#[derive(Serialize)]
struct Workers {
    active: i64,
    configured: usize,
}

fn check<T, E: std::fmt::Display>(
    name: &str,
    r: Result<Result<T, E>, tokio::time::error::Elapsed>,
) -> Option<T> {
    match r {
        Ok(Ok(v)) => Some(v),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "health: {name} check failed");
            None
        }
        Err(_) => {
            tracing::error!("health: {name} check timed out");
            None
        }
    }
}

/// `GET /api/v1/health`: database, sandbox and Redis within 5 s. Database down is
/// `unhealthy` (503); sandbox or Redis down is `degraded` (200).
pub async fn health(State(st): State<AppState>) -> Response {
    let deadline = tokio::time::Instant::now() + HEALTH_TIMEOUT;
    let db = async {
        tokio::time::timeout_at(deadline, sqlx::query("SELECT 1").execute(st.db.pool())).await
    };
    let sandbox = async { tokio::time::timeout_at(deadline, st.sandbox.version()).await };
    let redis = async { tokio::time::timeout_at(deadline, st.queue.ping()).await };
    let (db, sandbox, redis) = tokio::join!(db, sandbox, redis);
    let db_ok = check("database", db).is_some();
    let sandbox_version = check("sandbox", sandbox);
    let redis_ok = check("redis", redis).is_some();
    let ok = |b: bool| if b { "ok" } else { "error" };
    let checks = BTreeMap::from([
        ("database", ok(db_ok)),
        ("sandbox", ok(sandbox_version.is_some())),
        ("redis", ok(redis_ok)),
    ]);
    let mut queues = BTreeMap::new();
    for s in st.queue.streams() {
        if let Ok(Ok(n)) = tokio::time::timeout_at(deadline, st.queue.depth(s)).await {
            queues.insert(s.to_owned(), n);
        }
    }
    let (status, code) = if !db_ok {
        ("unhealthy", StatusCode::SERVICE_UNAVAILABLE)
    } else if sandbox_version.is_none() || !redis_ok {
        ("degraded", StatusCode::OK)
    } else {
        ("healthy", StatusCode::OK)
    };
    #[allow(clippy::cast_possible_truncation)]
    let active = metrics::metrics().active_workers.get() as i64;
    json_response(
        code,
        &HealthResponse {
            status,
            version: env!("CARGO_PKG_VERSION"),
            go_version: concat!("rust", env!("CARGO_PKG_RUST_VERSION")),
            uptime_seconds: st.started.elapsed().as_secs(),
            checks,
            sandbox_version: sandbox_version.unwrap_or_default(),
            workers: Workers {
                active,
                configured: st.worker_count,
            },
            queues,
        },
    )
}

#[derive(Serialize)]
struct LanguageInfo {
    id: String,
    compiled: bool,
    default_cpu_limit_ms: u64,
    default_memory_limit_kb: u64,
    run_proc_limit: u64,
    /// False when the sandbox lacks the language; true until the first check answers.
    available: bool,
}

/// `GET /api/v1/languages`, sorted by id.
pub async fn languages(State(st): State<AppState>) -> Response {
    let list: Vec<LanguageInfo> = st
        .registry
        .languages()
        .iter()
        .map(|(id, l)| LanguageInfo {
            id: id.to_owned(),
            compiled: l.is_compiled(),
            default_cpu_limit_ms: l.limits().cpu / 1_000_000,
            default_memory_limit_kb: l.limits().memory / 1024,
            run_proc_limit: l.limits().procs,
            available: st.registry.get(id).is_some(),
        })
        .collect();
    json_response(StatusCode::OK, &list)
}

/// `GET /readyz`: 200 once [`AppState::set_ready`] is true, 503 before and while draining.
pub async fn readyz(State(st): State<AppState>) -> Response {
    if st.ready.load(Ordering::SeqCst) {
        (StatusCode::OK, "ok\n").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n").into_response()
    }
}
