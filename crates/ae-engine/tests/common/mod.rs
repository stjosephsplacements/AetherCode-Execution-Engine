//! Harness for the engine binary: Postgres and Redis containers, a fake go-judge HTTP
//! server, and the real `engine` process with its stderr captured line by line.
#![allow(dead_code)]

use axum::Router;
use axum::extract::State;
use axum::routing::{delete, get, post};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::Redis;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::RwLock;

/// Prewarm runs are this exact command (see `ae-engine` `startup::prewarm`).
pub const PREWARM_ARGS: [&str; 3] = ["/usr/bin/test", "-x", "/usr/bin/test"];

pub const LANGUAGES: &str = r#"
[languages.python]
compiler = ["/usr/bin/python3", "--version"]
placement = { source = "/w/main.py" }
hello = 'print("hello")'
[languages.python.execute_cmd]
args = ["/usr/bin/python3", "/w/main.py"]
env = ["PATH=/usr/bin:/bin"]
limits = { cpu = 2_000_000_000, memory = 536_870_912, procs = 50 }

[languages.ghost]
compiler = ["/usr/bin/ghost", "--version"]
placement = { source = "/w/main.gh" }
hello = 'hello'
[languages.ghost.execute_cmd]
args = ["/usr/bin/ghost", "/w/main.gh"]
env = ["PATH=/usr/bin:/bin"]
limits = { cpu = 2_000_000_000, memory = 536_870_912, procs = 50 }
"#;

/// Polls `f` every 50 ms until it returns true or `limit` passes.
pub async fn eventually<F, Fut>(limit: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        if f().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub struct Infra {
    pub _pg: ContainerAsync<Postgres>,
    pub _redis: ContainerAsync<Redis>,
    pub db_url: String,
    pub redis_addr: String,
}

pub async fn infra() -> Infra {
    let pg = async {
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
    };
    let redis = async {
        let c = Redis::default()
            .with_tag("7")
            .start()
            .await
            .expect("start redis:7");
        let port = c.get_host_port_ipv4(6379).await.unwrap();
        (c, format!("127.0.0.1:{port}"))
    };
    let ((pg, db_url), (rc, redis_addr)) = tokio::join!(pg, redis);
    Infra {
        _pg: pg,
        _redis: rc,
        db_url,
        redis_addr,
    }
}

impl Infra {
    pub fn config(&self) -> ae_core::Config {
        ae_core::Config::from_map(&HashMap::from([
            ("AC_DATABASE_URL".to_owned(), self.db_url.clone()),
            ("AC_REDIS_ADDR".to_owned(), self.redis_addr.clone()),
        ]))
        .unwrap()
    }

    pub async fn redis(&self) -> redis::aio::ConnectionManager {
        redis::Client::open(format!("redis://{}/", self.redis_addr))
            .unwrap()
            .get_connection_manager()
            .await
            .unwrap()
    }
}

/// go-judge stand-in. Every program prints "hello". Prewarm runs wait on `prewarm_gate`
/// and test runs whose stdin contains `SLOW` wait on `slow_gate` (hold the write lock to stall).
#[derive(Clone, Default)]
pub struct FakeJudge {
    pub url: String,
    pub missing: Arc<HashSet<String>>,
    pub prewarm_gate: Arc<RwLock<()>>,
    pub prewarms: Arc<AtomicUsize>,
    pub slow_gate: Arc<RwLock<()>>,
    pub slow_started: Arc<AtomicUsize>,
    pub runs: Arc<AtomicUsize>,
}

fn accepted() -> Value {
    json!({"status": "Accepted", "exitStatus": 0, "time": 1000, "memory": 4096, "runTime": 1000, "files": {}, "fileIds": {}})
}

async fn run(State(j): State<FakeJudge>, body: axum::Json<Value>) -> axum::Json<Value> {
    let cmds = body["cmd"].as_array().cloned().unwrap_or_default();
    let mut out = Vec::new();
    for c in cmds {
        let args: Vec<&str> = c["args"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a.as_str().unwrap())
            .collect();
        let mut r = accepted();
        if args == PREWARM_ARGS {
            j.prewarms.fetch_add(1, Ordering::SeqCst);
            let _g = j.prewarm_gate.read().await;
        } else if args[0] == "/usr/bin/test" {
            if j.missing.contains(args[2]) {
                r["status"] = "Nonzero Exit Status".into();
                r["exitStatus"] = 1.into();
            }
        } else {
            j.runs.fetch_add(1, Ordering::SeqCst);
            let stdin = c["files"][0]["content"].as_str().unwrap_or_default();
            if stdin.contains("SLOW") {
                j.slow_started.fetch_add(1, Ordering::SeqCst);
                let _g = j.slow_gate.read().await;
            }
            r["files"] = json!({"stdout": "hello\n", "stderr": ""});
        }
        out.push(r);
    }
    axum::Json(Value::Array(out))
}

impl FakeJudge {
    pub async fn start(missing: &[&str]) -> FakeJudge {
        let mut j = FakeJudge {
            missing: Arc::new(missing.iter().map(|s| (*s).to_owned()).collect()),
            ..FakeJudge::default()
        };
        let app = Router::new()
            .route(
                "/version",
                get(|| async { axum::Json(json!({"buildVersion": "fake-1.0"})) }),
            )
            .route("/run", post(run))
            .route("/file", post(|| async { axum::Json(json!("file1")) }))
            .route("/file/{id}", delete(|| async { axum::Json(json!({})) }))
            .with_state(j.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        j.url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        j
    }
}

pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

pub struct Engine {
    pub child: Child,
    pub logs: Arc<Mutex<Vec<String>>>,
    pub url: String,
    pub debug_port: u16,
    _languages: tempfile_path::TempFile,
}

mod tempfile_path {
    /// A file removed on drop.
    pub struct TempFile(pub std::path::PathBuf);
    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
}

pub struct EngineOpts<'a> {
    pub args: &'a [&'a str],
    pub judge_url: &'a str,
    pub env: &'a [(&'a str, &'a str)],
}

pub fn spawn_engine(infra: &Infra, o: &EngineOpts<'_>) -> Engine {
    let port = free_port();
    let debug_port = free_port();
    let lang_path =
        std::env::temp_dir().join(format!("ae-engine-langs-{}.toml", uuid::Uuid::new_v4()));
    std::fs::write(&lang_path, LANGUAGES).unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_engine"));
    cmd.args(o.args)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("AC_DATABASE_URL", &infra.db_url)
        .env("AC_REDIS_ADDR", &infra.redis_addr)
        .env("AC_JUDGE_URL", o.judge_url)
        .env("AC_GATEWAY_ADDR", format!("127.0.0.1:{port}"))
        .env("AC_DEBUG_PORT", debug_port.to_string())
        .env("AC_LANGUAGES_FILE", &lang_path)
        .env("AC_AUTH_ENABLED", "false")
        .env("AC_ALLOW_UNAUTH", "true")
        .env("AC_BLOCK_READ_TIMEOUT", "100ms")
        .env("AC_DB_MAX_CONNS", "8")
        .env("AC_WORKERS", "1")
        .env("AC_PREWARM", "2")
        .env("AC_LOG_FORMAT", "json")
        .env("AC_LOG_LEVEL", "info");
    for (k, v) in o.env {
        cmd.env(k, v);
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = cmd.spawn().expect("spawn engine");
    let logs = Arc::new(Mutex::new(Vec::new()));
    let stderr = child.stderr.take().unwrap();
    let sink = logs.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(l)) = lines.next_line().await {
            eprintln!("engine: {l}");
            sink.lock().unwrap().push(l);
        }
    });
    Engine {
        child,
        logs,
        url: format!("http://127.0.0.1:{port}"),
        debug_port,
        _languages: tempfile_path::TempFile(lang_path),
    }
}

impl Engine {
    pub fn has_log(&self, needle: &str) -> bool {
        self.logs.lock().unwrap().iter().any(|l| l.contains(needle))
    }

    pub async fn wait_log(&self, needle: &str, limit: Duration) -> bool {
        eventually(limit, || async { self.has_log(needle) }).await
    }

    pub async fn wait_exit(&mut self, limit: Duration) -> Option<ExitStatus> {
        tokio::time::timeout(limit, self.child.wait())
            .await
            .ok()
            .map(Result::unwrap)
    }

    pub fn signal(&self, sig: &str) {
        let pid = self.child.id().expect("engine still running");
        let ok = std::process::Command::new("kill")
            .arg(format!("-{sig}"))
            .arg(pid.to_string())
            .status()
            .unwrap()
            .success();
        assert!(ok, "kill -{sig} {pid}");
    }

    /// HTTP status of `GET <gateway><path>`, or `None` when the server is not answering.
    pub async fn status(&self, path: &str) -> Option<u16> {
        reqwest::Client::new()
            .get(format!("{}{path}", self.url))
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .ok()
            .map(|r| r.status().as_u16())
    }

    pub async fn wait_ready(&self) {
        assert!(
            eventually(Duration::from_secs(60), || async {
                self.status("/readyz").await == Some(200)
            })
            .await,
            "engine never became ready"
        );
    }
}

/// One python test whose stdin is `input`; the fake judge prints "hello".
pub fn run_body(input: &str) -> String {
    json!({
        "language": "python",
        "source_code": "print('hello')",
        "tests": [{"input": input, "expected_output": "hello"}],
    })
    .to_string()
}

pub async fn execute(url: &str, body: &str) -> Value {
    let r = reqwest::Client::new()
        .post(format!("{url}/api/v1/execute"))
        .header("content-type", "application/json")
        .header(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )
        .body(body.to_owned())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201, "execute");
    r.json().await.unwrap()
}
