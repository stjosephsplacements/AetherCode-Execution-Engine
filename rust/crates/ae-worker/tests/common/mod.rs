#![allow(dead_code)]
//! Test doubles: a scriptable sandbox, an in-memory store and bus that record the
//! order of side effects. Test code only.

use ae_core::Config;
use ae_core::eventlog::{Event, EventType};
use ae_core::model::{Checker, Limits, Status, Submission, TestResult, Verdict};
use ae_queue::{Delivery, JobSpec, TestData, TestSet};
use ae_sandbox::{
    Cmd, Error as SbError, FileId, PipeMap, Registry, RunResult, STATUS_ACCEPTED,
    STATUS_NONZERO_EXIT, Sandbox,
};
use ae_worker::{Bus, Cost, Error, Result, Store, WebhookClient, Worker, WorkerOptions};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};

pub type Log = Arc<Mutex<Vec<String>>>;

pub type Hook = Arc<dyn Fn(&Cmd) -> Option<RunResult> + Send + Sync>;

#[derive(Default)]
pub struct FakeSandbox {
    /// Every test-run request (compiles and probes excluded), in arrival order.
    pub runs: Mutex<Vec<Vec<Cmd>>>,
    pub piped: Mutex<Vec<(Vec<Cmd>, Vec<PipeMap>)>>,
    pub compiles: AtomicUsize,
    pub cached: Mutex<Vec<(String, String)>>,
    pub deleted: Mutex<Vec<FileId>>,
    pub in_flight: AtomicUsize,
    pub max_in_flight: AtomicUsize,
    pub run_delay: Mutex<Duration>,
    /// Held for writing by a test to block every compile and run.
    pub gate: RwLock<()>,
    /// Compile result status and stderr; accepted by default.
    pub compile_fail: Mutex<Option<String>>,
    pub hook: Mutex<Option<Hook>>,
    pub panic_on_run: AtomicBool,
    pub piped_fail: AtomicBool,
    pub missing: HashSet<String>,
    next_id: AtomicUsize,
}

fn ok() -> RunResult {
    RunResult {
        status: STATUS_ACCEPTED.into(),
        ..RunResult::default()
    }
}

fn is_version_probe(c: &Cmd) -> bool {
    c.args
        .iter()
        .any(|a| a == "--version" || a == "-version" || a == "version")
}

impl FakeSandbox {
    pub fn with_missing(missing: &[&str]) -> Self {
        Self {
            missing: missing.iter().map(|s| (*s).to_owned()).collect(),
            ..Self::default()
        }
    }

    pub fn set_hook(&self, f: impl Fn(&Cmd) -> Option<RunResult> + Send + Sync + 'static) {
        *self.hook.lock().unwrap() = Some(Arc::new(f));
    }

    pub fn reset(&self) {
        self.runs.lock().unwrap().clear();
        self.piped.lock().unwrap().clear();
        self.cached.lock().unwrap().clear();
        self.deleted.lock().unwrap().clear();
        self.compiles.store(0, Ordering::SeqCst);
        self.max_in_flight.store(0, Ordering::SeqCst);
    }

    pub fn runs(&self) -> Vec<Vec<Cmd>> {
        self.runs.lock().unwrap().clone()
    }

    fn answer(&self, c: &Cmd) -> RunResult {
        if let Some(h) = self.hook.lock().unwrap().clone()
            && let Some(r) = h(c)
        {
            return r;
        }
        let stdin = c.files.first().and_then(|f| f.content.clone());
        let mut r = ok();
        let out = match stdin.as_deref() {
            Some("") | None => "hello".to_owned(),
            Some(s) => s.to_owned(),
        };
        r.files.insert("stdout".into(), out);
        r.time = 1_000;
        r.memory = 2_048;
        r
    }
}

#[async_trait]
impl Sandbox for FakeSandbox {
    async fn run(&self, cmds: Vec<Cmd>) -> std::result::Result<Vec<RunResult>, SbError> {
        let first = &cmds[0];
        if first.args[0] == "/usr/bin/test" {
            return Ok(cmds
                .iter()
                .map(|c| {
                    if self.missing.contains(&c.args[2]) {
                        RunResult {
                            status: STATUS_NONZERO_EXIT.into(),
                            ..RunResult::default()
                        }
                    } else {
                        ok()
                    }
                })
                .collect());
        }
        if is_version_probe(first) {
            let mut r = ok();
            r.files.insert("stdout".into(), "tool 1.0\n".into());
            return Ok(vec![r]);
        }
        let _g = self.gate.read().await;
        if let Some(bin) = first.copy_out_cached.first() {
            self.compiles.fetch_add(1, Ordering::SeqCst);
            let fail = self.compile_fail.lock().unwrap().clone();
            let mut r = ok();
            match fail {
                Some(stderr) => {
                    r.status = STATUS_NONZERO_EXIT.into();
                    r.exit_status = 1;
                    r.files.insert("stderr".into(), stderr);
                }
                None => {
                    let n = self.next_id.fetch_add(1, Ordering::SeqCst);
                    r.file_ids.insert(bin.clone(), format!("bin-{n}"));
                }
            }
            return Ok(vec![r]);
        }
        // Registry self-test runs are cleared by `reset` after the registry loads.
        if self.panic_on_run.swap(false, Ordering::SeqCst) {
            panic!("sandbox exploded");
        }
        self.runs.lock().unwrap().push(cmds.clone());
        let now = self.in_flight.fetch_add(cmds.len(), Ordering::SeqCst) + cmds.len();
        self.max_in_flight.fetch_max(now, Ordering::SeqCst);
        let delay = *self.run_delay.lock().unwrap();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        self.in_flight.fetch_sub(cmds.len(), Ordering::SeqCst);
        Ok(cmds.iter().map(|c| self.answer(c)).collect())
    }

    async fn run_piped(
        &self,
        cmds: Vec<Cmd>,
        pipes: Vec<PipeMap>,
    ) -> std::result::Result<Vec<RunResult>, SbError> {
        self.piped.lock().unwrap().push((cmds.clone(), pipes));
        if self.piped_fail.load(Ordering::SeqCst) {
            return Err(SbError::Status {
                status: 502,
                body: "down".into(),
            });
        }
        Ok(cmds.iter().map(|c| self.answer(c)).collect())
    }

    async fn cache_file(&self, name: &str, bytes: &[u8]) -> std::result::Result<FileId, SbError> {
        self.cached
            .lock()
            .unwrap()
            .push((name.to_owned(), String::from_utf8_lossy(bytes).into_owned()));
        let n = self.next_id.fetch_add(1, Ordering::SeqCst);
        Ok(FileId(format!("src-{n}")))
    }

    async fn delete_file(&self, id: FileId) -> std::result::Result<(), SbError> {
        self.deleted.lock().unwrap().push(id);
        Ok(())
    }

    async fn version(&self) -> std::result::Result<String, SbError> {
        Ok("fake".into())
    }
}

pub fn config() -> Config {
    Config::from_map(&HashMap::new()).unwrap()
}

pub async fn registry(sb: &FakeSandbox) -> Arc<Registry> {
    let reg = Registry::load_with_budget(&config(), sb, Duration::from_secs(5))
        .await
        .unwrap();
    sb.reset();
    Arc::new(reg)
}

#[derive(Default, Clone)]
pub struct Row {
    pub language: String,
    pub source: String,
    pub status: Option<Status>,
    pub verdict: Option<Verdict>,
    pub stderr: String,
    pub results: Vec<TestResult>,
    pub webhook_marked: bool,
}

#[derive(Default)]
pub struct FakeStore {
    pub rows: Mutex<HashMap<String, Row>>,
    pub log: Log,
    pub finishes: AtomicUsize,
}

impl FakeStore {
    pub fn insert(&self, id: &str, language: &str, source: &str) {
        self.rows.lock().unwrap().insert(
            id.to_owned(),
            Row {
                language: language.into(),
                source: source.into(),
                status: Some(Status::Queued),
                ..Row::default()
            },
        );
    }
    pub fn row(&self, id: &str) -> Row {
        self.rows.lock().unwrap()[id].clone()
    }
    fn terminal(s: Option<Status>) -> bool {
        matches!(s, Some(Status::Completed | Status::Failed))
    }
}

#[async_trait]
impl Store for FakeStore {
    async fn get_submission(&self, id: &str) -> Result<Option<Submission>> {
        Ok(self.rows.lock().unwrap().get(id).map(|r| Submission {
            id: id.to_owned(),
            tenant_id: ae_core::model::DEFAULT_TENANT_ID.into(),
            user_id: None,
            idempotency_key: None,
            language: r.language.clone(),
            source_code: r.source.clone(),
            status: r.status.unwrap_or(Status::Queued),
            verdict: r.verdict,
            compile_stderr: r.stderr.clone(),
            cpu_time_ns: 0,
            memory_bytes: 0,
            test_count: 0,
            tests_passed: 0,
            created_at: 0,
            updated_at: 0,
        }))
    }
    async fn set_status(&self, id: &str, status: Status) -> Result<bool> {
        let mut rows = self.rows.lock().unwrap();
        let r = rows
            .get_mut(id)
            .ok_or_else(|| Error::Other("missing".into()))?;
        if Self::terminal(r.status) {
            return Ok(false);
        }
        r.status = Some(status);
        Ok(true)
    }
    async fn touch(&self, _id: &str) -> Result<()> {
        Ok(())
    }
    async fn finish_with_results(
        &self,
        id: &str,
        verdict: Verdict,
        results: &[TestResult],
    ) -> Result<bool> {
        self.log.lock().unwrap().push("db:finish".into());
        let mut rows = self.rows.lock().unwrap();
        let r = rows
            .get_mut(id)
            .ok_or_else(|| Error::Other("missing".into()))?;
        if Self::terminal(r.status) {
            return Ok(false);
        }
        self.finishes.fetch_add(1, Ordering::SeqCst);
        r.status = Some(Status::Completed);
        r.verdict = Some(verdict);
        r.results = results.to_vec();
        Ok(true)
    }
    async fn fail_submission(&self, id: &str, verdict: Verdict, stderr: &str) -> Result<bool> {
        self.log.lock().unwrap().push("db:fail".into());
        let mut rows = self.rows.lock().unwrap();
        let r = rows
            .get_mut(id)
            .ok_or_else(|| Error::Other("missing".into()))?;
        if Self::terminal(r.status) {
            return Ok(false);
        }
        r.status = Some(Status::Failed);
        r.verdict = Some(verdict);
        r.stderr = stderr.to_owned();
        Ok(true)
    }
    async fn mark_webhook_delivered(&self, id: &str) -> Result<bool> {
        let mut rows = self.rows.lock().unwrap();
        let r = rows
            .get_mut(id)
            .ok_or_else(|| Error::Other("missing".into()))?;
        Ok(!std::mem::replace(&mut r.webhook_marked, true))
    }
}

#[derive(Default)]
pub struct FakeBus {
    pub specs: Mutex<HashMap<String, JobSpec>>,
    pub events: Mutex<Vec<Event>>,
    pub acks: Mutex<Vec<Delivery>>,
    pub fail_next_ack: AtomicBool,
    pub log: Log,
    pub feed: tokio::sync::Mutex<Option<mpsc::Receiver<Delivery>>>,
}

impl FakeBus {
    pub fn events_of(&self, id: &str, kind: EventType) -> Vec<Event> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.job_id == id && e.kind == kind)
            .cloned()
            .collect()
    }
    pub fn acked(&self, id: &str) -> usize {
        self.acks
            .lock()
            .unwrap()
            .iter()
            .filter(|d| d.job_id == id)
            .count()
    }
}

#[async_trait]
impl Bus for FakeBus {
    async fn get_tests(&self, job_id: &str) -> Result<JobSpec> {
        self.specs
            .lock()
            .unwrap()
            .get(job_id)
            .cloned()
            .ok_or_else(|| Error::Other("no test data".into()))
    }
    async fn publish(&self, ev: &Event) -> Result<()> {
        self.log
            .lock()
            .unwrap()
            .push(format!("event:{}", ev.kind.as_str()));
        self.events.lock().unwrap().push(ev.clone());
        Ok(())
    }
    async fn ack(&self, d: &Delivery) -> Result<()> {
        if self.fail_next_ack.swap(false, Ordering::SeqCst) {
            return Err(Error::Other("crash before ack".into()));
        }
        self.log.lock().unwrap().push("ack".into());
        self.acks.lock().unwrap().push(d.clone());
        Ok(())
    }
    async fn consume(&self, tx: mpsc::Sender<Delivery>) {
        let Some(mut rx) = self.feed.lock().await.take() else {
            return;
        };
        while let Some(d) = rx.recv().await {
            if tx.send(d).await.is_err() {
                return;
            }
        }
    }
}

pub struct Harness {
    pub sb: Arc<FakeSandbox>,
    pub store: Arc<FakeStore>,
    pub bus: Arc<FakeBus>,
    pub log: Log,
    pub registry: Arc<Registry>,
    pub feed: mpsc::Sender<Delivery>,
}

impl Harness {
    pub async fn new() -> Self {
        Self::with_sandbox(FakeSandbox::default()).await
    }

    pub async fn with_sandbox(sb: FakeSandbox) -> Self {
        let sb = Arc::new(sb);
        let registry = registry(&sb).await;
        let log: Log = Arc::default();
        let store = Arc::new(FakeStore {
            log: log.clone(),
            ..FakeStore::default()
        });
        let (feed, rx) = mpsc::channel(64);
        let bus = Arc::new(FakeBus {
            log: log.clone(),
            feed: tokio::sync::Mutex::new(Some(rx)),
            ..FakeBus::default()
        });
        Self {
            sb,
            store,
            bus,
            log,
            registry,
            feed,
        }
    }

    pub fn options(&self) -> WorkerOptions {
        WorkerOptions {
            job_timeout: Duration::from_secs(30),
            max_inflight: 4,
            shutdown_grace: Duration::from_secs(120),
            budget: Cost {
                cpu_millis: 4_000,
                mem_bytes: u64::MAX,
            },
            cache_ttl: Duration::from_secs(300),
            cache_max_bytes: 1 << 30,
            webhook: WebhookClient::allowing_private_addresses(),
        }
    }

    pub fn worker(&self) -> Worker {
        self.worker_with(self.options())
    }

    pub fn worker_with(&self, opts: WorkerOptions) -> Worker {
        Worker::new(
            self.store.clone(),
            self.bus.clone(),
            self.sb.clone(),
            self.registry.clone(),
            opts,
        )
    }

    /// Registers a submission and its tests (input, expected) under `id`.
    pub fn job(&self, id: &str, lang: &str, source: &str, tests: &[(&str, &str)]) {
        self.job_with(id, lang, source, tests, |_| {});
    }

    pub fn job_with(
        &self,
        id: &str,
        lang: &str,
        source: &str,
        tests: &[(&str, &str)],
        f: impl FnOnce(&mut JobSpec),
    ) {
        self.store.insert(id, lang, source);
        let mut spec = JobSpec {
            limits: Limits::default(),
            checker: Checker::Exact,
            set: TestSet {
                tests: tests
                    .iter()
                    .map(|(i, e)| TestData {
                        input: (*i).to_owned(),
                        expected_output: (*e).to_owned(),
                        env: Vec::new(),
                    })
                    .collect(),
                ..TestSet::default()
            },
        };
        f(&mut spec);
        self.bus.specs.lock().unwrap().insert(id.to_owned(), spec);
    }

    pub fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }
}

pub fn delivery(id: &str) -> Delivery {
    Delivery {
        stream: "ac:submissions:run".into(),
        msg_id: format!("{id}-0"),
        job_id: id.to_owned(),
        expires_at: None,
    }
}

pub fn id(n: u32) -> String {
    format!("00000000-0000-4000-8000-{n:012}")
}
