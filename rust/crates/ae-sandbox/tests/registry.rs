//! Registry self-test, startup retry and refresh against a fake Sandbox.

use ae_core::Config;
use ae_sandbox::{
    Cmd, Error, FileId, LanguageStatus, Registry, RunResult, STATUS_ACCEPTED, STATUS_NONZERO_EXIT,
    Sandbox,
};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Behaves like a go-judge with every toolchain installed, except that runs whose
/// argv starts with one of `broken` print the wrong text, binaries in `missing`
/// are absent, `version()` fails `down_versions` times, every `run` errors while
/// `run_down` is set, and version probes error while `version_probe_down` is set.
#[derive(Default)]
struct Fake {
    broken: Mutex<HashSet<String>>,
    missing: HashSet<String>,
    deleted: Mutex<Vec<FileId>>,
    down_versions: AtomicUsize,
    version_calls: AtomicUsize,
    run_down: AtomicBool,
    version_probe_down: bool,
}

fn ok() -> RunResult {
    RunResult {
        status: STATUS_ACCEPTED.into(),
        ..RunResult::default()
    }
}

fn down() -> Error {
    Error::Status {
        status: 502,
        body: "down".into(),
    }
}

fn is_version_probe(c: &Cmd) -> bool {
    c.args
        .iter()
        .any(|a| a == "--version" || a == "-version" || a == "version")
}

#[async_trait]
impl Sandbox for Fake {
    async fn run(&self, cmds: Vec<Cmd>) -> Result<Vec<RunResult>, Error> {
        if self.run_down.load(Ordering::SeqCst) {
            return Err(down());
        }
        if self.version_probe_down && cmds.iter().any(is_version_probe) {
            return Err(down());
        }
        let broken = self.broken.lock().unwrap().clone();
        Ok(cmds
            .into_iter()
            .map(|c| {
                let a0 = c.args[0].as_str();
                if a0 == "/usr/bin/test" {
                    if self.missing.contains(&c.args[2]) {
                        return RunResult {
                            status: STATUS_NONZERO_EXIT.into(),
                            ..RunResult::default()
                        };
                    }
                    return ok();
                }
                if is_version_probe(&c) {
                    let mut r = ok();
                    r.files
                        .insert("stdout".into(), format!("{a0} 1.2.3\nextra\n"));
                    return r;
                }
                if let Some(bin) = c.copy_out_cached.first() {
                    let mut r = ok();
                    r.file_ids.insert(bin.clone(), format!("id-{}", a0.len()));
                    return r;
                }
                let mut r = ok();
                let out = if broken.contains(a0) {
                    "nope\n"
                } else {
                    "hello\n"
                };
                r.files.insert("stdout".into(), out.into());
                r
            })
            .collect())
    }
    async fn cache_file(&self, _: &str, _: &[u8]) -> Result<FileId, Error> {
        Ok(FileId("x".into()))
    }
    async fn delete_file(&self, id: FileId) -> Result<(), Error> {
        self.deleted.lock().unwrap().push(id);
        Ok(())
    }
    async fn version(&self) -> Result<String, Error> {
        self.version_calls.fetch_add(1, Ordering::SeqCst);
        let left = self.down_versions.load(Ordering::SeqCst);
        if left > 0 {
            self.down_versions.store(left - 1, Ordering::SeqCst);
            return Err(down());
        }
        Ok("fake".into())
    }
}

fn cfg() -> Config {
    Config::from_map(&HashMap::new()).unwrap()
}

fn available(reg: &Registry, id: &str) -> bool {
    reg.catalogue()
        .iter()
        .find(|s| s.id == id)
        .unwrap()
        .available
}

#[tokio::test]
async fn all_languages_available_when_self_test_passes() {
    let fake = Fake::default();
    let reg = Registry::load(&cfg(), &fake).await.unwrap();
    let cat = reg.catalogue();
    assert_eq!(cat.len(), 14);
    assert!(cat.iter().all(|s| s.available), "{cat:?}");
    assert!(reg.get("rust").is_some());
    assert_eq!(
        reg.toolchain_version("rust").as_deref(),
        Some("/usr/bin/rustc 1.2.3")
    );
    assert_eq!(reg.toolchain_version("nope"), None);
    // Every compiled language's cached binary is released after its self-test.
    assert_eq!(fake.deleted.lock().unwrap().len(), 7);
}

#[tokio::test]
async fn failed_self_test_marks_language_unavailable() {
    let fake = Fake {
        broken: Mutex::new(["/usr/bin/python3".to_owned()].into()),
        missing: ["/usr/bin/php".to_owned()].into(),
        ..Fake::default()
    };
    let reg = Registry::load(&cfg(), &fake).await.unwrap();
    assert!(!available(&reg, "python"));
    assert!(!available(&reg, "php"));
    assert!(available(&reg, "c") && available(&reg, "java") && available(&reg, "javascript"));
    assert!(reg.get("python").is_none());
    assert!(reg.get("php").is_none());
    assert!(reg.get("c").is_some());
    assert!(reg.catalogue().contains(&LanguageStatus {
        id: "python".into(),
        available: false
    }));
}

#[tokio::test]
async fn version_probe_failure_keeps_language_available() {
    let fake = Fake {
        version_probe_down: true,
        ..Fake::default()
    };
    let reg = Registry::load(&cfg(), &fake).await.unwrap();
    assert!(reg.catalogue().iter().all(|s| s.available));
    assert_eq!(reg.toolchain_version("rust").as_deref(), Some("unknown"));
}

#[tokio::test]
async fn loads_after_sandbox_comes_up() {
    let fake = Fake {
        down_versions: AtomicUsize::new(3),
        ..Fake::default()
    };
    let reg = Registry::load_with_budget(&cfg(), &fake, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(fake.version_calls.load(Ordering::SeqCst), 4);
    assert!(reg.catalogue().iter().all(|s| s.available));
}

#[tokio::test]
async fn permanently_unreachable_sandbox_is_an_error() {
    let fake = Fake {
        down_versions: AtomicUsize::new(usize::MAX),
        ..Fake::default()
    };
    let started = std::time::Instant::now();
    let err = Registry::load_with_budget(&cfg(), &fake, Duration::from_millis(300))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::SandboxUnreachable), "{err}");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(fake.version_calls.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn refresh_keeps_last_answer_on_probe_error_and_flips_on_real_failure() {
    let fake = Arc::new(Fake::default());
    let reg = Registry::load(&cfg(), fake.as_ref()).await.unwrap();
    assert!(available(&reg, "python"));

    // Sandbox errors: a failed probe is not a missing toolchain.
    fake.run_down.store(true, Ordering::SeqCst);
    reg.refresh(fake.as_ref()).await;
    assert!(reg.catalogue().iter().all(|s| s.available));
    assert!(reg.get("python").is_some());

    // The self-test genuinely fails: python flips to unavailable, then back.
    fake.run_down.store(false, Ordering::SeqCst);
    fake.broken
        .lock()
        .unwrap()
        .insert("/usr/bin/python3".to_owned());
    reg.refresh(fake.as_ref()).await;
    assert!(!available(&reg, "python"));
    assert!(available(&reg, "c"));

    fake.broken.lock().unwrap().clear();
    let reg = Arc::new(reg);
    let sb: Arc<dyn Sandbox> = fake.clone();
    let handle = reg.clone().spawn_refresher(sb, Duration::from_millis(20));
    for _ in 0..100 {
        if available(&reg, "python") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    handle.abort();
    assert!(available(&reg, "python"));
}

#[tokio::test]
async fn bad_languages_file_is_an_error() {
    let m: HashMap<String, String> = [(
        "AC_LANGUAGES_FILE".to_owned(),
        "/nonexistent/langs.toml".to_owned(),
    )]
    .into();
    let cfg = Config::from_map(&m).unwrap();
    assert!(Registry::load(&cfg, &Fake::default()).await.is_err());
}
