//! Registry self-test against a fake Sandbox.

use ae_core::Config;
use ae_sandbox::{
    Cmd, Error, FileId, LanguageStatus, Registry, RunResult, STATUS_ACCEPTED, STATUS_NONZERO_EXIT,
    Sandbox,
};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// Behaves like a go-judge with every toolchain installed, except that runs whose
/// argv starts with one of `broken` print the wrong text and binaries in `missing`
/// are absent.
#[derive(Default)]
struct Fake {
    broken: HashSet<String>,
    missing: HashSet<String>,
    deleted: Mutex<Vec<FileId>>,
}

fn ok() -> RunResult {
    RunResult {
        status: STATUS_ACCEPTED.into(),
        ..RunResult::default()
    }
}

#[async_trait]
impl Sandbox for Fake {
    async fn run(&self, cmds: Vec<Cmd>) -> Result<Vec<RunResult>, Error> {
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
                if c.args
                    .iter()
                    .any(|a| a == "--version" || a == "-version" || a == "version")
                {
                    let mut r = ok();
                    r.files
                        .insert("stdout".into(), format!("{a0} 1.2.3\nextra\n"));
                    return r;
                }
                if let Some(bin) = c.copy_out_cached.first() {
                    let mut r = ok();
                    r.file_ids.insert(bin.clone(), format!("id-{a0}"));
                    return r;
                }
                let mut r = ok();
                let out = if self.broken.contains(a0) {
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
        Ok("fake".into())
    }
}

fn cfg() -> Config {
    Config::from_map(&HashMap::new()).unwrap()
}

#[tokio::test]
async fn all_languages_available_when_self_test_passes() {
    let fake = Fake::default();
    let reg = Registry::load(&cfg(), &fake).await.unwrap();
    let cat = reg.catalogue();
    assert_eq!(cat.len(), 14);
    assert!(cat.iter().all(|s| s.available), "{cat:?}");
    assert!(reg.get("rust").is_some());
    assert_eq!(reg.toolchain_version("rust"), Some("/usr/bin/rustc 1.2.3"));
    assert_eq!(reg.toolchain_version("nope"), None);
    // Every compiled language's cached binary is released after its self-test.
    assert_eq!(fake.deleted.lock().unwrap().len(), 7);
}

#[tokio::test]
async fn failed_self_test_marks_language_unavailable() {
    let fake = Fake {
        broken: ["/usr/bin/python3".to_owned()].into(),
        missing: ["/usr/bin/php".to_owned()].into(),
        ..Fake::default()
    };
    let reg = Registry::load(&cfg(), &fake).await.unwrap();
    let cat = reg.catalogue();
    let status = |id: &str| cat.iter().find(|s| s.id == id).unwrap().available;
    assert!(!status("python"));
    assert!(!status("php"));
    assert!(status("c") && status("java") && status("javascript"));
    assert!(reg.get("python").is_none());
    assert!(reg.get("php").is_none());
    assert!(reg.get("c").is_some());
    assert_eq!(
        cat.iter().find(|s| s.id == "python"),
        Some(&LanguageStatus {
            id: "python".into(),
            available: false
        })
    );
}

struct Down;
#[async_trait]
impl Sandbox for Down {
    async fn run(&self, _: Vec<Cmd>) -> Result<Vec<RunResult>, Error> {
        Err(Error::Status {
            status: 502,
            body: "down".into(),
        })
    }
    async fn cache_file(&self, _: &str, _: &[u8]) -> Result<FileId, Error> {
        unreachable!()
    }
    async fn delete_file(&self, _: FileId) -> Result<(), Error> {
        unreachable!()
    }
    async fn version(&self) -> Result<String, Error> {
        unreachable!()
    }
}

#[tokio::test]
async fn unreachable_sandbox_marks_everything_unavailable() {
    let sb: Arc<dyn Sandbox> = Arc::new(Down);
    let reg = Registry::load(&cfg(), sb.as_ref()).await.unwrap();
    assert!(reg.catalogue().iter().all(|s| !s.available));
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
