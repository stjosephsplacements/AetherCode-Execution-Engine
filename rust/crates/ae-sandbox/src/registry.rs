//! Language registry: the table plus per-language availability from a self-test
//! (toolchain probe, version probe, hello-world compile and run), refreshed
//! periodically like the Go engine's language probe.

use crate::probe::{probe_languages, toolchain_version};
use crate::{
    Error, FileId, Language, Languages, OutputCaps, RunInput, RunResult, STATUS_ACCEPTED, Sandbox,
    compile_cmd, run_cmd,
};
use ae_core::Config;
use futures_util::future::join_all;
use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant};

/// How long [`Registry::load`] waits for go-judge to answer at startup.
pub const STARTUP_BUDGET: Duration = Duration::from_secs(30);
/// Go's language probe interval (cmd/stj-exec: `StartLanguageProbe(ctx, 5*time.Minute)`).
pub const REFRESH_INTERVAL: Duration = Duration::from_secs(5 * 60);

const BACKOFF_START: Duration = Duration::from_millis(100);
const BACKOFF_MAX: Duration = Duration::from_secs(2);
const UNKNOWN_VERSION: &str = "unknown";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LanguageStatus {
    pub id: String,
    pub available: bool,
}

/// The last definite answer for a language.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Answer {
    available: bool,
    version: String,
}

/// Self-test result. `Error` means the sandbox could not be asked.
enum Outcome {
    Pass(String),
    Fail(String),
    Error(String),
}

#[derive(Debug)]
pub struct Registry {
    languages: Languages,
    caps: OutputCaps,
    /// No entry means no answer yet; such a language counts as available.
    answers: RwLock<BTreeMap<String, Answer>>,
}

impl Registry {
    /// [`Registry::load_with_budget`] with [`STARTUP_BUDGET`].
    pub async fn load(cfg: &Config, sb: &dyn Sandbox) -> Result<Self, Error> {
        Self::load_with_budget(cfg, sb, STARTUP_BUDGET).await
    }

    /// Loads the table (`AC_LANGUAGES_FILE` or the built-in one), waits for the
    /// sandbox to answer `version()` with bounded exponential backoff, then
    /// self-tests every language. Errors on a bad table or with
    /// [`Error::SandboxUnreachable`] when the sandbox never answers within `budget`.
    pub async fn load_with_budget(
        cfg: &Config,
        sb: &dyn Sandbox,
        budget: Duration,
    ) -> Result<Self, Error> {
        let languages = Languages::load(&cfg.languages_file)?;
        wait_for_sandbox(sb, budget).await?;
        let reg = Self {
            languages,
            caps: OutputCaps::from_config(cfg),
            answers: RwLock::new(BTreeMap::new()),
        };
        reg.refresh(sb).await;
        Ok(reg)
    }

    /// Re-runs the probes and self-tests. A probe that errors keeps the previous
    /// answer: a failed probe is not a missing toolchain.
    pub async fn refresh(&self, sb: &dyn Sandbox) {
        let present = match probe_languages(sb, &self.languages).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "sandbox: language probe failed; keeping the last answer");
                return;
            }
        };
        let checks = self.languages.iter().map(|(id, lang)| {
            let present = present.get(id).copied().unwrap_or(false);
            async move {
                if !present {
                    return (id, Outcome::Fail("toolchain missing".into()));
                }
                (id, self_test(sb, lang, self.caps).await)
            }
        });
        let outcomes = join_all(checks).await;
        let mut answers = self.answers.write().unwrap_or_else(PoisonError::into_inner);
        for (id, outcome) in outcomes {
            let answer = match outcome {
                Outcome::Pass(version) => Answer {
                    available: true,
                    version,
                },
                Outcome::Fail(reason) => {
                    tracing::warn!(language = %id, %reason, "sandbox: language unavailable");
                    Answer {
                        available: false,
                        version: String::new(),
                    }
                }
                Outcome::Error(reason) => {
                    tracing::warn!(language = %id, %reason, "sandbox: language self-test errored; keeping the last answer");
                    continue;
                }
            };
            answers.insert(id.to_owned(), answer);
        }
    }

    /// Refreshes every `every` until the task is aborted.
    pub fn spawn_refresher(
        self: Arc<Self>,
        sb: Arc<dyn Sandbox>,
        every: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                self.refresh(sb.as_ref()).await;
            }
        })
    }

    fn is_available(&self, id: &str) -> bool {
        self.answers
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .is_none_or(|a| a.available)
    }

    /// An available language (or one with no answer yet).
    pub fn get(&self, id: &str) -> Option<&Language> {
        self.languages.get(id).filter(|_| self.is_available(id))
    }

    /// Every configured language and whether it is available, by id.
    pub fn catalogue(&self) -> Vec<LanguageStatus> {
        self.languages
            .iter()
            .map(|(id, _)| LanguageStatus {
                id: id.to_owned(),
                available: self.is_available(id),
            })
            .collect()
    }

    /// The compiler version probe output of an available language; "unknown"
    /// when the probe failed but the self-test passed.
    pub fn toolchain_version(&self, id: &str) -> Option<String> {
        self.answers
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .filter(|a| a.available)
            .map(|a| a.version.clone())
    }

    /// The full table, including unavailable languages (for env validation).
    pub fn languages(&self) -> &Languages {
        &self.languages
    }

    pub fn output_caps(&self) -> OutputCaps {
        self.caps
    }
}

async fn wait_for_sandbox(sb: &dyn Sandbox, budget: Duration) -> Result<(), Error> {
    let start = Instant::now();
    let mut delay = BACKOFF_START;
    loop {
        match sb.version().await {
            Ok(_) => return Ok(()),
            Err(e) => {
                if start.elapsed() + delay > budget {
                    tracing::error!(error = %e, "sandbox: go-judge unreachable");
                    return Err(Error::SandboxUnreachable);
                }
                tracing::warn!(error = %e, retry_in = ?delay, "sandbox: go-judge not ready");
            }
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(BACKOFF_MAX);
    }
}

/// Probes the version, then compiles and runs the hello-world program. A failed
/// version probe only records "unknown".
async fn self_test(sb: &dyn Sandbox, lang: &Language, caps: OutputCaps) -> Outcome {
    let version = toolchain_version(sb, lang).await.unwrap_or_else(|reason| {
        tracing::warn!(language = %lang.id, %reason, "sandbox: toolchain version probe failed");
        UNKNOWN_VERSION.to_owned()
    });
    let mut file_id = None;
    if let Some(cmd) = compile_cmd(lang, &lang.hello, &BTreeMap::new(), &[]) {
        let r = match first(sb.run(vec![cmd]).await) {
            Ok(r) => r,
            Err(e) => return Outcome::Error(e),
        };
        let binary = lang.placement.binary.as_deref().unwrap_or_default();
        match r.file_ids.get(binary) {
            Some(id) if r.status == STATUS_ACCEPTED => file_id = Some(FileId(id.clone())),
            _ => {
                let stderr = r.files.get("stderr").map_or("", String::as_str);
                return Outcome::Fail(format!("compile: {} {}", r.status, stderr.trim()));
            }
        }
    }
    let cmd = run_cmd(
        lang,
        &RunInput {
            source: &lang.hello,
            file_id: file_id.as_ref(),
            ..RunInput::default()
        },
        caps,
    );
    let run = first(sb.run(vec![cmd]).await);
    if let Some(id) = file_id
        && let Err(e) = sb.delete_file(id).await
    {
        tracing::warn!(language = %lang.id, error = %e, "sandbox: release self-test binary");
    }
    let r = match run {
        Ok(r) => r,
        Err(e) => return Outcome::Error(e),
    };
    let stdout = r.files.get("stdout").map_or("", String::as_str);
    if r.status != STATUS_ACCEPTED || stdout.trim() != "hello" {
        return Outcome::Fail(format!("run: {} {:?}", r.status, stdout));
    }
    Outcome::Pass(version)
}

fn first(r: Result<Vec<RunResult>, Error>) -> Result<RunResult, String> {
    r.map_err(|e| e.to_string())?
        .into_iter()
        .next()
        .ok_or_else(|| "no result".to_owned())
}
