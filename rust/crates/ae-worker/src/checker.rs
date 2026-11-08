//! Problem-setter programs (custom checker, interactor) and the custom checker
//! itself (Go: `custom_checker.go`).

use crate::cache::{CacheKey, CachedFile, CompileCache};
use crate::{Error, Result};
use ae_core::model::Verdict;
use ae_sandbox::{
    Cmd, CmdFile, CopyIn, FileId, Language, Registry, STATUS_ACCEPTED,
    STATUS_MEMORY_LIMIT_EXCEEDED, STATUS_NONZERO_EXIT, STATUS_TIME_LIMIT_EXCEEDED, Sandbox,
    clock_limit, compile_cmd,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// CPU cap of a checker or interactor run.
pub(crate) const PROGRAM_CPU: u64 = 5_000_000_000;
pub(crate) const PROGRAM_MEMORY: u64 = 512 << 20;
pub(crate) const PROGRAM_OUTPUT_MAX: u64 = 64 << 10;
const CHECKER_TIMEOUT: Duration = Duration::from_secs(10);

/// A checker or interactor ready to run: compiled, or its source uploaded.
#[derive(Debug, Clone)]
pub struct Program {
    pub language: Language,
    pub source: String,
    /// The binary (compiled) or the uploaded source (interpreted); `None` inlines the source.
    pub file: Option<CachedFile>,
}

impl Program {
    pub fn file_id(&self) -> Option<&FileId> {
        self.file.as_ref().map(CachedFile::id)
    }

    /// The program's files: its binary or source, by id when cached.
    pub(crate) fn mount(&self, copy_in: &mut BTreeMap<String, CopyIn>) {
        let lang = &self.language;
        let target = if lang.is_compiled() {
            &lang.placement.binary
        } else {
            &lang.placement.source
        };
        match (self.file_id(), target) {
            (Some(id), Some(t)) => {
                copy_in.insert(t.clone(), CopyIn::file_id(&id.0));
            }
            (None, Some(t)) if !lang.is_compiled() => {
                copy_in.insert(t.clone(), CopyIn::content(&self.source));
            }
            _ => {}
        }
    }
}

pub(crate) fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// Compiles (compiled languages) or uploads (interpreted) a checker or
/// interactor through the compile cache. `kind` ("checker" or "interactor")
/// namespaces the cache key and errors; an empty `lang` means python.
///
/// # Errors
/// An unsupported language, a compile failure or a sandbox error while compiling.
/// A failed upload of interpreted source is not an error: the source is inlined.
pub async fn prepare_program(
    cache: &CompileCache,
    sandbox: &Arc<dyn Sandbox>,
    registry: &Registry,
    kind: &str,
    lang: &str,
    code: &str,
) -> Result<Program> {
    let lang = if lang.is_empty() { "python" } else { lang };
    let language = registry
        .get(lang)
        .cloned()
        .ok_or_else(|| Error::Other(format!("unsupported {kind} language: {lang}")))?;
    let version = registry.toolchain_version(lang).unwrap_or_default();
    let key = CacheKey::new(code, &format!("{kind}:{lang}"), &[], &version);
    let mut program = Program {
        language,
        source: code.to_owned(),
        file: None,
    };
    let lang = &program.language;
    if lang.is_compiled() {
        let compiled = cache
            .get_or_compile(
                key,
                compile(sandbox.clone(), lang, code, &BTreeMap::new(), &[]),
            )
            .await;
        program.file = Some(match compiled {
            Ok((f, _)) => f,
            Err(Error::Compile(stderr)) => {
                return Err(Error::Other(format!("{kind} compilation failed: {stderr}")));
            }
            Err(e) => return Err(Error::Other(format!("compile {kind}: {e}"))),
        });
    } else if !lang.placement.stdin_is_source
        && let Some(src) = lang.placement.source.as_deref()
    {
        let (sb, name, bytes) = (sandbox.clone(), file_name(src).to_owned(), code.to_owned());
        let upload = async move {
            tokio::time::timeout(UPLOAD_TIMEOUT, sb.cache_file(&name, bytes.as_bytes()))
                .await
                .map_err(|_| Error::Other(format!("upload timed out after {UPLOAD_TIMEOUT:?}")))?
                .map_err(Error::from)
        };
        match cache.get_or_compile(key, upload).await {
            Ok((f, _)) => program.file = Some(f),
            Err(e) => tracing::warn!(kind, error = %e, "upload failed; inlining the source"),
        }
    }
    Ok(program)
}

/// Bound on one checker or interactor source upload to go-judge.
pub(crate) const UPLOAD_TIMEOUT: Duration = Duration::from_secs(30);

/// Allowance beyond the compile's wall-clock limit for the go-judge round trip.
pub(crate) const COMPILE_SLACK: Duration = Duration::from_secs(5);

/// One compile, as an owned future (the cache runs it as its own task): the
/// binary's file id, or [`Error::Compile`] with the diagnostics. The go-judge
/// client has no overall timeout and the detached task outlives its caller, so
/// the call is bounded here by the compile clock limit plus [`COMPILE_SLACK`];
/// on expiry the cache clears the flight and waiters retry.
pub(crate) fn compile(
    sandbox: Arc<dyn Sandbox>,
    lang: &Language,
    source: &str,
    files: &BTreeMap<String, String>,
    flags: &[String],
) -> impl Future<Output = Result<FileId>> + Send + 'static {
    let cmd = compile_cmd(lang, source, files, flags);
    let (id, binary) = (
        lang.id.clone(),
        lang.placement.binary.clone().unwrap_or_default(),
    );
    let limit = Duration::from_nanos(
        lang.compile
            .as_ref()
            .map_or(0, |c| clock_limit(c.limits.cpu)),
    ) + COMPILE_SLACK;
    async move {
        tokio::time::timeout(limit, run_compile(sandbox.as_ref(), cmd, &id, &binary))
            .await
            .unwrap_or_else(|_| Err(Error::Other(format!("compile timed out after {limit:?}"))))
    }
}

async fn run_compile(
    sandbox: &dyn Sandbox,
    cmd: Option<Cmd>,
    lang_id: &str,
    binary: &str,
) -> Result<FileId> {
    let cmd = cmd.ok_or_else(|| Error::Other(format!("{lang_id} is not compiled")))?;
    let r = sandbox
        .run(vec![cmd])
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| Error::Other("compile: empty response".into()))?;
    if r.status != STATUS_ACCEPTED {
        return Err(Error::Compile(
            r.files.get("stderr").cloned().unwrap_or_default(),
        ));
    }
    r.file_ids
        .get(binary)
        .or_else(|| r.file_ids.values().next())
        .map(|id| FileId(id.clone()))
        .ok_or_else(|| Error::Other("no cached file returned from compile".into()))
}

/// The checker run (testlib convention): `checker input user_output expected_output`.
pub fn checker_cmd(p: &Program, input: &str, expected: &str, actual: &str) -> Cmd {
    let lang = &p.language;
    let mut args = lang.run.args.clone();
    args.extend(
        [
            "/w/input.txt",
            "/w/user_output.txt",
            "/w/expected_output.txt",
        ]
        .map(str::to_owned),
    );
    let mut copy_in = BTreeMap::from([
        ("/w/input.txt".to_owned(), CopyIn::content(input)),
        ("/w/user_output.txt".to_owned(), CopyIn::content(actual)),
        (
            "/w/expected_output.txt".to_owned(),
            CopyIn::content(expected),
        ),
    ]);
    p.mount(&mut copy_in);
    Cmd {
        args,
        env: lang.run.env.clone(),
        files: vec![
            CmdFile::stdin(""),
            CmdFile::collector("stdout", PROGRAM_OUTPUT_MAX),
            CmdFile::collector("stderr", PROGRAM_OUTPUT_MAX),
        ],
        cpu_limit: PROGRAM_CPU,
        clock_limit: clock_limit(PROGRAM_CPU),
        memory_limit: PROGRAM_MEMORY,
        proc_limit: lang.run.limits.procs,
        stack_limit: 0,
        copy_in,
        copy_out: vec!["stdout".into(), "stderr".into()],
        copy_out_cached: Vec::new(),
    }
}

/// Runs the checker and reads its verdict from the exit status: 0 accepts,
/// anything else is a wrong answer; a checker that times out or crashes is an
/// internal error. Returns the verdict and the checker's message.
pub async fn evaluate_checker(
    sandbox: &dyn Sandbox,
    p: &Program,
    input: &str,
    expected: &str,
    actual: &str,
) -> (Verdict, String) {
    let cmd = checker_cmd(p, input, expected, actual);
    let res = match tokio::time::timeout(CHECKER_TIMEOUT, sandbox.run(vec![cmd])).await {
        Err(_) => {
            return (
                Verdict::InternalError,
                "checker runner failed: context deadline exceeded".into(),
            );
        }
        Ok(Err(e)) => {
            return (
                Verdict::InternalError,
                format!("checker runner failed: {e}"),
            );
        }
        Ok(Ok(r)) => r,
    };
    let Some(r) = res.into_iter().next() else {
        return (Verdict::InternalError, "no result from checker".into());
    };
    let stderr = r.files.get("stderr").cloned().unwrap_or_default();
    match r.status.as_str() {
        STATUS_ACCEPTED if r.exit_status == 0 => (Verdict::Accepted, String::new()),
        STATUS_ACCEPTED => (Verdict::WrongAnswer, stderr),
        STATUS_NONZERO_EXIT if r.exit_status == 1 => (Verdict::WrongAnswer, stderr),
        STATUS_NONZERO_EXIT => {
            let msg = stderr.trim();
            if msg.is_empty() {
                (
                    Verdict::WrongAnswer,
                    format!("checker exited with code {}", r.exit_status),
                )
            } else {
                (Verdict::WrongAnswer, msg.to_owned())
            }
        }
        STATUS_TIME_LIMIT_EXCEEDED => (Verdict::InternalError, "checker timed out".into()),
        STATUS_MEMORY_LIMIT_EXCEEDED => (
            Verdict::InternalError,
            "checker exceeded memory limit".into(),
        ),
        other => (
            Verdict::InternalError,
            format!("checker failure ({other}): {stderr}"),
        ),
    }
}
