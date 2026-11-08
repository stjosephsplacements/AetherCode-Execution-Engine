//! Language table, request validation and the go-judge command builders.

use crate::{Cmd, CmdFile, CopyIn, Error, FileId};
use ae_core::Config;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};

const BUILTIN: &str = include_str!("languages.toml");

/// Caller-supplied command-line argument limits. Arguments are separate argv
/// entries, never passed through a shell.
pub const MAX_ARGS: usize = 16;
pub const MAX_ARG_BYTES: usize = 1 << 10;
pub const MAX_ARGS_TOTAL_LEN: usize = 4 << 10;

/// Caller-supplied environment limits.
pub const MAX_ENV_VARS: usize = 32;
pub const MAX_ENV_VALUE_BYTES: usize = 4 << 10;
pub const MAX_ENV_TOTAL_BYTES: usize = 8 << 10;

/// Bounds for per-request run limits; requests outside them are rejected upstream.
pub const MIN_RUN_CPU: u64 = 100_000_000;
pub const MAX_RUN_CPU: u64 = 20_000_000_000;
pub const MIN_RUN_MEM: u64 = 16 << 20;
pub const MAX_RUN_MEM: u64 = 2 << 30;
pub const MIN_RUN_STACK: u64 = 1 << 20;
pub const MAX_RUN_STACK: u64 = 1 << 30;
pub const MIN_RUN_PROCS: u64 = 1;
pub const MAX_RUN_PROCS: u64 = 512;

/// Compiler diagnostics stay small; not configurable.
const COMPILE_OUT_MAX: u64 = 64 << 10;

/// Variable families that let a loader or runtime run code before the submission
/// starts, or change how it reports resource exhaustion. Audited per runtime in
/// the language table; a new language must add its own hooks.
const DENIED_ENV_PREFIXES: &[&str] = &[
    "LD_", "DYLD_", "MALLOC_", "PYTHON", "NODE_", "BUN_", "JAVA_", "_JAVA_", "JDK_", "MONO_",
    "RUBY", "PHP", "BASH",
];

const DENIED_ENV_KEYS: &[&str] = &[
    "GLIBC_TUNABLES",
    "GCONV_PATH",
    "LOCPATH",
    "NLSPATH",
    "TMPDIR",
    "IFS",
    "ENV",
    "SHELLOPTS",
    "PS4",
    "GODEBUG",
    "GOGC",
    "GOMEMLIMIT",
    "GOMAXPROCS",
    "GOTRACEBACK",
];

/// Resource limits of one command: CPU time (ns), memory (bytes), processes.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub cpu: u64,
    pub memory: u64,
    pub procs: u64,
    /// Floor for a per-request memory limit (JVM heap + metaspace); run only.
    #[serde(default)]
    pub min_memory: u64,
}

/// A command template: argv, default flags, engine-owned env and limits.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CmdTemplate {
    pub args: Vec<String>,
    /// Flags appended after `args` unless a caller flag has the same name (the
    /// text before '='), which then replaces it.
    #[serde(default)]
    pub defaults: Vec<String>,
    #[serde(default)]
    pub env: Vec<String>,
    pub limits: Limits,
}

/// Where the source and compiled binary live inside the sandbox.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    pub source: Option<String>,
    pub binary: Option<String>,
    /// The source is delivered on stdin instead of as a file (SQLite).
    #[serde(default)]
    pub stdin_is_source: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Language {
    pub id: String,
    /// Toolchain version probe argv.
    pub compiler: Vec<String>,
    pub compile: Option<CmdTemplate>,
    pub run: CmdTemplate,
    pub placement: Placement,
    /// Concurrent tests per job; `None` means the worker default (32).
    pub parallelism: Option<usize>,
    pub allowed_flags: Vec<String>,
    /// Self-test program; prints "hello".
    pub hello: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LangDef {
    compiler: Vec<String>,
    compile_cmd: Option<CmdTemplate>,
    execute_cmd: CmdTemplate,
    #[serde(default)]
    placement: Placement,
    parallelism: Option<usize>,
    #[serde(default)]
    allowed_flags: Vec<String>,
    hello: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TableDef {
    languages: BTreeMap<String, LangDef>,
}

impl Language {
    /// Default run limits.
    pub fn limits(&self) -> &Limits {
        &self.run.limits
    }

    pub fn is_compiled(&self) -> bool {
        self.compile.is_some()
    }

    /// CPU and memory a run uses. Memory never drops below `min_memory`: a JVM
    /// cannot start in a small question limit.
    pub fn effective(&self, limits: &RunLimits) -> (u64, u64) {
        let l = &self.run.limits;
        let cpu = if limits.cpu != 0 { limits.cpu } else { l.cpu };
        let memory = if limits.memory != 0 {
            limits.memory.max(l.min_memory)
        } else {
            l.memory
        };
        (cpu, memory)
    }

    /// Executables the language needs inside the sandbox: its compiler, if any,
    /// and the program that runs it (not the compiled program itself).
    pub fn binaries(&self) -> Vec<&str> {
        let mut out = Vec::new();
        if let Some(c) = &self.compile {
            out.push(c.args[0].as_str());
        }
        let r = self.run.args[0].as_str();
        if Some(r) != self.placement.binary.as_deref() {
            out.push(r);
        }
        out
    }

    /// Env for the version probe: the compile env, or the run env when interpreted.
    pub(crate) fn tool_env(&self) -> &[String] {
        self.compile.as_ref().map_or(&self.run.env, |c| &c.env)
    }
}

/// The language table, keyed by id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Languages {
    by_id: BTreeMap<String, Language>,
    reserved_env: HashSet<String>,
}

impl Languages {
    /// The built-in table.
    pub fn builtin() -> Self {
        Self::from_toml(BUILTIN).unwrap_or_else(|e| panic!("built-in language table: {e}"))
    }

    /// The table at `path`, or the built-in one when `path` is empty.
    pub fn load(path: &str) -> Result<Self, Error> {
        if path.is_empty() {
            return Ok(Self::builtin());
        }
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("read language table {path}: {e}")))?;
        Self::from_toml(&text)
    }

    pub fn from_toml(text: &str) -> Result<Self, Error> {
        let def: TableDef = toml::from_str(text)
            .map_err(|e| Error::Config(format!("parse language table: {e}")))?;
        let mut by_id = BTreeMap::new();
        for (id, d) in def.languages {
            let lang = Language {
                id: id.clone(),
                compiler: d.compiler,
                compile: d.compile_cmd,
                run: d.execute_cmd,
                placement: d.placement,
                parallelism: d.parallelism,
                allowed_flags: d.allowed_flags,
                hello: d.hello,
            };
            check(&lang).map_err(|e| Error::Config(format!("language {id}: {e}")))?;
            by_id.insert(id, lang);
        }
        if by_id.is_empty() {
            return Err(Error::Config("language table is empty".into()));
        }
        let reserved_env = by_id
            .values()
            .flat_map(|l| l.compile.iter().flat_map(|c| &c.env).chain(&l.run.env))
            .filter_map(|kv| kv.split_once('=').map(|(k, _)| k.to_owned()))
            .collect();
        Ok(Self {
            by_id,
            reserved_env,
        })
    }

    pub fn get(&self, id: &str) -> Option<&Language> {
        self.by_id.get(id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Language)> {
        self.by_id.iter().map(|(k, v)| (k.as_str(), v))
    }
}

fn check(l: &Language) -> Result<(), String> {
    let in_w = |p: &Option<String>| p.as_deref().is_none_or(|p| p.starts_with("/w/"));
    if l.compiler.is_empty() {
        return Err("compiler probe argv is empty".into());
    }
    if l.parallelism == Some(0) {
        return Err("parallelism must be at least 1 (omit it for the default of 32)".into());
    }
    if !in_w(&l.placement.source) || !in_w(&l.placement.binary) {
        return Err("placement paths must be under /w/".into());
    }
    if l.placement.source.is_none() && !l.placement.stdin_is_source {
        return Err("placement needs a source path or stdin_is_source".into());
    }
    if l.compile.is_some() && (l.placement.binary.is_none() || l.placement.source.is_none()) {
        return Err("a compiled language needs placement source and binary paths".into());
    }
    for t in l.compile.iter().chain(std::iter::once(&l.run)) {
        if t.args.is_empty() || t.args[0].is_empty() {
            return Err("command args are empty".into());
        }
        if let Some(kv) = t.env.iter().find(|kv| !kv.contains('=')) {
            return Err(format!("env entry {kv:?} must be KEY=VALUE"));
        }
    }
    Ok(())
}

/// Per-run output caps (`AC_SANDBOX_STDOUT_MAX_BYTES`, `AC_SANDBOX_STDERR_MAX_BYTES`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputCaps {
    pub stdout: u64,
    pub stderr: u64,
}

impl OutputCaps {
    pub fn from_config(cfg: &Config) -> Self {
        Self {
            stdout: cfg.sandbox_stdout_max_bytes,
            stderr: cfg.sandbox_stderr_max_bytes,
        }
    }
}

/// Per-job overrides of a language's run limits. Zero keeps the default.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunLimits {
    pub cpu: u64,
    pub memory: u64,
    /// Stack limit in bytes; zero keeps the sandbox default.
    pub stack: u64,
    pub procs: u64,
    /// Arguments appended to the program's argv (see [`validate_args`]).
    pub args: Vec<String>,
}

/// Inputs of one test run.
#[derive(Debug, Clone, Default)]
pub struct RunInput<'a> {
    /// Inlined when there is no `file_id` (interpreted) or on stdin (`stdin_is_source`).
    pub source: &'a str,
    pub stdin: &'a str,
    /// Cached binary (compiled) or source (interpreted).
    pub file_id: Option<&'a FileId>,
    pub limits: RunLimits,
    pub extra_files: Option<&'a BTreeMap<String, String>>,
    /// Caller env, validated by [`validate_env`]; appended after the language env.
    pub env: &'a [String],
}

/// Wall-clock limit: twice the CPU limit plus a second, so a program blocked on
/// sleep or I/O cannot hold a worker forever.
pub fn clock_limit(cpu: u64) -> u64 {
    cpu.saturating_mul(2).saturating_add(1_000_000_000)
}

fn flag_name(flag: &str) -> &str {
    flag.split_once('=').map_or(flag, |(n, _)| n)
}

/// Compile command; `None` for interpreted languages.
pub fn compile_cmd(
    lang: &Language,
    source: &str,
    extra_files: &BTreeMap<String, String>,
    flags: &[String],
) -> Option<Cmd> {
    let c = lang.compile.as_ref()?;
    let mut args = c.args.clone();
    for def in &c.defaults {
        if !flags.iter().any(|f| flag_name(f) == flag_name(def)) {
            args.push(def.clone());
        }
    }
    args.extend(flags.iter().cloned());

    let mut copy_in = BTreeMap::new();
    if let Some(src) = &lang.placement.source {
        copy_in.insert(src.clone(), CopyIn::content(source));
    }
    for (name, content) in extra_files {
        copy_in.insert(format!("/w/{name}"), CopyIn::content(content));
    }
    Some(Cmd {
        args,
        env: c.env.clone(),
        files: vec![
            CmdFile::stdin(""),
            CmdFile::collector("stdout", COMPILE_OUT_MAX),
            CmdFile::collector("stderr", COMPILE_OUT_MAX),
        ],
        cpu_limit: c.limits.cpu,
        clock_limit: clock_limit(c.limits.cpu),
        memory_limit: c.limits.memory,
        proc_limit: c.limits.procs,
        stack_limit: 0,
        copy_in,
        copy_out: vec!["stdout".into(), "stderr".into()],
        copy_out_cached: lang.placement.binary.iter().cloned().collect(),
    })
}

/// No-op command that uploads interpreted source into the file cache so a batch
/// of runs can reference it by id. `None` when the source is delivered on stdin.
pub fn cache_source_cmd(lang: &Language, source: &str) -> Option<Cmd> {
    let src = lang.placement.source.as_ref()?;
    Some(Cmd {
        args: vec!["/bin/true".into()],
        env: Vec::new(),
        files: vec![
            CmdFile::stdin(""),
            CmdFile::collector("stdout", 0),
            CmdFile::collector("stderr", 0),
        ],
        cpu_limit: 1_000_000_000,
        clock_limit: 2_000_000_000,
        memory_limit: 4 << 20,
        proc_limit: 1,
        stack_limit: 0,
        copy_in: BTreeMap::from([(src.clone(), CopyIn::content(source))]),
        copy_out: Vec::new(),
        copy_out_cached: vec![src.clone()],
    })
}

/// Command for one test run.
pub fn run_cmd(lang: &Language, input: &RunInput<'_>, caps: OutputCaps) -> Cmd {
    let (cpu, memory) = lang.effective(&input.limits);
    let mut args = lang.run.args.clone();
    args.extend(input.limits.args.iter().cloned());
    let mut env = lang.run.env.clone();
    env.extend(input.env.iter().cloned());

    let mut stdin = input.stdin;
    let mut copy_in = BTreeMap::new();
    if let Some(id) = input.file_id {
        let target = if lang.is_compiled() {
            &lang.placement.binary
        } else {
            &lang.placement.source
        };
        if let Some(t) = target {
            copy_in.insert(t.clone(), CopyIn::file_id(&id.0));
        }
    } else if lang.placement.stdin_is_source {
        stdin = input.source;
    } else if !lang.is_compiled()
        && let Some(src) = &lang.placement.source
    {
        copy_in.insert(src.clone(), CopyIn::content(input.source));
    }
    for (name, content) in input.extra_files.into_iter().flatten() {
        copy_in.insert(format!("/w/{name}"), CopyIn::content(content));
    }

    Cmd {
        args,
        env,
        files: vec![
            CmdFile::stdin(stdin),
            CmdFile::collector("stdout", caps.stdout),
            CmdFile::collector("stderr", caps.stderr),
        ],
        cpu_limit: cpu,
        clock_limit: clock_limit(cpu),
        memory_limit: memory,
        proc_limit: if input.limits.procs != 0 {
            input.limits.procs
        } else {
            lang.run.limits.procs
        },
        stack_limit: input.limits.stack,
        copy_in,
        copy_out: vec!["stdout".into(), "stderr".into()],
        copy_out_cached: Vec::new(),
    }
}

/// Checks the command-line arguments of a run request.
pub fn validate_args(args: &[String]) -> Result<(), String> {
    if args.len() > MAX_ARGS {
        return Err(format!("at most {MAX_ARGS} arguments"));
    }
    let mut total = 0;
    for (i, a) in args.iter().enumerate() {
        if a.len() > MAX_ARG_BYTES {
            return Err(format!("argument {i} is longer than {MAX_ARG_BYTES} bytes"));
        }
        if a.contains('\0') {
            return Err(format!("argument {i} contains a NUL byte"));
        }
        total += a.len();
    }
    if total > MAX_ARGS_TOTAL_LEN {
        return Err(format!(
            "arguments total more than {MAX_ARGS_TOTAL_LEN} bytes"
        ));
    }
    Ok(())
}

fn valid_env_key(k: &str) -> bool {
    let mut b = k.bytes();
    matches!(b.next(), Some(c) if c.is_ascii_alphabetic() || c == b'_')
        && b.all(|c| c.is_ascii_alphanumeric() || c == b'_')
}

/// Checks a caller environment: unique portable `KEY=VALUE` entries, no denied or
/// engine-reserved keys, within the size caps. Duplicates are rejected because
/// glibc getenv returns the first match.
pub fn validate_env(env: &[String], langs: &Languages) -> Result<(), String> {
    if env.len() > MAX_ENV_VARS {
        return Err(format!(
            "too many variables (maximum {MAX_ENV_VARS} allowed)"
        ));
    }
    let mut seen = HashSet::new();
    let mut total = 0;
    for kv in env {
        let Some((key, value)) = kv.split_once('=') else {
            return Err(format!("entry {kv:?} must be KEY=VALUE"));
        };
        if !valid_env_key(key) {
            return Err(format!("key {key:?} is invalid"));
        }
        if value.contains('\0') {
            return Err(format!("value for {key:?} contains a NUL byte"));
        }
        if value.len() > MAX_ENV_VALUE_BYTES {
            return Err(format!(
                "value for {key:?} exceeds {MAX_ENV_VALUE_BYTES} bytes"
            ));
        }
        if DENIED_ENV_KEYS.contains(&key)
            || langs.reserved_env.contains(key)
            || DENIED_ENV_PREFIXES.iter().any(|p| key.starts_with(p))
        {
            return Err(format!("key {key:?} is not permitted"));
        }
        if !seen.insert(key) {
            return Err(format!("key {key:?} is duplicated"));
        }
        total += kv.len();
        if total > MAX_ENV_TOTAL_BYTES {
            return Err(format!("total size exceeds {MAX_ENV_TOTAL_BYTES} bytes"));
        }
    }
    Ok(())
}

/// `base` with `overrides` applied: a same-key override replaces the base entry in
/// place, new keys are appended in order.
pub fn merge_env(base: &[String], overrides: &[String]) -> Vec<String> {
    let key = |kv: &str| kv.split_once('=').map_or(kv, |(k, _)| k).to_owned();
    let mut out: Vec<String> = Vec::with_capacity(base.len() + overrides.len());
    let mut idx = std::collections::HashMap::new();
    for kv in base.iter().chain(overrides) {
        match idx.get(&key(kv)) {
            Some(&i) => out[i] = kv.clone(),
            None => {
                idx.insert(key(kv), out.len());
                out.push(kv.clone());
            }
        }
    }
    out
}

/// Checks that every caller compiler flag is allowed for the language.
pub fn validate_compiler_flags(lang: &Language, flags: &[String]) -> Result<(), String> {
    if lang.allowed_flags.is_empty() {
        if flags.is_empty() {
            return Ok(());
        }
        return Err(format!(
            "custom compiler flags are not supported for {}",
            lang.id
        ));
    }
    match flags.iter().find(|f| !lang.allowed_flags.contains(f)) {
        Some(f) => Err(format!("flag {f:?} is not permitted for {}", lang.id)),
        None => Ok(()),
    }
}

/// Lexically cleans a slash path like Go's `path.Clean`; `None` if it climbs
/// above its start.
fn clean_relative(name: &str) -> Option<String> {
    let mut parts: Vec<&str> = Vec::new();
    for seg in name.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                parts.pop()?;
            }
            s => parts.push(s),
        }
    }
    Some(parts.join("/"))
}

/// Checks multi-file submissions: file count, combined size, and relative paths
/// that stay inside the sandbox working directory.
pub fn validate_files(
    files: &BTreeMap<String, String>,
    max_files: usize,
    max_total_bytes: usize,
) -> Result<(), String> {
    if files.len() > max_files {
        return Err(format!("too many files (maximum {max_files} allowed)"));
    }
    let mut total = 0;
    for (name, content) in files {
        let ok = !name.starts_with('/')
            && !name.contains('\0')
            && clean_relative(name).is_some_and(|c| !c.is_empty());
        if !ok {
            return Err(format!("invalid file path {name:?}"));
        }
        total += content.len();
        if total > max_total_bytes {
            return Err(format!(
                "total multi-file size exceeds {max_total_bytes} bytes"
            ));
        }
    }
    Ok(())
}
