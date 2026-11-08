//! Sandbox abstraction, the go-judge backend and the language registry.
//!
//! Commands are argv arrays sent to go-judge; nothing is ever run through a shell.

mod gojudge;
mod language;
mod probe;
mod registry;

pub use gojudge::{GoJudge, with_request_id};
pub use language::{
    CmdTemplate, Language, Languages, Limits, MAX_ARG_BYTES, MAX_ARGS, MAX_ARGS_TOTAL_LEN,
    MAX_ENV_TOTAL_BYTES, MAX_ENV_VALUE_BYTES, MAX_ENV_VARS, MAX_RUN_CPU, MAX_RUN_MEM,
    MAX_RUN_PROCS, MAX_RUN_STACK, MIN_RUN_CPU, MIN_RUN_MEM, MIN_RUN_PROCS, MIN_RUN_STACK,
    OutputCaps, Placement, RunInput, RunLimits, cache_source_cmd, clock_limit, compile_cmd,
    merge_env, run_cmd, validate_args, validate_compiler_flags, validate_env, validate_files,
};
pub use probe::probe_languages;
pub use registry::{LanguageStatus, REFRESH_INTERVAL, Registry, STARTUP_BUDGET};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

pub const STATUS_ACCEPTED: &str = "Accepted";
pub const STATUS_NONZERO_EXIT: &str = "Nonzero Exit Status";
pub const STATUS_TIME_LIMIT_EXCEEDED: &str = "Time Limit Exceeded";
pub const STATUS_MEMORY_LIMIT_EXCEEDED: &str = "Memory Limit Exceeded";
pub const STATUS_OUTPUT_LIMIT_EXCEEDED: &str = "Output Limit Exceeded";
pub const STATUS_SIGNALLED: &str = "Signalled";
pub const STATUS_INTERNAL_ERROR: &str = "Internal Error";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sandbox: request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("sandbox: HTTP {status}: {body}")]
    Status { status: u16, body: String },
    #[error("sandbox: decode response: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("sandbox: response larger than {0} bytes")]
    TooLarge(usize),
    #[error("sandbox: got {got} results for {want} commands")]
    ResultCount { got: usize, want: usize },
    #[error("sandbox: invalid file id {0:?}")]
    InvalidFileId(String),
    #[error("sandbox: {0}")]
    Config(String),
    #[error("sandbox: go-judge did not answer within the startup budget")]
    SandboxUnreachable,
}

/// A go-judge file-cache id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileId(pub String);

/// One go-judge command (the REST `Cmd` object).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Cmd {
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<String>,
    pub files: Vec<CmdFile>,
    pub cpu_limit: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub clock_limit: u64,
    pub memory_limit: u64,
    pub proc_limit: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub stack_limit: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub copy_in: BTreeMap<String, CopyIn>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub copy_out: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub copy_out_cached: Vec<String>,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}

fn is_none_or_zero(v: &Option<u64>) -> bool {
    v.is_none_or(|v| v == 0)
}

/// An entry of `Cmd.files`: stdin content, or a named output collector with a size cap.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CmdFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// `None` and `Some(0)` are both omitted, like Go's `omitempty`.
    #[serde(default, skip_serializing_if = "is_none_or_zero")]
    pub max: Option<u64>,
}

impl CmdFile {
    pub fn stdin(content: &str) -> Self {
        Self {
            content: Some(content.to_owned()),
            ..Self::default()
        }
    }
    pub fn collector(name: &str, max: u64) -> Self {
        Self {
            name: Some(name.to_owned()),
            max: Some(max),
            ..Self::default()
        }
    }
}

/// A file copied into the sandbox: inline content or a cached file id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CopyIn {
    #[serde(rename = "content")]
    Content(String),
    #[serde(rename = "fileId")]
    FileId(String),
}

impl CopyIn {
    pub fn content(s: &str) -> Self {
        Self::Content(s.to_owned())
    }
    pub fn file_id(s: &str) -> Self {
        Self::FileId(s.to_owned())
    }
}

/// One go-judge result, one per `Cmd`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RunResult {
    pub status: String,
    pub exit_status: i32,
    pub error: String,
    pub time: u64,
    pub memory: u64,
    pub run_time: u64,
    pub files: HashMap<String, String>,
    pub file_ids: HashMap<String, String>,
}

/// One end of a pipe: command index and file descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipeIndex {
    pub index: usize,
    pub fd: u32,
}

/// Connects `in` (a writer) to `out` (a reader) between two commands of one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipeMap {
    #[serde(rename = "in")]
    pub input: PipeIndex,
    #[serde(rename = "out")]
    pub output: PipeIndex,
    pub proxy: bool,
}

/// A code-execution sandbox. Object safe: share it as `Arc<dyn Sandbox>`.
#[async_trait]
pub trait Sandbox: Send + Sync {
    /// Runs a batch of commands and returns one result per command, in order.
    async fn run(&self, cmds: Vec<Cmd>) -> Result<Vec<RunResult>, Error>;
    /// Runs commands wired together by `pipes` (interactive problems). Backends
    /// without pipe support reject the request.
    async fn run_piped(
        &self,
        cmds: Vec<Cmd>,
        pipes: Vec<PipeMap>,
    ) -> Result<Vec<RunResult>, Error> {
        if pipes.is_empty() {
            return self.run(cmds).await;
        }
        Err(Error::Config("pipe mapping is not supported".into()))
    }
    /// Stores a file in the sandbox file cache.
    async fn cache_file(&self, name: &str, bytes: &[u8]) -> Result<FileId, Error>;
    /// Removes a cached file.
    async fn delete_file(&self, id: FileId) -> Result<(), Error>;
    /// The sandbox build version.
    async fn version(&self) -> Result<String, Error>;
}
