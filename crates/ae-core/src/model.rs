use serde::{Deserialize, Serialize};

/// Output checker. The wire names match the Go engine; unknown or empty names mean `Exact`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Checker {
    #[default]
    Exact,
    Token,
    Float,
}

impl Checker {
    /// Mirrors Go's `JudgeOutput` dispatch: "token" and "float" select those checkers, anything else is exact.
    #[must_use]
    pub fn from_name(s: &str) -> Self {
        match s {
            "token" => Self::Token,
            "float" => Self::Float,
            _ => Self::Exact,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Accepted,
    WrongAnswer,
    #[serde(rename = "time_limit")]
    TimeLimit,
    #[serde(rename = "memory_limit")]
    MemoryLimit,
    RuntimeError,
    CompilationError,
    InternalError,
    #[serde(rename = "output_limit")]
    OutputLimit,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Queued,
    Compiling,
    Running,
    Completed,
    Failed,
}

/// Seeded default tenant from migration 002.
pub const DEFAULT_TENANT_ID: &str = "00000000-0000-0000-0000-000000000001";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Submission {
    pub id: String,
    pub tenant_id: String,
    pub user_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub language: String,
    pub source_code: String,
    pub status: Status,
    pub verdict: Option<Verdict>,
    pub compile_stderr: String,
    pub cpu_time_ns: i64,
    pub memory_bytes: i64,
    pub test_count: i32,
    pub tests_passed: i32,
    /// Unix milliseconds.
    pub created_at: i64,
    /// Unix milliseconds.
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestCase {
    pub input: String,
    pub expected_output: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestResult {
    pub test_index: i32,
    pub verdict: Verdict,
    pub cpu_time_ns: i64,
    pub memory_bytes: i64,
    pub run_time_ns: i64,
    /// Exit code, or the signal number when the program was killed.
    pub exit_status: i32,
    pub sandbox_status: String,
    pub message: String,
    pub stdout_preview: String,
    pub stderr_preview: String,
}

/// Per-job run limits, stored in the `ac:tests:<job>` hash under these field names, except
/// `args`, which Go stores as JSON in the hash field `args_json`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    /// 0 keeps the language default.
    pub cpu_ns: u64,
    pub mem_bytes: u64,
    pub stack_bytes: u64,
    pub procs: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
}

/// A queued unit of work. The Go engine queues only the submission id and keeps the rest in
/// Redis (`ac:tests:<job>`); `tests_key` names that hash and `expires_at` is its deadline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub job_id: String,
    pub submission_id: String,
    pub language: String,
    pub source: String,
    pub limits: Limits,
    pub checker: Checker,
    pub tests_key: String,
    /// Unix milliseconds; `None` means no deadline.
    pub expires_at: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verdict_wire_strings_match_go() {
        let cases = [
            (Verdict::Accepted, "accepted"),
            (Verdict::WrongAnswer, "wrong_answer"),
            (Verdict::TimeLimit, "time_limit"),
            (Verdict::MemoryLimit, "memory_limit"),
            (Verdict::RuntimeError, "runtime_error"),
            (Verdict::CompilationError, "compilation_error"),
            (Verdict::InternalError, "internal_error"),
            (Verdict::OutputLimit, "output_limit"),
            (Verdict::Cancelled, "cancelled"),
        ];
        for (v, s) in cases {
            assert_eq!(serde_json::to_string(&v).unwrap(), format!("\"{s}\""));
            assert_eq!(
                serde_json::from_str::<Verdict>(&format!("\"{s}\"")).unwrap(),
                v
            );
        }
    }

    #[test]
    fn status_and_checker_wire_strings() {
        assert_eq!(
            serde_json::to_string(&Status::Completed).unwrap(),
            "\"completed\""
        );
        assert_eq!(serde_json::to_string(&Checker::Float).unwrap(), "\"float\"");
        assert_eq!(Checker::from_name(""), Checker::Exact);
        assert_eq!(Checker::from_name("custom"), Checker::Exact);
        assert_eq!(Checker::from_name("token"), Checker::Token);
    }

    #[test]
    fn job_round_trips_with_go_field_names() {
        let j = Job {
            job_id: "j".into(),
            submission_id: "s".into(),
            language: "python".into(),
            source: "print(1)".into(),
            limits: Limits {
                cpu_ns: 1,
                mem_bytes: 2,
                stack_bytes: 0,
                procs: 0,
                args: vec![],
            },
            checker: Checker::Token,
            tests_key: "ac:tests:j".into(),
            expires_at: None,
        };
        let s = serde_json::to_string(&j).unwrap();
        assert_eq!(
            s,
            r#"{"job_id":"j","submission_id":"s","language":"python","source":"print(1)","limits":{"cpu_ns":1,"mem_bytes":2,"stack_bytes":0,"procs":0},"checker":"token","tests_key":"ac:tests:j","expires_at":null}"#
        );
        assert_eq!(serde_json::from_str::<Job>(&s).unwrap(), j);
    }
}
