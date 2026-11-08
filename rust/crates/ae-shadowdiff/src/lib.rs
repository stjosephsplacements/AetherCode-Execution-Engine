//! Compares `ac:shadow:<job_id>` records (written by `engine --shadow`) with the
//! submission row the authoritative Go engine left in Postgres.
//!
//! Compared: the overall verdict, the passed/total test counts and each test's verdict.
//! Ignored: CPU time, memory, run time and output previews (they legitimately differ
//! between engines). Shadow records of jobs whose `ac:tests:<job>` hash had expired are
//! ignored too, and jobs the Go engine has not finished are pending, not diffs.

use ae_core::model::{Status, Submission, TestResult, Verdict};
use serde::Deserialize;

/// The fields of a shadow record this tool reads (see `ae-engine`'s `shadow.rs`).
#[derive(Debug, Deserialize)]
pub struct ShadowRecord {
    pub job_id: String,
    pub verdict: Verdict,
    pub tests_passed: usize,
    pub test_count: usize,
    #[serde(default)]
    pub results: Vec<TestResult>,
    #[serde(default)]
    pub compile_stderr: String,
    /// Unix milliseconds.
    pub recorded_at: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Match,
    /// One line per differing field.
    Diff(Vec<String>),
    /// The Go engine has no terminal result for the job yet (or no row at all).
    Pending,
    /// The shadow run had no tests to run, so there is nothing to compare.
    Ignored,
}

/// The message the worker records when a job's `ac:tests:<job>` hash is gone: the worker
/// wraps the queue's `no test count in <key>` error as `load tests: {error}`.
#[must_use]
pub fn tests_gone_message(job_id: &str) -> String {
    format!("load tests: queue: no test count in ac:tests:{job_id}")
}

/// True when the shadow run failed because the job's tests hash was already gone. Matches
/// the engine's own message exactly, so user compiler output can never trigger it.
#[must_use]
pub fn tests_gone(rec: &ShadowRecord) -> bool {
    rec.verdict == Verdict::InternalError
        && rec.results.is_empty()
        && rec.compile_stderr == tests_gone_message(&rec.job_id)
}

fn wire(v: Verdict) -> String {
    serde_json::to_value(v)
        .ok()
        .and_then(|x| x.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// Judges one shadow record against the Go engine's row and test results.
#[must_use]
pub fn compare(rec: &ShadowRecord, sub: Option<&Submission>, go: &[TestResult]) -> Outcome {
    if tests_gone(rec) {
        return Outcome::Ignored;
    }
    let Some(sub) = sub else {
        return Outcome::Pending;
    };
    if !matches!(sub.status, Status::Completed | Status::Failed) {
        return Outcome::Pending;
    }
    let mut d = Vec::new();
    let go_verdict = sub.verdict.map(wire).unwrap_or_default();
    if go_verdict != wire(rec.verdict) {
        d.push(format!(
            "verdict: go={go_verdict} rust={}",
            wire(rec.verdict)
        ));
    }
    let go_count = usize::try_from(sub.test_count).unwrap_or(0);
    let go_passed = usize::try_from(sub.tests_passed).unwrap_or(0);
    if go_count != rec.test_count || go_passed != rec.tests_passed {
        d.push(format!(
            "tests_passed/test_count: go={go_passed}/{go_count} rust={}/{}",
            rec.tests_passed, rec.test_count
        ));
    }
    let mut idx: Vec<i32> = rec.results.iter().chain(go).map(|r| r.test_index).collect();
    idx.sort_unstable();
    idx.dedup();
    for i in idx {
        let g = go
            .iter()
            .find(|r| r.test_index == i)
            .map(|r| wire(r.verdict));
        let r = rec
            .results
            .iter()
            .find(|r| r.test_index == i)
            .map(|r| wire(r.verdict));
        if g != r {
            d.push(format!(
                "test {i}: go={} rust={}",
                g.as_deref().unwrap_or("<missing>"),
                r.as_deref().unwrap_or("<missing>")
            ));
        }
    }
    if d.is_empty() {
        Outcome::Match
    } else {
        Outcome::Diff(d)
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Summary {
    pub compared: usize,
    pub matched: usize,
    pub diff: usize,
    pub pending: usize,
    pub ignored: usize,
    /// Shadow records that could not be parsed; nothing was compared for them.
    pub unreadable: usize,
    /// Records older than `--since`; not compared.
    pub skipped_since: usize,
}

impl Summary {
    pub fn add(&mut self, o: &Outcome) {
        match o {
            Outcome::Match => {
                self.compared += 1;
                self.matched += 1;
            }
            Outcome::Diff(_) => {
                self.compared += 1;
                self.diff += 1;
            }
            Outcome::Pending => self.pending += 1,
            Outcome::Ignored => self.ignored += 1,
        }
    }

    #[must_use]
    pub fn line(&self) -> String {
        format!(
            "compared={} matched={} diff={} pending={} ignored={} unreadable={} skipped_since={}",
            self.compared,
            self.matched,
            self.diff,
            self.pending,
            self.ignored,
            self.unreadable,
            self.skipped_since
        )
    }

    /// Fewer than `min_compared` records compared: parity cannot be claimed.
    #[must_use]
    pub fn too_few(&self, min_compared: usize) -> bool {
        self.compared < min_compared
    }

    /// Fail closed: a diff, an unreadable record, fewer than `min_compared` compared
    /// records (an empty or mis-pointed run), or a run that compared nothing while jobs
    /// were pending (for instance `--pg` connected as an RLS-limited role that sees no
    /// rows) is a failure; `fail_on_pending` makes any pending job one too.
    #[must_use]
    pub fn failed(&self, fail_on_pending: bool, min_compared: usize) -> bool {
        self.diff > 0
            || self.unreadable > 0
            || self.too_few(min_compared)
            || self.pending > 0 && (fail_on_pending || self.compared == 0)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Since {
    Keep,
    /// Older than the cutoff.
    Skip,
    /// No usable timestamp, so age cannot be judged.
    Unreadable,
}

/// Applies `--since` (`cutoff` in Unix ms) to a record; a record without a positive
/// `recorded_at` is unreadable rather than silently dropped.
#[must_use]
pub fn since_filter(rec: &ShadowRecord, cutoff: Option<i64>) -> Since {
    match cutoff {
        None => Since::Keep,
        Some(_) if rec.recorded_at <= 0 => Since::Unreadable,
        Some(c) if rec.recorded_at < c => Since::Skip,
        Some(_) => Since::Keep,
    }
}
