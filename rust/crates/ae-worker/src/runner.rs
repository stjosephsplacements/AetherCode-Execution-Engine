//! Test execution (Go: `runTestsBatched`, `judgeResult`, `verdictResultsFor`).
//!
//! Tests go to the sandbox in batches of at most [`MAX_BATCH`]. All batches of a
//! job run concurrently under a per-job semaphore sized by the language's
//! parallelism hint, so at most that many tests are in the sandbox at once.

use crate::checker::{Program, evaluate_checker};
use crate::interactor::interactive_cmds;
use crate::{Bus, Error, Result};
use ae_core::eventlog::{Event, EventType};
use ae_core::judge::judge_output;
use ae_core::model::{Checker, TestResult, Verdict};
use ae_db::truncate;
use ae_queue::TestData;
use ae_sandbox::{
    FileId, Language, OutputCaps, RunInput, RunLimits, RunResult, STATUS_ACCEPTED,
    STATUS_MEMORY_LIMIT_EXCEEDED, STATUS_NONZERO_EXIT, STATUS_OUTPUT_LIMIT_EXCEEDED,
    STATUS_SIGNALLED, STATUS_TIME_LIMIT_EXCEEDED, Sandbox, clock_limit, run_cmd,
};
use futures_util::future::try_join_all;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::Instant;

/// Most commands sent in one sandbox call (Go: `maxBatchSize`).
pub const MAX_BATCH: usize = 32;

/// Slack on top of the worst-case run time for sandbox and database round trips.
const BUDGET_SLACK: Duration = Duration::from_secs(15);

fn ns(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// Worst-case time a job needs: every test at its wall-clock limit plus the
/// compile, plus slack. Tests usually overlap, so this is a safe upper bound.
pub fn job_budget(lang: &Language, limits: &RunLimits, tests: usize) -> Duration {
    let (cpu, _) = lang.effective(limits);
    let mut total = clock_limit(cpu).saturating_mul(u64::try_from(tests).unwrap_or(u64::MAX));
    if let Some(c) = &lang.compile {
        total = total.saturating_add(clock_limit(c.limits.cpu));
    }
    Duration::from_nanos(total) + BUDGET_SLACK
}

fn base_result(r: &RunResult, test_index: i32) -> TestResult {
    TestResult {
        test_index,
        verdict: Verdict::InternalError,
        cpu_time_ns: ns(r.time),
        memory_bytes: ns(r.memory),
        run_time_ns: ns(r.run_time),
        exit_status: r.exit_status,
        sandbox_status: r.status.clone(),
        message: r.error.clone(),
        stdout_preview: r.files.get("stdout").cloned().unwrap_or_default(),
        stderr_preview: r.files.get("stderr").cloned().unwrap_or_default(),
    }
}

/// Verdict of a run that did not exit cleanly; `None` for an accepted run.
fn status_verdict(status: &str) -> Option<Verdict> {
    match status {
        STATUS_ACCEPTED => None,
        STATUS_TIME_LIMIT_EXCEEDED => Some(Verdict::TimeLimit),
        STATUS_MEMORY_LIMIT_EXCEEDED => Some(Verdict::MemoryLimit),
        STATUS_OUTPUT_LIMIT_EXCEEDED => Some(Verdict::OutputLimit),
        STATUS_NONZERO_EXIT | STATUS_SIGNALLED => Some(Verdict::RuntimeError),
        _ => Some(Verdict::InternalError),
    }
}

/// One sandbox result as a test result, judged against `expected` with `checker`.
pub fn judge_result(
    r: &RunResult,
    test_index: i32,
    expected: &str,
    checker: Checker,
) -> TestResult {
    let mut tr = base_result(r, test_index);
    tr.verdict = status_verdict(&r.status).unwrap_or_else(|| {
        if judge_output(checker, tr.stdout_preview.as_bytes(), expected.as_bytes()) {
            Verdict::Accepted
        } else {
            Verdict::WrongAnswer
        }
    });
    tr
}

fn internal_error(test_index: i32) -> TestResult {
    TestResult {
        test_index,
        verdict: Verdict::InternalError,
        cpu_time_ns: 0,
        memory_bytes: 0,
        run_time_ns: 0,
        exit_status: 0,
        sandbox_status: String::new(),
        message: String::new(),
        stdout_preview: String::new(),
        stderr_preview: String::new(),
    }
}

/// Per-test entries of the VERDICT event and webhook. `full` (or empty) carries
/// the whole output plus 256-byte previews, `preview` only the previews, `none`
/// no output text.
pub fn verdict_results_for(results: &[TestResult], mode: &str) -> Vec<Value> {
    results
        .iter()
        .map(|r| {
            let mut e = json!({
                "test_index": r.test_index,
                "verdict": r.verdict,
                "cpu_time_ns": r.cpu_time_ns,
                "memory_bytes": r.memory_bytes,
                "run_time_ns": r.run_time_ns,
                "exit_status": r.exit_status,
                "sandbox_status": r.sandbox_status,
                "message": truncate(&r.message, 512),
            });
            if let Value::Object(m) = &mut e {
                if mode != "none" {
                    m.insert(
                        "stdout_preview".into(),
                        truncate(&r.stdout_preview, 256).into(),
                    );
                    m.insert(
                        "stderr_preview".into(),
                        truncate(&r.stderr_preview, 256).into(),
                    );
                }
                if mode != "none" && mode != "preview" {
                    m.insert("stdout".into(), r.stdout_preview.clone().into());
                    m.insert("stderr".into(), r.stderr_preview.clone().into());
                }
            }
            e
        })
        .collect()
}

/// Everything a job's test run needs.
pub(crate) struct RunSpec<'a> {
    pub sandbox: &'a dyn Sandbox,
    pub bus: &'a dyn Bus,
    pub job_id: &'a str,
    pub lang: &'a Language,
    pub source: &'a str,
    pub file_id: Option<&'a FileId>,
    pub limits: &'a RunLimits,
    pub files: &'a BTreeMap<String, String>,
    pub caps: OutputCaps,
    pub checker: Checker,
    pub custom: Option<&'a Program>,
    pub interactor: Option<&'a Program>,
    pub deadline: Instant,
}

pub(crate) struct Outcome {
    pub results: Vec<TestResult>,
    pub verdict: Verdict,
}

fn index(i: usize) -> i32 {
    i32::try_from(i).unwrap_or(i32::MAX)
}

/// Runs every test and returns the results in test order. The overall verdict
/// is the first non-accepted verdict by test index. Tests whose batch starts
/// after the job deadline are internal errors.
///
/// # Errors
/// A sandbox failure or a deadline hit during a non-interactive batch.
pub(crate) async fn run_tests(spec: &RunSpec<'_>, tests: &[TestData]) -> Result<Outcome> {
    let hint = spec.lang.parallelism;
    let batch_size = hint.unwrap_or(MAX_BATCH).clamp(1, MAX_BATCH);
    let permits = hint.unwrap_or(tests.len()).max(batch_size);
    let sem = Semaphore::new(permits);
    let progress = Mutex::new((0usize, 0usize));
    let total = tests.len();

    let batches = tests.chunks(batch_size).enumerate().map(|(b, chunk)| {
        let (sem, progress) = (&sem, &progress);
        async move {
            let from = b * batch_size;
            let n = u32::try_from(chunk.len()).unwrap_or(u32::MAX);
            let _permit = sem
                .acquire_many(n)
                .await
                .map_err(|e| Error::Other(e.to_string()))?;
            let results = if Instant::now() >= spec.deadline {
                tracing::warn!(
                    job_id = spec.job_id,
                    from,
                    total,
                    "job deadline passed, skipping tests"
                );
                (from..from + chunk.len())
                    .map(|i| internal_error(index(i)))
                    .collect()
            } else if let Some(it) = spec.interactor {
                run_interactive(spec, it, from, chunk).await
            } else {
                run_batch(spec, from, chunk).await?
            };
            let passed_here = results
                .iter()
                .filter(|r: &&TestResult| r.verdict == Verdict::Accepted)
                .count();
            let (completed, passed) = {
                let mut p = progress
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                p.0 += chunk.len();
                p.1 += passed_here;
                *p
            };
            let ev = Event::new(
                spec.job_id,
                EventType::TestResult,
                Some(json!({
                    "completed": completed,
                    "total": total,
                    "passed": passed,
                    "batch_from": from,
                    "batch_to": from + chunk.len() - 1,
                })),
            );
            if let Err(e) = spec.bus.publish(&ev).await {
                tracing::warn!(error = %e, "failed to publish TEST_RESULT event");
            }
            Ok::<_, Error>(results)
        }
    });
    let results: Vec<TestResult> = try_join_all(batches).await?.into_iter().flatten().collect();
    let verdict = results
        .iter()
        .map(|r| r.verdict)
        .find(|v| *v != Verdict::Accepted)
        .unwrap_or(Verdict::Accepted);
    Ok(Outcome { results, verdict })
}

async fn run_batch(spec: &RunSpec<'_>, from: usize, chunk: &[TestData]) -> Result<Vec<TestResult>> {
    let to = from + chunk.len() - 1;
    let cmds = chunk
        .iter()
        .map(|tc| {
            run_cmd(
                spec.lang,
                &RunInput {
                    source: spec.source,
                    stdin: &tc.input,
                    file_id: spec.file_id,
                    limits: spec.limits.clone(),
                    extra_files: Some(spec.files),
                    env: &tc.env,
                },
                spec.caps,
            )
        })
        .collect();
    let out = tokio::time::timeout_at(spec.deadline, spec.sandbox.run(cmds))
        .await
        .map_err(|_| {
            Error::Other(format!(
                "batch run (tests {from}-{to}): job deadline exceeded"
            ))
        })?
        .map_err(|e| Error::Other(format!("batch run (tests {from}-{to}): {e}")))?;
    if out.len() != chunk.len() {
        return Err(Error::Other(format!(
            "batch run: expected {} results, got {}",
            chunk.len(),
            out.len()
        )));
    }
    let mut results = Vec::with_capacity(chunk.len());
    for (i, (r, tc)) in out.iter().zip(chunk).enumerate() {
        let mut tr = judge_result(r, index(from + i), &tc.expected_output, spec.checker);
        if tr.verdict == Verdict::Accepted
            && let Some(c) = spec.custom
        {
            let stdout = r.files.get("stdout").map_or("", String::as_str);
            tr.verdict = evaluate_checker(spec.sandbox, c, &tc.input, &tc.expected_output, stdout)
                .await
                .0;
        }
        results.push(tr);
    }
    Ok(results)
}

/// Interactive tests run one at a time within a batch; a failed call is an
/// internal error for that test only.
async fn run_interactive(
    spec: &RunSpec<'_>,
    it: &Program,
    from: usize,
    chunk: &[TestData],
) -> Vec<TestResult> {
    let mut results = Vec::with_capacity(chunk.len());
    for (i, tc) in chunk.iter().enumerate() {
        let idx = index(from + i);
        let (cmds, pipes) = interactive_cmds(
            spec.lang,
            spec.source,
            spec.file_id,
            spec.limits,
            spec.files,
            &tc.env,
            spec.caps,
            it,
            &tc.input,
            &tc.expected_output,
        );
        let out = tokio::time::timeout_at(spec.deadline, spec.sandbox.run_piped(cmds, pipes)).await;
        let rs = match out {
            Ok(Ok(rs)) if rs.len() >= 2 => rs,
            _ => {
                results.push(internal_error(idx));
                continue;
            }
        };
        let (student, interactor) = (&rs[0], &rs[1]);
        let mut tr = base_result(student, idx);
        tr.verdict = match status_verdict(&student.status) {
            Some(v) => v,
            None if interactor.status == STATUS_ACCEPTED && interactor.exit_status == 0 => {
                Verdict::Accepted
            }
            None => {
                tr.stderr_preview = interactor.files.get("stderr").cloned().unwrap_or_default();
                Verdict::WrongAnswer
            }
        };
        results.push(tr);
    }
    results
}
