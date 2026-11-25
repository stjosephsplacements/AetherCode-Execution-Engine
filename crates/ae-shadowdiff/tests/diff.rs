use ae_core::model::{DEFAULT_TENANT_ID, Status, Submission, TestResult, Verdict};
use ae_shadowdiff::{
    Outcome, ShadowRecord, Since, Summary, compare, since_filter, tests_gone_message,
};

fn tr(i: i32, v: Verdict, cpu: i64, mem: i64) -> TestResult {
    TestResult {
        test_index: i,
        verdict: v,
        cpu_time_ns: cpu,
        memory_bytes: mem,
        run_time_ns: cpu,
        exit_status: 0,
        sandbox_status: String::new(),
        message: String::new(),
        stdout_preview: String::new(),
        stderr_preview: String::new(),
    }
}

fn sub(status: Status, verdict: Option<Verdict>, count: i32, passed: i32) -> Submission {
    Submission {
        id: "j".into(),
        tenant_id: DEFAULT_TENANT_ID.into(),
        user_id: None,
        idempotency_key: None,
        language: "python".into(),
        source_code: String::new(),
        status,
        verdict,
        compile_stderr: String::new(),
        cpu_time_ns: 0,
        memory_bytes: 0,
        test_count: count,
        tests_passed: passed,
        created_at: 0,
        updated_at: 0,
    }
}

fn rec(v: Verdict, results: Vec<TestResult>, stderr: &str) -> ShadowRecord {
    let passed = results
        .iter()
        .filter(|r| r.verdict == Verdict::Accepted)
        .count();
    ShadowRecord {
        job_id: "j".into(),
        verdict: v,
        tests_passed: passed,
        test_count: results.len(),
        results,
        compile_stderr: stderr.into(),
        recorded_at: 0,
    }
}

#[test]
fn diff_reports_mismatched_verdict() {
    let go = vec![
        tr(0, Verdict::Accepted, 1, 1),
        tr(1, Verdict::WrongAnswer, 1, 1),
    ];
    let s = sub(Status::Completed, Some(Verdict::WrongAnswer), 2, 1);
    let r = rec(
        Verdict::Accepted,
        vec![
            tr(0, Verdict::Accepted, 1, 1),
            tr(1, Verdict::Accepted, 1, 1),
        ],
        "",
    );
    let Outcome::Diff(lines) = compare(&r, Some(&s), &go) else {
        panic!("expected diff");
    };
    assert!(
        lines
            .iter()
            .any(|l| l == "verdict: go=wrong_answer rust=accepted")
    );
    assert!(
        lines
            .iter()
            .any(|l| l == "test 1: go=wrong_answer rust=accepted")
    );
}

#[test]
fn diff_ignores_timing_fields() {
    let go = vec![tr(0, Verdict::Accepted, 5_000_000, 1000)];
    let s = sub(Status::Completed, Some(Verdict::Accepted), 1, 1);
    let r = rec(
        Verdict::Accepted,
        vec![tr(0, Verdict::Accepted, 9, 777_777)],
        "",
    );
    assert_eq!(compare(&r, Some(&s), &go), Outcome::Match);
}

#[test]
fn diff_ignores_tests_not_found() {
    // The Go engine finished the job; the shadow run found no tests hash.
    let s = sub(Status::Completed, Some(Verdict::Accepted), 1, 1);
    let r = rec(Verdict::InternalError, vec![], &tests_gone_message("j"));
    assert_eq!(compare(&r, Some(&s), &[]), Outcome::Ignored);
}

#[test]
fn compile_error_mentioning_tests_not_found_is_still_compared() {
    let s = sub(Status::Completed, Some(Verdict::Accepted), 1, 1);
    let go = [tr(0, Verdict::Accepted, 1, 1)];
    for msg in [
        "main.c:1:2: error: #error tests not found".to_owned(),
        format!("{}\n#error", tests_gone_message("j")),
    ] {
        let r = rec(Verdict::CompilationError, vec![], &msg);
        assert!(
            matches!(compare(&r, Some(&s), &go), Outcome::Diff(_)),
            "{msg}"
        );
    }
}

#[test]
fn unreadable_and_all_pending_fail_closed() {
    let u = Summary {
        unreadable: 1,
        ..Summary::default()
    };
    assert!(u.failed(false, 0));
    let mut p = Summary::default();
    p.add(&Outcome::Pending);
    assert!(p.failed(false, 0), "compared == 0 with pending must fail");
    let mut ok = Summary::default();
    ok.add(&Outcome::Match);
    ok.add(&Outcome::Pending);
    assert!(!ok.failed(false, 0));
    assert!(ok.failed(true, 0));
    assert!(!Summary::default().failed(true, 0));
}

#[test]
fn pending_is_not_a_diff() {
    let r = rec(Verdict::Accepted, vec![tr(0, Verdict::Accepted, 1, 1)], "");
    let running = sub(Status::Running, None, 0, 0);
    assert_eq!(compare(&r, Some(&running), &[]), Outcome::Pending);
    assert_eq!(compare(&r, None, &[]), Outcome::Pending);
    let mut sum = Summary::default();
    sum.add(&Outcome::Pending);
    assert_eq!(sum.diff, 0);
    assert_eq!(
        sum.line(),
        "compared=0 matched=0 diff=0 pending=1 ignored=0 unreadable=0 skipped_since=0"
    );
}

#[test]
fn empty_runs_fail_closed_unless_allowed() {
    let empty = Summary::default();
    assert!(empty.failed(false, 1), "nothing compared must fail");
    assert!(!empty.failed(false, 0), "--allow-empty");
    let mut ignored = Summary::default();
    ignored.add(&Outcome::Ignored);
    assert!(ignored.failed(false, 1), "all ignored must fail");
    let mut one = Summary::default();
    one.add(&Outcome::Match);
    assert!(!one.failed(false, 1));
    assert!(one.failed(false, 2), "--min-compared 2");
}

#[test]
fn since_counts_skips_and_rejects_missing_timestamps() {
    let mut r = rec(Verdict::Accepted, vec![], "");
    assert_eq!(since_filter(&r, None), Since::Keep);
    r.recorded_at = 0;
    assert_eq!(since_filter(&r, Some(1000)), Since::Unreadable);
    r.recorded_at = 999;
    assert_eq!(since_filter(&r, Some(1000)), Since::Skip);
    r.recorded_at = 1000;
    assert_eq!(since_filter(&r, Some(1000)), Since::Keep);
}
