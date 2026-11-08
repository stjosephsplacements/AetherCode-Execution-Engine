//! Custom checker, interactor and result shaping. Ports custom_checker_test.go,
//! interactor_test.go, verdict_test.go and budget_test.go.

mod common;

use ae_core::model::{Checker, TestResult, Verdict};
use ae_sandbox::{
    CopyIn, RunLimits, RunResult, STATUS_ACCEPTED, STATUS_NONZERO_EXIT, STATUS_SIGNALLED,
    STATUS_TIME_LIMIT_EXCEEDED, Sandbox,
};
use ae_worker::CompileCache;
use ae_worker::checker::{checker_cmd, evaluate_checker, prepare_program};
use ae_worker::interactor::interactive_cmds;
use ae_worker::runner::{job_budget, judge_result, verdict_results_for};
use common::FakeSandbox;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

async fn setup() -> (Arc<FakeSandbox>, Arc<ae_sandbox::Registry>, CompileCache) {
    let sb = Arc::new(FakeSandbox::default());
    let reg = common::registry(&sb).await;
    let cache = CompileCache::new(
        sb.clone() as Arc<dyn Sandbox>,
        Duration::from_secs(300),
        1 << 30,
    );
    (sb, reg, cache)
}

fn content(c: &CopyIn) -> &str {
    match c {
        CopyIn::Content(s) | CopyIn::FileId(s) => s,
    }
}

#[tokio::test]
async fn custom_checker_compiles_once_and_judges_by_exit_status() {
    let (sb, reg, cache) = setup().await;
    sb.set_hook(|c| {
        let user = c.copy_in.get("/w/user_output.txt")?;
        let mut r = RunResult::default();
        if content(user) == "valid answer\n" {
            r.status = STATUS_ACCEPTED.into();
        } else {
            r.status = STATUS_NONZERO_EXIT.into();
            r.exit_status = 1;
            r.files.insert(
                "stderr".into(),
                "WA: answer does not satisfy graph conditions".into(),
            );
        }
        Some(r)
    });
    let sbd: Arc<dyn Sandbox> = sb.clone();
    let p = prepare_program(
        &cache,
        &sbd,
        &reg,
        "checker",
        "cpp",
        "#include <iostream> ...",
    )
    .await
    .unwrap();
    assert!(p.file_id().is_some_and(|f| f.0.starts_with("bin-")));
    let again = prepare_program(
        &cache,
        &sbd,
        &reg,
        "checker",
        "cpp",
        "#include <iostream> ...",
    )
    .await
    .unwrap();
    assert_eq!(p.file_id(), again.file_id());
    assert_eq!(sb.compiles.load(std::sync::atomic::Ordering::SeqCst), 1);

    let (v, msg) = evaluate_checker(sb.as_ref(), &p, "10\n", "expected\n", "valid answer\n").await;
    assert_eq!(v, Verdict::Accepted, "{msg}");
    let (v, msg) = evaluate_checker(sb.as_ref(), &p, "10\n", "expected\n", "wrong answer\n").await;
    assert_eq!(v, Verdict::WrongAnswer);
    assert_eq!(msg, "WA: answer does not satisfy graph conditions");
}

#[tokio::test]
async fn checker_status_mapping() {
    let (sb, reg, cache) = setup().await;
    let sbd: Arc<dyn Sandbox> = sb.clone();
    let p = prepare_program(&cache, &sbd, &reg, "checker", "python", "import sys")
        .await
        .unwrap();
    for (status, exit, stderr, want, want_msg) in [
        (STATUS_ACCEPTED, 0, "", Verdict::Accepted, ""),
        (STATUS_ACCEPTED, 3, "odd", Verdict::WrongAnswer, "odd"),
        (
            STATUS_NONZERO_EXIT,
            2,
            "",
            Verdict::WrongAnswer,
            "checker exited with code 2",
        ),
        (
            STATUS_TIME_LIMIT_EXCEEDED,
            0,
            "",
            Verdict::InternalError,
            "checker timed out",
        ),
        (
            STATUS_SIGNALLED,
            0,
            "boom",
            Verdict::InternalError,
            "checker failure (Signalled): boom",
        ),
    ] {
        sb.set_hook(move |_| {
            let mut r = RunResult {
                status: status.into(),
                exit_status: exit,
                ..RunResult::default()
            };
            r.files.insert("stderr".into(), stderr.into());
            Some(r)
        });
        let (v, msg) = evaluate_checker(sb.as_ref(), &p, "i", "e", "a").await;
        assert_eq!((v, msg.as_str()), (want, want_msg), "{status} {exit}");
    }
}

#[tokio::test]
async fn interpreted_checker_is_uploaded_once_and_mounted_at_its_source_path() {
    let (sb, reg, cache) = setup().await;
    let sbd: Arc<dyn Sandbox> = sb.clone();
    let p = prepare_program(&cache, &sbd, &reg, "checker", "", "print(1)")
        .await
        .unwrap();
    assert_eq!(p.language.id, "python", "an empty language means python");
    let cmd = checker_cmd(&p, "in", "exp", "out");
    let src = p.language.placement.source.clone().unwrap();
    assert!(matches!(&cmd.copy_in[&src], CopyIn::FileId(id) if id.starts_with("src-")));
    assert_eq!(content(&cmd.copy_in["/w/input.txt"]), "in");
    assert_eq!(content(&cmd.copy_in["/w/user_output.txt"]), "out");
    assert_eq!(content(&cmd.copy_in["/w/expected_output.txt"]), "exp");
    assert_eq!(
        &cmd.args[cmd.args.len() - 3..],
        [
            "/w/input.txt",
            "/w/user_output.txt",
            "/w/expected_output.txt"
        ]
    );
    assert_eq!(
        (cmd.cpu_limit, cmd.memory_limit),
        (5_000_000_000, 512 << 20)
    );
    assert_eq!(sb.cached.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn unsupported_checker_language_is_an_error() {
    let (sb, reg, cache) = setup().await;
    let sbd: Arc<dyn Sandbox> = sb.clone();
    let err = prepare_program(&cache, &sbd, &reg, "checker", "brainfuck", "+")
        .await
        .err()
        .unwrap();
    assert_eq!(err.to_string(), "unsupported checker language: brainfuck");
}

#[tokio::test]
async fn interactor_compile_failure_is_an_error() {
    let (sb, reg, cache) = setup().await;
    *sb.compile_fail.lock().unwrap() = Some("bad interactor".into());
    let sbd: Arc<dyn Sandbox> = sb.clone();
    let err = prepare_program(&cache, &sbd, &reg, "interactor", "cpp", "x")
        .await
        .err()
        .unwrap();
    assert_eq!(
        err.to_string(),
        "interactor compilation failed: bad interactor"
    );
}

#[tokio::test]
async fn interactive_cmds_wire_bidirectional_pipes_and_keep_env_on_the_student() {
    let (sb, reg, cache) = setup().await;
    let sbd: Arc<dyn Sandbox> = sb.clone();
    let it = prepare_program(
        &cache,
        &sbd,
        &reg,
        "interactor",
        "cpp",
        "#include <iostream> ...",
    )
    .await
    .unwrap();
    let py = reg.get("python").unwrap();
    let env = vec!["TZ=UTC".to_owned()];
    let (cmds, pipes) = interactive_cmds(
        py,
        "print('interactive')",
        None,
        &RunLimits::default(),
        &BTreeMap::new(),
        &env,
        reg.output_caps(),
        &it,
        "input_secret\n",
        "answer\n",
    );
    assert_eq!(cmds.len(), 2);
    assert_eq!(pipes.len(), 2);
    assert_eq!((pipes[0].input.index, pipes[0].input.fd), (0, 1));
    assert_eq!((pipes[0].output.index, pipes[0].output.fd), (1, 0));
    assert_eq!((pipes[1].input.index, pipes[1].input.fd), (1, 1));
    assert_eq!((pipes[1].output.index, pipes[1].output.fd), (0, 0));
    assert!(pipes.iter().all(|p| p.proxy));

    let mut student_env = py.run.env.clone();
    student_env.push("TZ=UTC".into());
    assert_eq!(cmds[0].env, student_env);
    assert_eq!(cmds[0].files[0], Default::default());
    assert_eq!(cmds[0].files[1], Default::default());
    assert_eq!(cmds[0].files[2].name.as_deref(), Some("stderr"));

    let cpp = reg.get("cpp").unwrap();
    assert_eq!(cmds[1].env, cpp.run.env, "the interactor keeps its own env");
    assert_eq!(content(&cmds[1].copy_in["/w/input.txt"]), "input_secret\n");
    assert_eq!(content(&cmds[1].copy_in["/w/expected.txt"]), "answer\n");
    let bin = cpp.placement.binary.clone().unwrap();
    assert!(matches!(&cmds[1].copy_in[&bin], CopyIn::FileId(_)));
    assert_eq!(
        &cmds[1].args[cmds[1].args.len() - 2..],
        ["/w/input.txt", "/w/expected.txt"]
    );
    assert_eq!(cmds[1].copy_out, vec!["stderr".to_owned()]);
}

#[test]
fn judge_result_reports_exit_status_and_sandbox_message() {
    let mut r = RunResult {
        status: STATUS_SIGNALLED.into(),
        exit_status: 11,
        error: "signal: segmentation fault".into(),
        time: 5_000_000,
        memory: 4096,
        run_time: 9_000_000,
        ..RunResult::default()
    };
    r.files.insert("stderr".into(), "boom".into());
    let tr = judge_result(&r, 3, "", Checker::Exact);
    assert_eq!(tr.verdict, Verdict::RuntimeError);
    assert_eq!((tr.test_index, tr.exit_status), (3, 11));
    assert_eq!(tr.sandbox_status, STATUS_SIGNALLED);
    assert_eq!(tr.message, "signal: segmentation fault");
    let e = &verdict_results_for(&[tr], "")[0];
    assert_eq!(e["exit_status"], 11);
    assert_eq!(e["sandbox_status"], STATUS_SIGNALLED);
    assert_eq!(e["message"], "signal: segmentation fault");
    assert_eq!(e["run_time_ns"], 9_000_000);
    assert_eq!(e["verdict"], "runtime_error");
}

#[test]
fn judge_result_status_mapping_and_checker() {
    let mk = |status: &str, out: &str| {
        let mut r = RunResult {
            status: status.into(),
            ..RunResult::default()
        };
        r.files.insert("stdout".into(), out.into());
        r
    };
    let j = |r: &RunResult, c| judge_result(r, 0, "1 2\n", c).verdict;
    assert_eq!(
        j(&mk(STATUS_ACCEPTED, "1 2"), Checker::Exact),
        Verdict::Accepted
    );
    assert_eq!(
        j(&mk(STATUS_ACCEPTED, "1  2"), Checker::Exact),
        Verdict::WrongAnswer
    );
    assert_eq!(
        j(&mk(STATUS_ACCEPTED, "1  2"), Checker::Token),
        Verdict::Accepted
    );
    assert_eq!(
        j(&mk(STATUS_TIME_LIMIT_EXCEEDED, ""), Checker::Exact),
        Verdict::TimeLimit
    );
    assert_eq!(
        j(
            &mk(ae_sandbox::STATUS_MEMORY_LIMIT_EXCEEDED, ""),
            Checker::Exact
        ),
        Verdict::MemoryLimit
    );
    assert_eq!(
        j(
            &mk(ae_sandbox::STATUS_OUTPUT_LIMIT_EXCEEDED, ""),
            Checker::Exact
        ),
        Verdict::OutputLimit
    );
    assert_eq!(
        j(&mk(STATUS_NONZERO_EXIT, ""), Checker::Exact),
        Verdict::RuntimeError
    );
    assert_eq!(
        j(&mk(ae_sandbox::STATUS_INTERNAL_ERROR, ""), Checker::Exact),
        Verdict::InternalError
    );
}

fn result_with(stdout: String, stderr: &str, message: String) -> TestResult {
    TestResult {
        test_index: 0,
        verdict: Verdict::Accepted,
        cpu_time_ns: 0,
        memory_bytes: 0,
        run_time_ns: 0,
        exit_status: 0,
        sandbox_status: String::new(),
        message,
        stdout_preview: stdout,
        stderr_preview: stderr.into(),
    }
}

#[test]
fn verdict_results_always_include_full_output_and_previews() {
    let e = &verdict_results_for(
        &[result_with("o".repeat(300), "warning", String::new())],
        "full",
    )[0];
    assert_eq!(e["stdout"].as_str().unwrap().len(), 300);
    assert_eq!(e["stdout_preview"].as_str().unwrap().len(), 256);
    assert_eq!(e["stderr"], "warning");
    assert_eq!(e["stderr_preview"], "warning");
}

#[test]
fn verdict_results_truncate_the_sandbox_message() {
    let e = &verdict_results_for(&[result_with(String::new(), "", "m".repeat(2000))], "")[0];
    assert_eq!(e["message"].as_str().unwrap().len(), 512);
}

#[test]
fn verdict_results_output_modes() {
    let r = [result_with("o".repeat(300), "warn", String::new())];
    let keys = |mode: &str| -> Vec<String> {
        let mut k: Vec<String> = verdict_results_for(&r, mode)[0]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        k.sort();
        k
    };
    let has = |k: &[String], s: &str| k.iter().any(|x| x == s);
    for mode in ["", "full"] {
        let k = keys(mode);
        for f in ["stdout", "stderr", "stdout_preview", "stderr_preview"] {
            assert!(has(&k, f), "mode {mode:?} dropped {f}");
        }
    }
    let k = keys("preview");
    assert!(!has(&k, "stdout") && !has(&k, "stderr"));
    assert!(has(&k, "stdout_preview") && has(&k, "stderr_preview"));
    let k = keys("none");
    for f in ["stdout", "stderr", "stdout_preview", "stderr_preview"] {
        assert!(!has(&k, f), "none mode kept {f}");
    }
    for mode in ["preview", "none"] {
        let k = keys(mode);
        for f in ["verdict", "cpu_time_ns", "exit_status", "test_index"] {
            assert!(has(&k, f), "mode {mode} lost {f}");
        }
    }
}

#[tokio::test]
async fn job_budget_covers_every_test_at_its_clock_limit() {
    let (_sb, reg, _cache) = setup().await;
    let java = reg.get("java").unwrap();
    let limits = RunLimits {
        cpu: 6_000_000_000,
        ..RunLimits::default()
    };
    // 20 tests x (2*6s+1s) + compile clock (2*cpu+1s) + 15s slack.
    let compile_cpu = java.compile.as_ref().unwrap().limits.cpu;
    let want = Duration::from_secs(20 * 13)
        + Duration::from_nanos(ae_sandbox::clock_limit(compile_cpu))
        + Duration::from_secs(15);
    assert_eq!(job_budget(java, &limits, 20), want);
}
