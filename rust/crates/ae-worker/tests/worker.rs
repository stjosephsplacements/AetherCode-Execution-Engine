//! Job processing end to end against in-memory doubles: ordering, batching,
//! parallelism, idempotent redelivery, panics and shutdown.

mod common;

use ae_core::eventlog::EventType;
use ae_core::model::{Status, Verdict};
use ae_sandbox::{CopyIn, RunResult, STATUS_ACCEPTED};
use common::{FakeSandbox, Harness, delivery, id};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

const CPP: &str = "int main(){}";

#[tokio::test]
async fn order_is_db_then_verdict_event_then_ack() {
    let h = Harness::new().await;
    let j = id(1);
    h.job(&j, "cpp", CPP, &[("1", "1"), ("2", "2")]);
    h.worker().process(delivery(&j)).await.unwrap();
    let log = h.log();
    let tail: Vec<&str> = log.iter().map(String::as_str).rev().take(3).collect();
    assert_eq!(tail, ["ack", "event:VERDICT", "db:finish"], "{log:?}");
    let kinds: Vec<EventType> = h
        .bus
        .events
        .lock()
        .unwrap()
        .iter()
        .map(|e| e.kind)
        .collect();
    assert_eq!(
        kinds,
        [
            EventType::Compiling,
            EventType::Running,
            EventType::TestResult,
            EventType::Verdict
        ]
    );
    let row = h.store.row(&j);
    assert_eq!(row.status, Some(Status::Completed));
    assert_eq!(row.verdict, Some(Verdict::Accepted));
    let v = &h.bus.events_of(&j, EventType::Verdict)[0];
    let data = v.data.as_ref().unwrap();
    assert_eq!(data["verdict"], "accepted");
    assert_eq!(data["tests_passed"], 2);
    assert_eq!(data["test_count"], 2);
    assert_eq!(data["results"].as_array().unwrap().len(), 2);
    assert!(data["applied_cpu_limit_ns"].as_u64().unwrap() > 0);
    assert!(data["applied_memory_limit_bytes"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn cache_hit_skips_compile_and_runs_tests() {
    let h = Harness::new().await;
    let w = h.worker();
    for n in [1, 2] {
        h.job(&id(n), "cpp", CPP, &[("7", "7")]);
        w.process(delivery(&id(n))).await.unwrap();
        assert_eq!(h.store.row(&id(n)).verdict, Some(Verdict::Accepted));
    }
    assert_eq!(h.sb.compiles.load(Ordering::SeqCst), 1);
    let runs = h.sb.runs();
    assert_eq!(runs.len(), 2, "each job still runs its tests");
    let bin = |c: &ae_sandbox::Cmd| match &c.copy_in["/w/a.out"] {
        CopyIn::FileId(f) => f.clone(),
        CopyIn::Content(_) => panic!("binary inlined"),
    };
    assert_eq!(bin(&runs[0][0]), bin(&runs[1][0]));
}

#[tokio::test]
async fn identical_concurrent_jobs_compile_once() {
    let h = Harness::new().await;
    let w = h.worker();
    for n in 1..=5 {
        h.job(&id(n), "cpp", CPP, &[("1", "1")]);
    }
    let gate = h.sb.gate.write().await;
    let mut tasks = Vec::new();
    for n in 1..=5 {
        let w = w.clone();
        tasks.push(tokio::spawn(
            async move { w.process(delivery(&id(n))).await },
        ));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(gate);
    for t in tasks {
        t.await.unwrap().unwrap();
    }
    assert_eq!(h.sb.compiles.load(Ordering::SeqCst), 1);
    for n in 1..=5 {
        assert_eq!(h.store.row(&id(n)).verdict, Some(Verdict::Accepted));
    }
}

#[tokio::test]
async fn compile_error_keeps_diagnostics() {
    let h = Harness::new().await;
    *h.sb.compile_fail.lock().unwrap() = Some("error[E0308]: mismatched types".into());
    let j = id(1);
    h.job(&j, "rust", "fn main(){", &[("1", "1")]);
    h.worker().process(delivery(&j)).await.unwrap();
    let row = h.store.row(&j);
    assert_eq!(row.status, Some(Status::Failed));
    assert_eq!(row.verdict, Some(Verdict::CompilationError));
    assert_eq!(row.stderr, "error[E0308]: mismatched types");
    let v = &h.bus.events_of(&j, EventType::Verdict)[0];
    let d = v.data.as_ref().unwrap();
    assert_eq!(d["verdict"], "compilation_error");
    assert_eq!(d["compile_stderr"], "error[E0308]: mismatched types");
    assert_eq!(h.bus.acked(&j), 1);
    assert!(h.sb.runs().is_empty());
}

#[tokio::test]
async fn zero_tests_job_gets_verdict() {
    let h = Harness::new().await;
    let j = id(1);
    h.job(&j, "cpp", CPP, &[]);
    h.worker().process(delivery(&j)).await.unwrap();
    let row = h.store.row(&j);
    assert_eq!(row.status, Some(Status::Failed));
    assert_eq!(row.verdict, Some(Verdict::InternalError));
    assert_eq!(row.stderr, "no tests found");
    assert_eq!(h.bus.events_of(&j, EventType::Verdict).len(), 1);
    assert_eq!(h.bus.acked(&j), 1);
}

#[tokio::test]
async fn thirty_three_tests_batch_into_32_plus_1_in_order() {
    let h = Harness::new().await;
    let j = id(1);
    let inputs: Vec<String> = (0..33).map(|i| format!("{i}")).collect();
    // Test 32 expects a different answer, so a misplaced result shows up.
    let mut tests: Vec<(&str, &str)> = inputs.iter().map(|s| (s.as_str(), s.as_str())).collect();
    tests[32].1 = "nope";
    h.job(&j, "cpp", CPP, &tests);
    h.worker().process(delivery(&j)).await.unwrap();

    let mut sizes: Vec<usize> = h.sb.runs().iter().map(Vec::len).collect();
    sizes.sort_unstable();
    assert_eq!(sizes, [1, 32]);
    let row = h.store.row(&j);
    assert_eq!(row.results.len(), 33);
    for (i, r) in row.results.iter().enumerate() {
        assert_eq!(r.test_index, i32::try_from(i).unwrap());
        assert_eq!(r.stdout_preview, inputs[i], "result {i} out of order");
        let want = if i == 32 {
            Verdict::WrongAnswer
        } else {
            Verdict::Accepted
        };
        assert_eq!(r.verdict, want, "test {i}");
    }
    assert_eq!(row.verdict, Some(Verdict::WrongAnswer));
    let progress = h.bus.events_of(&j, EventType::TestResult);
    assert_eq!(progress.len(), 2);
    let last = progress[1].data.as_ref().unwrap();
    assert_eq!(last["completed"], 33);
    assert_eq!(last["total"], 33);
}

#[tokio::test]
async fn all_tests_of_job_run_in_parallel_bounded_by_language_hint() {
    let h = Harness::new().await;
    *h.sb.run_delay.lock().unwrap() = Duration::from_millis(50);
    let tests: Vec<(&str, &str)> = (0..20).map(|_| ("x", "x")).collect();

    // python: hint 8, so at most 8 tests are in the sandbox at once.
    h.job(&id(1), "python", "print(1)", &tests);
    h.worker().process(delivery(&id(1))).await.unwrap();
    assert_eq!(h.sb.max_in_flight.load(Ordering::SeqCst), 8);
    assert!(h.sb.runs().iter().all(|b| b.len() <= 8));
    assert_eq!(h.store.row(&id(1)).results.len(), 20);

    // c: no hint, so one batch of 32 is in flight per job; 40 tests take two rounds.
    h.sb.reset();
    let tests: Vec<(&str, &str)> = (0..40).map(|_| ("x", "x")).collect();
    h.job(&id(2), "c", "int main(){}", &tests);
    h.worker().process(delivery(&id(2))).await.unwrap();
    assert_eq!(h.sb.max_in_flight.load(Ordering::SeqCst), 32);
    assert_eq!(h.store.row(&id(2)).results.len(), 40);
}

#[test]
fn job_width_is_bounded_without_hint() {
    use ae_worker::runner::{MAX_JOB_WIDTH, job_width};
    assert_eq!(job_width(None, 1000), MAX_JOB_WIDTH);
    assert_eq!(job_width(None, 5), 5);
    assert_eq!(job_width(Some(8), 1000), 8);
    assert_eq!(job_width(Some(8), 3), 3);
    assert_eq!(job_width(None, 0), 1);
}

#[tokio::test]
async fn job_cost_matches_actual_concurrency() {
    use ae_worker::runner::{job_cost, job_width};
    let h = Harness::new().await;
    *h.sb.run_delay.lock().unwrap() = Duration::from_millis(5);
    let limits = ae_sandbox::RunLimits::default();
    for (n, lang) in [(1000usize, "c"), (20, "python")] {
        h.sb.reset();
        let l = h.registry.get(lang).unwrap();
        let width = job_width(l.parallelism, n);
        let (_, mem) = l.effective(&limits);
        let cost = job_cost(l, &limits, n);
        assert_eq!(cost.cpu_millis, 1_000 * width as u64);
        assert_eq!(cost.mem_bytes, mem * width as u64);
        let tests: Vec<(&str, &str)> = (0..n).map(|_| ("x", "x")).collect();
        let j = id(u32::try_from(n).unwrap());
        h.job(&j, lang, "src", &tests);
        h.worker().process(delivery(&j)).await.unwrap();
        assert!(
            h.sb.max_in_flight.load(Ordering::SeqCst) <= width,
            "{lang}: exceeded width"
        );
        assert_eq!(h.store.row(&j).results.len(), n);
    }
}

#[tokio::test(start_paused = true)]
async fn custom_checker_calls_respect_the_job_deadline() {
    let h = Harness::new().await;
    let j = id(1);
    h.job_with(
        &j,
        "python",
        "print(1)",
        &[("a", "a"), ("b", "b"), ("c", "c")],
        |s| {
            s.limits.cpu_ns = 100_000_000;
            s.set.checker_code = "import sys".into();
            s.set.checker_language = "python".into();
        },
    );
    // Each sandbox call takes 9s of virtual time; three sequential checker calls
    // overrun the job budget (3 x 1.2s + 15s).
    *h.sb.run_delay.lock().unwrap() = Duration::from_secs(9);
    let mut opts = h.options();
    opts.job_timeout = Duration::from_secs(1);
    let start = tokio::time::Instant::now();
    h.worker_with(opts).process(delivery(&j)).await.unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(25),
        "ran past the deadline: {:?}",
        start.elapsed()
    );
    let row = h.store.row(&j);
    assert!(
        row.results
            .iter()
            .any(|r| r.verdict == Verdict::InternalError),
        "{:?}",
        row.results
    );
}

#[tokio::test]
async fn interactive_sandbox_error_is_reported_in_the_result() {
    let h = Harness::new().await;
    let j = id(1);
    h.job_with(&j, "python", "print(42)", &[("10", "42")], |s| {
        s.set.interactor_code = "int main(){}".into();
        s.set.interactor_language = "cpp".into();
    });
    h.sb.piped_fail.store(true, Ordering::SeqCst);
    h.worker().process(delivery(&j)).await.unwrap();
    let r = &h.store.row(&j).results[0];
    assert_eq!(r.verdict, Verdict::InternalError);
    assert!(
        r.message.starts_with("interactive run failed"),
        "{:?}",
        r.message
    );
}

#[tokio::test]
async fn interpreted_source_is_uploaded_once_for_many_tests() {
    let h = Harness::new().await;
    let j = id(1);
    h.job(&j, "python", "print(input())", &[("1", "1"), ("2", "2")]);
    h.worker().process(delivery(&j)).await.unwrap();
    let cached = h.sb.cached.lock().unwrap().clone();
    assert_eq!(
        cached,
        vec![("main.py".to_owned(), "print(input())".to_owned())]
    );
    let runs = h.sb.runs();
    assert!(
        runs[0].iter().all(
            |c| matches!(&c.copy_in["/w/main.py"], CopyIn::FileId(f) if f.starts_with("src-"))
        )
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        h.sb.deleted.lock().unwrap().len(),
        1,
        "the uploaded source is released"
    );
}

#[tokio::test]
async fn per_test_env_is_forwarded_and_not_shared() {
    let h = Harness::new().await;
    let j = id(1);
    h.job_with(
        &j,
        "python",
        "print('ok')",
        &[("1", "1"), ("2", "2")],
        |s| {
            s.set.tests[0].env = vec!["TZ=UTC".into(), "AC_MODE=strict".into()];
        },
    );
    h.worker().process(delivery(&j)).await.unwrap();
    let runs = h.sb.runs();
    assert_eq!(runs.len(), 1);
    let base = h.registry.get("python").unwrap().run.env.clone();
    let mut want = base.clone();
    want.extend(["TZ=UTC".to_owned(), "AC_MODE=strict".to_owned()]);
    assert_eq!(runs[0][0].env, want);
    assert_eq!(runs[0][1].env, base);
}

#[tokio::test]
async fn interactive_job_runs_each_test_with_pipes() {
    let h = Harness::new().await;
    let j = id(1);
    h.job_with(&j, "python", "print(42)", &[("10", "42")], |s| {
        s.set.interactor_code = "int main(){}".into();
        s.set.interactor_language = "cpp".into();
    });
    h.sb.set_hook(|_| {
        Some(RunResult {
            status: STATUS_ACCEPTED.into(),
            ..RunResult::default()
        })
    });
    h.worker().process(delivery(&j)).await.unwrap();
    let piped = h.sb.piped.lock().unwrap().clone();
    assert_eq!(piped.len(), 1);
    assert_eq!(piped[0].0.len(), 2);
    assert_eq!(piped[0].1.len(), 2);
    let row = h.store.row(&j);
    assert_eq!(row.verdict, Some(Verdict::Accepted));
    assert_eq!(row.results.len(), 1);
}

#[tokio::test]
async fn custom_checker_runs_after_an_accepted_output() {
    let h = Harness::new().await;
    let j = id(1);
    h.job_with(&j, "python", "print(1)", &[("a", "a"), ("b", "b")], |s| {
        s.set.checker_code = "import sys".into();
        s.set.checker_language = "python".into();
    });
    // The checker rejects test b.
    h.sb.set_hook(|c| {
        let user = c.copy_in.get("/w/user_output.txt")?;
        let ok = matches!(user, CopyIn::Content(s) if s == "a");
        let mut r = RunResult {
            status: if ok {
                STATUS_ACCEPTED
            } else {
                ae_sandbox::STATUS_NONZERO_EXIT
            }
            .into(),
            exit_status: i32::from(!ok),
            ..RunResult::default()
        };
        r.files.insert("stderr".into(), "nope".into());
        Some(r)
    });
    h.worker().process(delivery(&j)).await.unwrap();
    let row = h.store.row(&j);
    let v: Vec<Verdict> = row.results.iter().map(|r| r.verdict).collect();
    assert_eq!(v, [Verdict::Accepted, Verdict::WrongAnswer]);
    assert_eq!(row.verdict, Some(Verdict::WrongAnswer));
}

#[tokio::test]
async fn crash_after_db_write_before_ack_redelivery_is_idempotent() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    let h = Harness::new().await;
    let j = id(1);
    let url = server.uri();
    h.job_with(&j, "cpp", CPP, &[("1", "1")], |s| {
        s.set.callback_url = url;
        s.set.callback_secret = "s3cret".into();
    });
    let w = h.worker();
    h.bus.fail_next_ack.store(true, Ordering::SeqCst);
    assert!(
        w.process(delivery(&j)).await.is_err(),
        "the crashed ack must surface"
    );
    assert_eq!(h.bus.acked(&j), 0);

    // Redelivery: the verdict is already persisted.
    w.process(delivery(&j)).await.unwrap();
    assert_eq!(h.bus.acked(&j), 1);
    assert_eq!(h.store.finishes.load(Ordering::SeqCst), 1);
    assert_eq!(h.store.row(&j).results.len(), 1);
    assert_eq!(
        h.bus.events_of(&j, EventType::Verdict).len(),
        1,
        "no second VERDICT"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    server.verify().await;
}

#[tokio::test]
async fn webhook_fires_only_when_the_delivery_marker_is_first_set() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;
    let h = Harness::new().await;
    let j = id(1);
    let url = server.uri();
    h.job_with(&j, "cpp", CPP, &[("1", "1")], |s| s.set.callback_url = url);
    h.store
        .rows
        .lock()
        .unwrap()
        .get_mut(&j)
        .unwrap()
        .webhook_marked = true;
    h.worker().process(delivery(&j)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    server.verify().await;
}

#[tokio::test]
async fn already_finished_submission_is_acked_without_work() {
    let h = Harness::new().await;
    let j = id(1);
    h.job(&j, "cpp", CPP, &[("1", "1")]);
    h.store.rows.lock().unwrap().get_mut(&j).unwrap().status = Some(Status::Completed);
    h.worker().process(delivery(&j)).await.unwrap();
    assert_eq!(h.bus.acked(&j), 1);
    assert!(h.bus.events.lock().unwrap().is_empty());
    assert_eq!(h.sb.compiles.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unavailable_language_yields_compile_error_verdict() {
    let h = Harness::with_sandbox(FakeSandbox::with_missing(&["/usr/bin/g++"])).await;
    let j = id(1);
    h.job(&j, "cpp", CPP, &[("1", "1")]);
    h.worker().process(delivery(&j)).await.unwrap();
    let row = h.store.row(&j);
    assert_eq!(row.status, Some(Status::Failed));
    assert_eq!(row.verdict, Some(Verdict::CompilationError));
    assert_eq!(row.stderr, "language unavailable: cpp");
    assert_eq!(h.bus.events_of(&j, EventType::Verdict).len(), 1);
    assert_eq!(h.bus.acked(&j), 1);

    // A language the table does not know is an internal error, as in Go.
    let k = id(2);
    h.job(&k, "cobol", "x", &[("1", "1")]);
    h.worker().process(delivery(&k)).await.unwrap();
    let row = h.store.row(&k);
    assert_eq!(row.verdict, Some(Verdict::InternalError));
    assert_eq!(row.stderr, "unknown language: cobol");
}

#[tokio::test]
async fn unknown_sandbox_status_is_an_internal_error() {
    let h = Harness::new().await;
    let j = id(1);
    h.job(&j, "cpp", CPP, &[("1", "1")]);
    h.sb.set_hook(|_| {
        Some(RunResult {
            status: "weird".into(),
            ..RunResult::default()
        })
    });
    h.worker().process(delivery(&j)).await.unwrap();
    assert_eq!(h.store.row(&j).verdict, Some(Verdict::InternalError));
}

#[tokio::test]
async fn panic_in_job_restarts_loop_after_backoff() {
    let h = Harness::new().await;
    let mut opts = h.options();
    opts.max_inflight = 1;
    let w = h.worker_with(opts);
    h.job(&id(1), "python", "print(1)", &[("1", "1")]);
    h.job(&id(2), "python", "print(1)", &[("1", "1")]);
    h.sb.panic_on_run.store(true, Ordering::SeqCst);
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(w.run(shutdown.clone()));
    let start = Instant::now();
    h.feed.send(delivery(&id(1))).await.unwrap();
    h.feed.send(delivery(&id(2))).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while h.bus.acked(&id(2)) == 0 {
        assert!(Instant::now() < deadline, "the loop died after a panic");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(start.elapsed() >= Duration::from_secs(1), "no 1s backoff");
    assert_eq!(h.bus.acked(&id(1)), 0, "a panicked job must stay unacked");
    assert_eq!(h.store.row(&id(2)).verdict, Some(Verdict::Accepted));
    shutdown.cancel();
    run.await.unwrap();
}

#[tokio::test]
async fn graceful_shutdown_finishes_in_flight_within_120s() {
    let h = Harness::new().await;
    assert_eq!(
        ae_worker::WorkerOptions::from_config(&common::config()).shutdown_grace,
        Duration::from_secs(120)
    );
    let w = h.worker();
    h.job(&id(1), "cpp", CPP, &[("1", "1")]);
    let gate = h.sb.gate.write().await;
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(w.run(shutdown.clone()));
    h.feed.send(delivery(&id(1))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!run.is_finished(), "returned with a job in flight");
    drop(gate);
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("shutdown hung")
        .unwrap();
    assert_eq!(h.bus.acked(&id(1)), 1);
    assert_eq!(h.store.row(&id(1)).verdict, Some(Verdict::Accepted));
}

#[tokio::test]
async fn shutdown_gives_up_on_jobs_after_the_grace_period() {
    let h = Harness::new().await;
    let mut opts = h.options();
    opts.shutdown_grace = Duration::from_millis(200);
    let w = h.worker_with(opts);
    h.job(&id(1), "cpp", CPP, &[("1", "1")]);
    let _gate = h.sb.gate.write().await;
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(w.run(shutdown.clone()));
    h.feed.send(delivery(&id(1))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let start = Instant::now();
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), run)
        .await
        .expect("grace period ignored")
        .unwrap();
    assert!(start.elapsed() >= Duration::from_millis(200));
    assert_eq!(h.bus.acked(&id(1)), 0, "an abandoned job stays pending");
}

#[tokio::test(start_paused = true)]
async fn a_stalled_compile_times_out_clears_the_flight_and_later_jobs_retry() {
    let h = Harness::new().await;
    for n in 1..=3 {
        h.job(&id(n), "cpp", CPP, &[("1", "1")]);
    }
    h.sb.compile_hang.store(true, Ordering::SeqCst);
    let w = h.worker();
    // Job 1 leads the stalled compile; job 2 waits on the same key.
    let (a, b) = tokio::join!(w.process(delivery(&id(1))), async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        w.process(delivery(&id(2))).await
    });
    a.unwrap();
    b.unwrap();
    let first = h.store.row(&id(1));
    assert_eq!(first.verdict, Some(Verdict::InternalError));
    assert!(
        first.stderr.contains("compile timed out"),
        "{:?}",
        first.stderr
    );
    assert_eq!(
        h.store.row(&id(2)).verdict,
        Some(Verdict::Accepted),
        "the waiter was stranded"
    );
    // A later job with the same key compiles (from cache now) and succeeds.
    w.process(delivery(&id(3))).await.unwrap();
    assert_eq!(h.store.row(&id(3)).verdict, Some(Verdict::Accepted));
    assert_eq!(
        h.sb.compiles.load(Ordering::SeqCst),
        2,
        "stalled call plus one retry"
    );
}

#[derive(Default)]
struct Recorder(std::sync::Mutex<Vec<String>>);

impl ae_core::observe::JobObserver for Recorder {
    fn job_started(&self) {
        self.0.lock().unwrap().push("started".into());
    }
    fn job_ended(&self) {
        self.0.lock().unwrap().push("ended".into());
    }
    fn job_finished(&self, language: &str, verdict: Verdict, _: Duration) {
        self.0
            .lock()
            .unwrap()
            .push(format!("finished:{language}:{verdict:?}"));
    }
    fn compiled(&self, language: &str, _: Duration, hit: bool) {
        self.0
            .lock()
            .unwrap()
            .push(format!("compiled:{language}:{hit}"));
    }
    fn stage(&self, stage: &str, _: Duration) {
        self.0.lock().unwrap().push(format!("stage:{stage}"));
    }
}

#[tokio::test]
async fn observer_sees_every_job_end_once() {
    let h = Harness::new().await;
    let rec = std::sync::Arc::new(Recorder::default());
    let mut opts = h.options();
    opts.observer = ae_core::observe::Observer(rec.clone());
    let w = h.worker_with(opts);
    h.job(&id(1), "cpp", CPP, &[("1", "1")]);
    w.process(delivery(&id(1))).await.unwrap();
    h.job(&id(2), "cpp", CPP, &[]);
    w.process(delivery(&id(2))).await.unwrap();
    // Redelivery of a finished job is skipped before the claim: no second end.
    w.process(delivery(&id(1))).await.unwrap();
    let got = rec.0.lock().unwrap().clone();
    assert_eq!(
        got,
        [
            "stage:claim",
            "started",
            "compiled:cpp:false",
            "stage:run",
            "stage:persist",
            "stage:publish",
            "finished:cpp:Accepted",
            "ended",
            "stage:claim",
            "started",
            "finished:cpp:InternalError",
            "ended",
        ]
    );
}
