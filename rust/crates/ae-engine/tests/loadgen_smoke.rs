//! `loadgen` end to end against the real engine binary with a fake go-judge. The sandbox
//! is a stub that answers instantly, so this measures engine overhead only (gateway, queue,
//! worker, Postgres, SSE), never sandbox or compile time. It proves the load generator
//! drives the API, reads verdicts and splits cold from warm.

mod common;

use ae_loadgen::{Config, run};
use common::{EngineOpts, FakeJudge, infra, spawn_engine};
use std::time::Duration;

#[tokio::test]
async fn loadgen_smoke_against_engine_with_stub_sandbox() {
    let infra = infra().await;
    let judge = FakeJudge::start(&[]).await;
    let e = spawn_engine(
        &infra,
        &EngineOpts {
            args: &[],
            judge_url: &judge.url,
            env: &[("AC_WORKERS", "4")],
        },
    );
    e.wait_ready().await;
    let report = run(Config {
        url: e.url.clone(),
        token: None,
        students: 20,
        languages: vec!["python".into()],
        duration: Duration::from_secs(5),
        ramp: Duration::from_secs(1),
        think_min: Duration::from_millis(50),
        think_max: Duration::from_millis(150),
        timeout: Duration::from_secs(30),
    })
    .await
    .unwrap();
    eprintln!("{}", report.table());
    assert!(report.total >= 20, "total {}", report.total);
    assert_eq!(report.lost, 0, "{:?}", report.errors);
    assert_eq!(report.completed, report.total);
    // The stub prints "hello" for every program, so templates expecting other output
    // are judged wrong_answer; only the transport and verdict wait are under test.
    assert!(report.verdicts.contains_key("wrong_answer"));
    // python has 4 distinct sources: at most 4 cold submissions, the rest warm.
    assert!((1..=4).contains(&report.verdict_cold.count));
    assert!(report.verdict_warm.count > 0);
    assert!(report.verdict.p50_ms.is_some());
}
