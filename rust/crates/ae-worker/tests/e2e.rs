//! Full path against real Postgres and Redis containers with a fake sandbox:
//! stream entry -> worker -> verdict row + results -> events -> XACK.

mod common;

use ae_core::eventlog::EventType;
use ae_core::model::{Checker, DEFAULT_TENANT_ID, Job, Limits, Status, Submission, Verdict};
use ae_db::Db;
use ae_queue::{Queue, TestData, TestSet};
use ae_sandbox::Sandbox;
use ae_worker::Worker;
use common::FakeSandbox;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::Redis;
use tokio_util::sync::CancellationToken;

async fn postgres() -> (ContainerAsync<Postgres>, String) {
    let c = Postgres::default()
        .with_user("aethercode")
        .with_password("aethercode")
        .with_db_name("aethercode_test")
        .with_tag("18")
        .start()
        .await
        .expect("start postgres:18 (is docker running?)");
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    (
        c,
        format!("postgres://aethercode:aethercode@127.0.0.1:{port}/aethercode_test"),
    )
}

async fn redis() -> (ContainerAsync<Redis>, String) {
    let mut attempt = 0;
    loop {
        attempt += 1;
        let c = Redis::default()
            .with_tag("7")
            .start()
            .await
            .expect("start redis:7");
        match c.get_host_port_ipv4(6379).await {
            Ok(p) => return (c, format!("127.0.0.1:{p}")),
            Err(e) if attempt < 5 => eprintln!("retrying redis container: {e}"),
            Err(e) => panic!("redis port: {e}"),
        }
    }
}

#[tokio::test]
async fn delivery_to_verdict_row_events_and_ack() {
    let ((_pg, db_url), (_rd, redis_addr)) = tokio::join!(postgres(), redis());
    let cfg = ae_core::Config::from_map(&HashMap::from([
        ("AC_DATABASE_URL".to_owned(), db_url),
        ("AC_REDIS_ADDR".to_owned(), redis_addr.clone()),
        ("AC_BLOCK_READ_TIMEOUT".to_owned(), "200ms".to_owned()),
    ]))
    .unwrap();
    let db = ae_db::connect(&cfg).await.unwrap();
    db.migrate().await.unwrap();
    let q = Queue::connect(&cfg, Arc::new(db.clone())).await.unwrap();

    let sid = uuid::Uuid::new_v4().to_string();
    db.insert_submission(&Submission {
        id: sid.clone(),
        tenant_id: DEFAULT_TENANT_ID.into(),
        user_id: None,
        idempotency_key: None,
        language: "cpp".into(),
        source_code: "int main(){}".into(),
        status: Status::Queued,
        verdict: None,
        compile_stderr: String::new(),
        cpu_time_ns: 0,
        memory_bytes: 0,
        test_count: 0,
        tests_passed: 0,
        created_at: 0,
        updated_at: 0,
    })
    .await
    .unwrap();
    let tests: Vec<TestData> = (0..33)
        .map(|i| TestData {
            input: format!("{i}"),
            expected_output: if i == 5 { "x".into() } else { format!("{i}") },
            env: Vec::new(),
        })
        .collect();
    let job = Job {
        job_id: sid.clone(),
        submission_id: sid.clone(),
        language: "cpp".into(),
        source: "int main(){}".into(),
        limits: Limits::default(),
        checker: Checker::Exact,
        tests_key: format!("ac:tests:{sid}"),
        expires_at: None,
    };
    let set = TestSet {
        tests,
        ..TestSet::default()
    };
    q.prepare_and_enqueue(&job, &set, q.run_stream())
        .await
        .unwrap();

    let sb = Arc::new(FakeSandbox::default());
    let registry = common::registry(&sb).await;
    let mut opts = ae_worker::WorkerOptions::from_config(&cfg);
    opts.max_inflight = 4;
    let store: Arc<Db> = Arc::new(db.clone());
    let bus: Arc<Queue> = Arc::new(q.clone());
    let w = Worker::new(store, bus, sb.clone() as Arc<dyn Sandbox>, registry, opts);
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(w.clone().run(shutdown.clone()));

    let deadline = Instant::now() + Duration::from_secs(20);
    let sub = loop {
        let s = db.get_submission(&sid).await.unwrap().unwrap();
        if s.status == Status::Completed {
            break s;
        }
        assert!(
            Instant::now() < deadline,
            "no verdict, status {:?}",
            s.status
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(sub.verdict, Some(Verdict::WrongAnswer));
    assert_eq!((sub.test_count, sub.tests_passed), (33, 32));

    let rows = db.get_test_results(&sid).await.unwrap();
    assert_eq!(rows.len(), 33);
    for (i, r) in rows.iter().enumerate() {
        assert_eq!(r.test_index, i32::try_from(i).unwrap());
        let want = if i == 5 {
            Verdict::WrongAnswer
        } else {
            Verdict::Accepted
        };
        assert_eq!(r.verdict, want);
    }

    // The ack follows the event; wait for the pending list to drain.
    let mut rdb = redis::Client::open(format!("redis://{redis_addr}/"))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    loop {
        let pending: redis::Value = redis::cmd("XPENDING")
            .arg(q.run_stream())
            .arg(&cfg.consumer_group)
            .query_async(&mut rdb)
            .await
            .unwrap();
        let count = match &pending {
            redis::Value::Array(v) => match v.first() {
                Some(redis::Value::Int(n)) => *n,
                _ => -1,
            },
            _ => -1,
        };
        if count == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "never acked: {pending:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let verdicts = |evs: &[ae_core::eventlog::Event]| {
        evs.iter().filter(|e| e.kind == EventType::Verdict).count()
    };
    let events = q.replay(&sid).await.unwrap();
    assert_eq!(verdicts(&events), 1);
    assert!(events.iter().any(|e| e.kind == EventType::Compiling));

    shutdown.cancel();
    run.await.unwrap();

    // A redelivery of the same message (crash after the DB write, before the ack) changes nothing.
    w.process(ae_queue::Delivery {
        stream: q.run_stream().to_owned(),
        msg_id: "0-1".into(),
        job_id: sid.clone(),
        expires_at: None,
    })
    .await
    .unwrap();
    assert_eq!(db.get_test_results(&sid).await.unwrap().len(), 33);
    assert_eq!(
        verdicts(&q.replay(&sid).await.unwrap()),
        1,
        "duplicate VERDICT"
    );
}
