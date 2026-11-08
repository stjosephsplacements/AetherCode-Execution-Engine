mod common;

use ae_core::eventlog::EventType;
use ae_core::model::{Checker, Job, Limits};
use ae_queue::{Delivery, MAX_DELIVERIES, RECLAIM_MIN_IDLE, TestSet};
use common::{env, env_with, go_enqueue, now_ms, pending};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

const SUBMIT: &str = "ac:submissions:submit";
const RUN: &str = "ac:submissions:run";
const BULK: &str = "ac:submissions:bulk";

fn job(id: &str, expires_at: Option<i64>) -> Job {
    Job {
        job_id: id.into(),
        submission_id: id.into(),
        language: "python".into(),
        source: String::new(),
        limits: Limits::default(),
        checker: Checker::Exact,
        tests_key: format!("ac:tests:{id}"),
        expires_at,
    }
}

fn one_test() -> TestSet {
    TestSet {
        tests: vec![ae_queue::TestData {
            input: "1".into(),
            expected_output: "1".into(),
            env: vec![],
        }],
        ..TestSet::default()
    }
}

fn ids(ds: &[Delivery]) -> Vec<&str> {
    ds.iter().map(|d| d.job_id.as_str()).collect()
}

#[tokio::test]
async fn connect_creates_groups_idempotently_and_enqueue_adds_entry() {
    let mut e = env().await;
    // A second connect must not fail on BUSYGROUP.
    ae_queue::Queue::connect(&common::cfg(&e.addr, &[]))
        .await
        .unwrap();
    e.q.enqueue(RUN, "job-1").await.unwrap();
    assert_eq!(e.q.depth(RUN).await.unwrap(), 1);
    let groups: Vec<redis::Value> = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(SUBMIT)
        .query_async(&mut e.rdb)
        .await
        .unwrap();
    assert_eq!(groups.len(), 1);
}

#[tokio::test]
async fn weighted_fair_queuing_delivers_three_submit_one_run() {
    let mut e = env().await;
    for i in 0..3 {
        go_enqueue(&mut e.rdb, SUBMIT, &format!("s{i}")).await;
    }
    go_enqueue(&mut e.rdb, RUN, "r0").await;
    let (tx, mut rx) = mpsc::channel(16);
    let q = e.q.clone();
    let h = tokio::spawn(async move { q.consume(tx).await });
    let mut got = Vec::new();
    for _ in 0..4 {
        let d = timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        got.push(d.job_id);
    }
    drop(rx);
    h.await.unwrap();
    assert_eq!(got, ["s0", "s1", "s2", "r0"]);
}

#[tokio::test]
async fn batched_read_returns_up_to_n_in_weight_order() {
    let mut e = env().await;
    for i in 0..5 {
        go_enqueue(&mut e.rdb, SUBMIT, &format!("s{i}")).await;
    }
    for i in 0..3 {
        go_enqueue(&mut e.rdb, RUN, &format!("r{i}")).await;
        go_enqueue(&mut e.rdb, BULK, &format!("b{i}")).await;
    }
    // Defaults: submit weight 3, run 1, bulk weight 1.
    assert_eq!(
        ids(&e.q.read_batch().await.unwrap()),
        ["s0", "s1", "s2", "r0", "b0"]
    );
    assert_eq!(
        ids(&e.q.read_batch().await.unwrap()),
        ["s3", "s4", "r1", "b1"]
    );
    assert_eq!(ids(&e.q.read_batch().await.unwrap()), ["r2", "b2"]);
    assert!(e.q.read_batch().await.unwrap().is_empty());
}

#[tokio::test]
async fn tiered_weights_come_from_config() {
    let mut e = env_with(&[("AC_SUBMIT_WEIGHT", "2"), ("AC_BULK_WEIGHT", "2")]).await;
    for i in 0..3 {
        go_enqueue(&mut e.rdb, SUBMIT, &format!("s{i}")).await;
        go_enqueue(&mut e.rdb, RUN, &format!("r{i}")).await;
        go_enqueue(&mut e.rdb, BULK, &format!("b{i}")).await;
    }
    let d = e.q.read_batch().await.unwrap();
    assert_eq!(ids(&d), ["s0", "s1", "r0", "b0", "b1"]);
    assert_eq!(d[0].stream, SUBMIT);
    assert_eq!(d[2].stream, RUN);
    assert_eq!(d[4].stream, BULK);
}

#[tokio::test]
async fn empty_streams_deliver_nothing() {
    let e = env().await;
    let (tx, mut rx) = mpsc::channel(4);
    let q = e.q.clone();
    let h = tokio::spawn(async move { q.consume(tx).await });
    assert!(
        timeout(Duration::from_millis(700), rx.recv())
            .await
            .is_err()
    );
    drop(rx);
    h.await.unwrap();
}

#[tokio::test]
async fn block_read_unblocks_on_new_message() {
    let mut e = env().await;
    let (tx, mut rx) = mpsc::channel(4);
    let q = e.q.clone();
    let h = tokio::spawn(async move { q.consume(tx).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    go_enqueue(&mut e.rdb, RUN, "unblock-me").await;
    let d = timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.job_id, "unblock-me");
    drop(rx);
    h.await.unwrap();
}

#[tokio::test]
async fn message_without_submission_id_is_acked_and_skipped() {
    let mut e = env().await;
    let _: String = redis::cmd("XADD")
        .arg(RUN)
        .arg("*")
        .arg("other")
        .arg("x")
        .query_async(&mut e.rdb)
        .await
        .unwrap();
    assert!(e.q.read_batch().await.unwrap().is_empty());
    assert_eq!(pending(&mut e.rdb, RUN).await, 0);
}

#[tokio::test]
async fn go_enqueued_message_without_expires_at_is_accepted() {
    let mut e = env().await;
    let msg_id = go_enqueue(&mut e.rdb, RUN, "go-job").await;
    let d = e.q.read_batch().await.unwrap();
    assert_eq!(
        d,
        [Delivery {
            stream: RUN.into(),
            msg_id,
            job_id: "go-job".into(),
            expires_at: None,
        }]
    );
}

/// Go's consumers read exactly `msg.Values["submission_id"].(string)` and ignore every other
/// field; this decodes the Rust-written entry the same way.
fn go_decode(fields: &HashMap<String, String>) -> Option<&str> {
    fields.get("submission_id").map(String::as_str)
}

#[tokio::test]
async fn rust_message_with_expires_at_is_ignored_by_go_decoder() {
    let mut e = env().await;
    let exp = now_ms() + 60_000;
    e.q.prepare_and_enqueue(&job("rust-job", Some(exp)), &one_test(), RUN)
        .await
        .unwrap();
    let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(RUN)
        .arg("-")
        .arg("+")
        .query_async(&mut e.rdb)
        .await
        .unwrap();
    assert_eq!(reply.ids.len(), 1);
    let fields: HashMap<String, String> = reply.ids[0]
        .map
        .iter()
        .map(|(k, v)| (k.clone(), redis::from_redis_value_ref(v).unwrap()))
        .collect();
    let mut keys: Vec<&str> = fields.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["expires_at", "submission_id"]);
    assert_eq!(fields["expires_at"], exp.to_string());
    assert_eq!(go_decode(&fields), Some("rust-job"));
    // And the Rust consumer surfaces the deadline.
    let d = e.q.read_batch().await.unwrap();
    assert_eq!(d[0].expires_at, Some(exp));
}

#[tokio::test]
async fn expired_job_is_dropped_acked_and_not_delivered() {
    let mut e = env().await;
    e.q.prepare_and_enqueue(&job("late", Some(now_ms() - 1_000)), &one_test(), RUN)
        .await
        .unwrap();
    e.q.prepare_and_enqueue(&job("fresh", Some(now_ms() + 60_000)), &one_test(), RUN)
        .await
        .unwrap();

    let (tx, mut rx) = mpsc::channel(4);
    let q = e.q.clone();
    let h = tokio::spawn(async move { q.consume(tx).await });
    let d = timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.job_id, "fresh");
    assert!(
        timeout(Duration::from_millis(500), rx.recv())
            .await
            .is_err(),
        "expired job must never be delivered"
    );
    drop(rx);
    h.await.unwrap();

    // Only the delivered (not yet acked) job is pending: the expired one was acked.
    assert_eq!(pending(&mut e.rdb, RUN).await, 1);
    let log = e.q.replay("late").await.unwrap();
    let last = log.last().unwrap();
    assert_eq!(last.kind, EventType::Verdict);
    assert!(last.is_terminal());
    assert_eq!(last.data.as_ref().unwrap()["verdict"], "internal_error");
    // The reclaimer never resurrects it.
    let (tx, mut rx) = mpsc::channel(4);
    e.q.reclaim_once(&tx, Duration::ZERO).await.unwrap();
    drop(tx);
    let mut again = Vec::new();
    while let Some(d) = rx.recv().await {
        again.push(d.job_id);
    }
    assert_eq!(again, ["fresh"]);
}

#[tokio::test]
async fn unacked_delivery_stays_pending_and_is_redelivered_after_reclaim() {
    let mut e = env().await;
    go_enqueue(&mut e.rdb, SUBMIT, "crashy").await;
    let first = e.q.read_batch().await.unwrap();
    assert_eq!(ids(&first), ["crashy"]);
    // Handler "crashed": no ack.
    assert_eq!(pending(&mut e.rdb, SUBMIT).await, 1);

    // Not idle long enough for the production threshold: nothing is reclaimed.
    let (tx, mut rx) = mpsc::channel(4);
    e.q.reclaim_once(&tx, RECLAIM_MIN_IDLE).await.unwrap();
    assert!(rx.try_recv().is_err());

    e.q.reclaim_once(&tx, Duration::ZERO).await.unwrap();
    let again = rx.try_recv().unwrap();
    assert_eq!(again.job_id, "crashy");
    assert_eq!(again.msg_id, first[0].msg_id);
    assert_eq!(pending(&mut e.rdb, SUBMIT).await, 1);

    e.q.ack_many(&[again]).await.unwrap();
    assert_eq!(pending(&mut e.rdb, SUBMIT).await, 0);
}

#[tokio::test]
async fn reclaimer_redelivers_after_30s_idle_and_dead_letters_after_5() {
    assert_eq!(RECLAIM_MIN_IDLE, Duration::from_secs(30));
    assert_eq!(MAX_DELIVERIES, 5);
    let mut e = env().await;
    let msg_id = go_enqueue(&mut e.rdb, SUBMIT, "poison").await;
    assert_eq!(e.q.read_batch().await.unwrap().len(), 1); // delivery 1

    let (tx, mut rx) = mpsc::channel(16);
    // Deliveries 2, 3 and 4 come from the reclaimer.
    for _ in 0..3 {
        e.q.reclaim_once(&tx, Duration::ZERO).await.unwrap();
        assert_eq!(rx.try_recv().unwrap().job_id, "poison");
    }
    assert_eq!(e.q.poisoned(), 0);
    // The fifth delivery is dead-lettered instead of handed out.
    e.q.reclaim_once(&tx, Duration::ZERO).await.unwrap();
    assert!(rx.try_recv().is_err());
    assert_eq!(e.q.poisoned(), 1);
    assert_eq!(pending(&mut e.rdb, SUBMIT).await, 0);

    let dlq: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg("ac:dlq:poison")
        .arg("-")
        .arg("+")
        .query_async(&mut e.rdb)
        .await
        .unwrap();
    assert_eq!(dlq.ids.len(), 1);
    let f = &dlq.ids[0];
    assert_eq!(f.get::<String>("submission_id").unwrap(), "poison");
    assert_eq!(f.get::<String>("source_stream").unwrap(), SUBMIT);
    assert_eq!(f.get::<String>("orig_msg_id").unwrap(), msg_id);
    assert_eq!(f.get::<String>("delivery_count").unwrap(), "5");
    assert!(f.get::<String>("reclaimed_at").unwrap().ends_with('Z'));
}

#[tokio::test]
async fn spawned_reclaimer_stops_when_receiver_drops() {
    let e = env().await;
    let (tx, rx) = mpsc::channel(4);
    let h = e.q.spawn_reclaimer(tx);
    drop(rx);
    timeout(Duration::from_secs(40), h).await.unwrap().unwrap();
}

#[tokio::test]
async fn legacy_stream_drains_once() {
    let mut e = env().await;
    for i in 0..5 {
        go_enqueue(&mut e.rdb, "ac:submissions", &format!("legacy-{i}")).await;
    }
    let (tx, mut rx) = mpsc::channel(16);
    let n = timeout(Duration::from_secs(10), e.q.drain_legacy(&tx))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 5);
    let mut got = Vec::new();
    while let Ok(d) = rx.try_recv() {
        assert_eq!(d.stream, "ac:submissions");
        got.push(d);
    }
    assert_eq!(
        ids(&got),
        ["legacy-0", "legacy-1", "legacy-2", "legacy-3", "legacy-4"]
    );
    e.q.ack_many(&got).await.unwrap();
    assert_eq!(pending(&mut e.rdb, "ac:submissions").await, 0);
    // A second drain finds nothing.
    assert_eq!(e.q.drain_legacy(&tx).await.unwrap(), 0);
}

#[tokio::test]
async fn trim_stale_removes_only_old_entries() {
    let mut e = env().await;
    let old = format!("{}-0", now_ms() - 200_000);
    let _: String = redis::cmd("XADD")
        .arg(SUBMIT)
        .arg(&old)
        .arg("submission_id")
        .arg("old")
        .query_async(&mut e.rdb)
        .await
        .unwrap();
    go_enqueue(&mut e.rdb, SUBMIT, "new").await;
    go_enqueue(&mut e.rdb, RUN, "new-run").await;
    assert_eq!(e.q.trim_stale(Duration::from_secs(90)).await.unwrap(), 1);
    assert_eq!(e.q.depth(SUBMIT).await.unwrap(), 1);
    assert_eq!(e.q.depth(RUN).await.unwrap(), 1);
    assert_eq!(ids(&e.q.read_batch().await.unwrap()), ["new", "new-run"]);
}

#[tokio::test]
async fn depth_and_oldest_age() {
    let mut e = env().await;
    assert_eq!(e.q.depth(BULK).await.unwrap(), 0);
    assert_eq!(e.q.oldest_age(BULK).await.unwrap(), Duration::ZERO);
    let old = format!("{}-0", now_ms() - 5_000);
    let _: String = redis::cmd("XADD")
        .arg(BULK)
        .arg(&old)
        .arg("submission_id")
        .arg("x")
        .query_async(&mut e.rdb)
        .await
        .unwrap();
    go_enqueue(&mut e.rdb, BULK, "y").await;
    assert_eq!(e.q.depth(BULK).await.unwrap(), 2);
    let age = e.q.oldest_age(BULK).await.unwrap();
    assert!(
        age >= Duration::from_secs(5) && age < Duration::from_secs(30),
        "{age:?}"
    );
}
