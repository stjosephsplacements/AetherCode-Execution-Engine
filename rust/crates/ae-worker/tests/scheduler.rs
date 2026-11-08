//! Capacity scheduler: admission against a CPU/memory budget, FIFO order.

use ae_worker::{Cost, Scheduler};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn cost(cpu: u64, mem: u64) -> Cost {
    Cost {
        cpu_millis: cpu,
        mem_bytes: mem,
    }
}

#[tokio::test]
async fn scheduler_blocks_when_budget_exhausted_and_releases_on_drop() {
    let s = Scheduler::new(cost(2_000, 1 << 30));
    let a = s.acquire(cost(1_000, 256 << 20)).await;
    let b = s.acquire(cost(1_000, 256 << 20)).await;
    assert_eq!(s.available(), cost(0, 512 << 20));

    let s2 = s.clone();
    let mut third = tokio::spawn(async move { s2.acquire(cost(1_000, 1 << 20)).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut third)
            .await
            .is_err(),
        "admitted past the CPU budget"
    );
    drop(a);
    let c = tokio::time::timeout(Duration::from_secs(1), third)
        .await
        .expect("a released permit must admit the waiter")
        .unwrap();
    drop((b, c));
    assert_eq!(s.available(), s.budget());
}

#[tokio::test]
async fn memory_is_a_budget_too() {
    let s = Scheduler::new(cost(64_000, 1 << 30));
    let _a = s.acquire(cost(1_000, 768 << 20)).await;
    let s2 = s.clone();
    let waiter = tokio::spawn(async move { s2.acquire(cost(1_000, 512 << 20)).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiter.is_finished(), "admitted past the memory budget");
    waiter.abort();
}

#[tokio::test]
async fn an_oversized_job_is_clamped_and_runs_alone() {
    let s = Scheduler::new(cost(1_000, 1 << 20));
    let p = tokio::time::timeout(Duration::from_secs(1), s.acquire(cost(8_000, 1 << 30)))
        .await
        .expect("a job bigger than the node must still run");
    assert_eq!(s.available(), cost(0, 0));
    drop(p);
    assert_eq!(s.available(), s.budget());
}

#[tokio::test]
async fn waiters_are_admitted_in_fifo_order() {
    let s = Scheduler::new(cost(2_000, u64::MAX));
    let big = s.acquire(cost(2_000, 0)).await;
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    // A large waiter first, then small ones: the small ones must not jump the queue.
    for (i, cpu) in [(0, 2_000), (1, 2_000), (2, 1_000)] {
        let (s, order) = (s.clone(), order.clone());
        tasks.push(tokio::spawn(async move {
            let p = s.acquire(cost(cpu, 0)).await;
            order.lock().unwrap().push(i);
            tokio::time::sleep(Duration::from_millis(20)).await;
            drop(p);
        }));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(big);
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(*order.lock().unwrap(), vec![0, 1, 2]);
}

#[tokio::test]
async fn a_cancelled_waiter_neither_blocks_the_queue_nor_leaks_budget() {
    let s = Scheduler::new(cost(1_000, u64::MAX));
    let held = s.acquire(cost(1_000, 0)).await;
    let s2 = s.clone();
    let cancelled = tokio::spawn(async move { s2.acquire(cost(1_000, 0)).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    cancelled.abort();
    let s3 = s.clone();
    let next = tokio::spawn(async move { s3.acquire(cost(1_000, 0)).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(held);
    let p = tokio::time::timeout(Duration::from_secs(1), next)
        .await
        .expect("the queue is stuck behind a cancelled waiter")
        .unwrap();
    drop(p);
    assert_eq!(s.available(), s.budget());
}

#[test]
fn budget_is_derived_from_workers_and_memory() {
    let cfg = ae_core::Config::from_map(
        &[("AC_WORKERS".to_owned(), "3".to_owned())]
            .into_iter()
            .collect(),
    )
    .unwrap();
    let b = Scheduler::budget_for(&cfg);
    assert_eq!(b.cpu_millis, 3_000);
    assert!(b.mem_bytes > 0);
}
