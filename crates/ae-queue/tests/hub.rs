mod common;

use ae_core::eventlog::{Event, EventType};
use ae_queue::{SUBSCRIBER_BUFFER, Subscription};
use common::env;
use std::time::Duration;
use tokio::time::timeout;

async fn recv(s: &mut Subscription) -> Event {
    timeout(Duration::from_secs(3), s.recv())
        .await
        .expect("timed out waiting for event")
        .expect("subscription closed")
}

async fn numpat(rdb: &mut redis::aio::MultiplexedConnection) -> i64 {
    redis::cmd("PUBSUB")
        .arg("NUMPAT")
        .query_async(rdb)
        .await
        .unwrap()
}

#[tokio::test]
async fn one_redis_subscription_serves_every_client() {
    let mut e = env().await;
    let mut a1 = e.q.subscribe("job-a").await.unwrap();
    let mut a2 = e.q.subscribe("job-a").await.unwrap();
    let mut b = e.q.subscribe("job-b").await.unwrap();
    assert_eq!(numpat(&mut e.rdb).await, 1);

    e.q.publish(&Event::new("job-a", EventType::Running, None))
        .await
        .unwrap();
    assert_eq!(recv(&mut a1).await.job_id, "job-a");
    assert_eq!(recv(&mut a2).await.job_id, "job-a");
    assert!(
        timeout(Duration::from_millis(150), b.recv()).await.is_err(),
        "job-b received job-a's event"
    );
}

#[tokio::test]
async fn dropped_client_is_removed() {
    let e = env().await;
    let s = e.q.subscribe("job-c").await.unwrap();
    assert_eq!(e.q.subscriber_count("job-c").await, 1);
    drop(s);
    assert_eq!(e.q.subscriber_count("job-c").await, 0);
}

#[tokio::test]
async fn a_client_that_falls_behind_is_closed_so_it_replays() {
    let e = env().await;
    let mut slow = e.q.subscribe("job-d").await.unwrap();
    let mut fast = e.q.subscribe("job-e").await.unwrap();
    e.q.publish(&Event::new("job-e", EventType::Running, None))
        .await
        .unwrap();
    recv(&mut fast).await;
    for _ in 0..SUBSCRIBER_BUFFER + 5 {
        e.q.publish(&Event::new("job-d", EventType::Running, None))
            .await
            .unwrap();
    }
    let drained = timeout(Duration::from_secs(3), async {
        let mut n = 0;
        while slow.recv().await.is_some() {
            n += 1;
        }
        n
    })
    .await
    .expect("slow client's channel was never closed");
    assert_eq!(drained, SUBSCRIBER_BUFFER);
    e.q.publish(&Event::new("job-e", EventType::Running, None))
        .await
        .unwrap();
    recv(&mut fast).await;
}

#[tokio::test]
async fn stop_hub_closes_every_client() {
    let mut e = env().await;
    let mut s = e.q.subscribe("job-f").await.unwrap();
    timeout(Duration::from_secs(3), e.q.stop_hub())
        .await
        .unwrap();
    assert!(s.recv().await.is_none());
    // The server drops the pattern once it notices the closed connection.
    let mut n = numpat(&mut e.rdb).await;
    for _ in 0..20 {
        if n == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        n = numpat(&mut e.rdb).await;
    }
    assert_eq!(n, 0);
}
