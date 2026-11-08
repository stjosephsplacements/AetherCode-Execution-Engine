#![allow(dead_code)]

use ae_core::Config;
use ae_core::model::Verdict;
use ae_queue::{BoxFuture, ExpiryStore, Queue};
use redis::aio::MultiplexedConnection;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::redis::Redis;

pub struct Env {
    _c: ContainerAsync<Redis>,
    pub addr: String,
    pub q: Queue,
    pub store: Arc<FakeStore>,
    pub rdb: MultiplexedConnection,
}

pub fn cfg(addr: &str, extra: &[(&str, &str)]) -> Config {
    let mut m = HashMap::from([
        ("AC_REDIS_ADDR".to_owned(), addr.to_owned()),
        ("AC_BLOCK_READ_TIMEOUT".to_owned(), "200ms".to_owned()),
    ]);
    for (k, v) in extra {
        m.insert((*k).to_owned(), (*v).to_owned());
    }
    Config::from_map(&m).unwrap()
}

pub async fn env_with(extra: &[(&str, &str)]) -> Env {
    // Docker Desktop occasionally reports a fresh container's port as unmapped when many
    // start at once; a new container fixes it.
    let mut attempt = 0;
    let (c, port) = loop {
        attempt += 1;
        let c = Redis::default()
            .with_tag("7")
            .start()
            .await
            .expect("start redis:7 (is docker running?)");
        match c.get_host_port_ipv4(6379).await {
            Ok(p) => break (c, p),
            Err(e) if attempt < 5 => eprintln!("retrying redis container: {e}"),
            Err(e) => panic!("redis port: {e}"),
        }
    };
    let addr = format!("127.0.0.1:{port}");
    let store = Arc::new(FakeStore::default());
    let q = Queue::connect(&cfg(&addr, extra), store.clone())
        .await
        .unwrap();
    let rdb = redis::Client::open(format!("redis://{addr}/"))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    Env {
        _c: c,
        addr,
        q,
        store,
        rdb,
    }
}

pub async fn env() -> Env {
    env_with(&[]).await
}

pub fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

/// `XADD` with only Go's field, exactly as the Go engine enqueues.
pub async fn go_enqueue(rdb: &mut MultiplexedConnection, stream: &str, id: &str) -> String {
    redis::cmd("XADD")
        .arg(stream)
        .arg("*")
        .arg("submission_id")
        .arg(id)
        .query_async(rdb)
        .await
        .unwrap()
}

pub async fn pending(rdb: &mut MultiplexedConnection, stream: &str) -> usize {
    let v: redis::Value = redis::cmd("XPENDING")
        .arg(stream)
        .arg("workers")
        .query_async(rdb)
        .await
        .unwrap();
    match v {
        redis::Value::Array(items) => match items.first() {
            Some(redis::Value::Int(n)) => usize::try_from(*n).unwrap(),
            _ => 0,
        },
        _ => 0,
    }
}

/// Records expired jobs instead of writing Postgres (tests/reaper.rs covers the real Db).
#[derive(Default)]
pub struct FakeStore {
    pub failed: Mutex<Vec<(String, Verdict)>>,
}

impl ExpiryStore for FakeStore {
    fn fail_expired<'a>(
        &'a self,
        job_id: &'a str,
        verdict: Verdict,
        _message: &'a str,
    ) -> BoxFuture<'a, ae_queue::Result<bool>> {
        self.failed
            .lock()
            .unwrap()
            .push((job_id.to_owned(), verdict));
        Box::pin(async { Ok(true) })
    }
}
