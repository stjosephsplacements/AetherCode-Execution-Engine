//! Redis L2 compile cache (Go: `compilecache_l2_test.go`).

mod common;

use ae_core::observe::CompileCacheSource;
use ae_sandbox::{FileId, Sandbox};
use ae_worker::{CacheKey, CompileCache};
use common::FakeSandbox;
use redis::aio::ConnectionManager;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use testcontainers::{ImageExt, runners::AsyncRunner};
use testcontainers_modules::redis::Redis;

async fn redis_conn() -> (testcontainers::ContainerAsync<Redis>, ConnectionManager) {
    let c = Redis::default()
        .with_tag("7")
        .start()
        .await
        .expect("start redis:7");
    let port = c.get_host_port_ipv4(6379).await.expect("redis port");
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}/")).expect("redis client");
    let conn = client.get_connection_manager().await.expect("redis conn");
    (c, conn)
}

fn key(s: &str) -> CacheKey {
    CacheKey::new(s, "cpp", &[], "g++")
}

#[tokio::test]
async fn l1_hit() {
    let sb = Arc::new(FakeSandbox::default());
    let c = CompileCache::new(sb.clone() as Arc<dyn Sandbox>, Duration::from_secs(60), 1 << 30, None);
    let k = key("int main(){}");
    c.get_or_compile(k.clone(), async { Ok(FileId("l1".into())) })
        .await
        .unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let runs2 = runs.clone();
    let (f, source) = c
        .get_or_compile(k, async move {
            runs2.fetch_add(1, Ordering::SeqCst);
            Ok(FileId("never".into()))
        })
        .await
        .unwrap();
    assert_eq!(source, CompileCacheSource::L1);
    assert_eq!(f.id().0, "l1");
    assert_eq!(runs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn l2_hit_warms_l1() {
    let (_c, mut conn) = redis_conn().await;
    let sb = Arc::new(FakeSandbox::default());
    let cache = CompileCache::new(
        sb.clone() as Arc<dyn Sandbox>,
        Duration::from_secs(60),
        1 << 30,
        Some(conn.clone()),
    );
    let k = key("int main(){return 0;}");
    redis::cmd("SET")
        .arg(format!("ac:compile:{}", k.as_str()))
        .arg("l2-file")
        .query_async::<()>(&mut conn)
        .await
        .unwrap();
    let runs = Arc::new(AtomicUsize::new(0));
    let runs2 = runs.clone();
    let (f, source) = cache
        .get_or_compile(k.clone(), async move {
            runs2.fetch_add(1, Ordering::SeqCst);
            Ok(FileId("never".into()))
        })
        .await
        .unwrap();
    assert_eq!(source, CompileCacheSource::L2);
    assert_eq!(f.id().0, "l2-file");
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    let (_, source) = cache
        .get_or_compile(k, async { Ok(FileId("never".into())) })
        .await
        .unwrap();
    assert_eq!(source, CompileCacheSource::L1);
}

#[tokio::test]
async fn miss_sets_redis() {
    let (_c, mut conn) = redis_conn().await;
    let sb = Arc::new(FakeSandbox::default());
    let cache = CompileCache::new(
        sb.clone() as Arc<dyn Sandbox>,
        Duration::from_secs(60),
        1 << 30,
        Some(conn.clone()),
    );
    let k = key("fn main(){}");
    let (_, source) = cache
        .get_or_compile(k.clone(), async { Ok(FileId("new".into())) })
        .await
        .unwrap();
    assert_eq!(source, CompileCacheSource::Miss);
    let got: String = redis::cmd("GET")
        .arg(format!("ac:compile:{}", k.as_str()))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(got, "new");
}