//! Compile cache. Ports compilecache_test.go plus the Rust-only LRU, size cap and
//! sandbox-file eviction.

mod common;

use ae_sandbox::{FileId, Sandbox};
use ae_worker::{CacheKey, CompileCache, Error};
use common::FakeSandbox;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

fn cache(sb: &Arc<FakeSandbox>, ttl: Duration, max_bytes: u64) -> CompileCache {
    CompileCache::new(sb.clone() as Arc<dyn Sandbox>, ttl, max_bytes)
}

fn key(src: &str) -> CacheKey {
    CacheKey::new(src, "cpp", &[], "g++ 13")
}

async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
}

#[test]
fn cache_key_is_deterministic_and_covers_every_input() {
    let base = CacheKey::new("int main(){}", "cpp", &["-O2".into()], "g++ 13");
    assert_eq!(
        base,
        CacheKey::new("int main(){}", "cpp", &["-O2".into()], "g++ 13")
    );
    assert_ne!(
        base,
        CacheKey::new("int main(){}", "c", &["-O2".into()], "g++ 13")
    );
    assert_ne!(
        base,
        CacheKey::new("int main(){ }", "cpp", &["-O2".into()], "g++ 13")
    );
    assert_ne!(
        base,
        CacheKey::new("int main(){}", "cpp", &["-O3".into()], "g++ 13")
    );
    let files = [("a.h".to_owned(), "x".to_owned())].into_iter().collect();
    assert_ne!(
        base,
        CacheKey::for_compile("int main(){}", "cpp", &files, &["-O2".into()], "g++ 13")
    );
    // Length prefixes keep field boundaries apart.
    assert_ne!(
        CacheKey::new("ab", "c", &[], ""),
        CacheKey::new("b", "ca", &[], "")
    );
    assert_eq!(base.as_str().len(), 64);
}

#[test]
fn cache_key_changes_with_toolchain_version() {
    assert_ne!(
        CacheKey::new("int main(){}", "cpp", &[], "g++ 13.2"),
        CacheKey::new("int main(){}", "cpp", &[], "g++ 14.1")
    );
}

#[tokio::test]
async fn hit_after_miss_and_ttl_expiry() {
    let sb = Arc::new(FakeSandbox::default());
    let c = cache(&sb, Duration::from_millis(50), 1 << 30);
    let (f, hit) = c
        .get_or_compile(key("a"), async { Ok(FileId("f1".into())) })
        .await
        .unwrap();
    assert!(!hit);
    assert_eq!(f.id(), &FileId("f1".into()));
    let (f2, hit) = c
        .get_or_compile(key("a"), async { Ok(FileId("never".into())) })
        .await
        .unwrap();
    assert!(hit);
    assert_eq!(f2.id(), &FileId("f1".into()));
    drop((f, f2));
    tokio::time::sleep(Duration::from_millis(80)).await;
    let (f3, hit) = c
        .get_or_compile(key("a"), async { Ok(FileId("f2".into())) })
        .await
        .unwrap();
    assert!(!hit, "an expired entry is a miss");
    assert_eq!(f3.id(), &FileId("f2".into()));
    settle().await;
    assert_eq!(*sb.deleted.lock().unwrap(), vec![FileId("f1".into())]);
}

#[tokio::test]
async fn sweep_evicts_expired_entries_and_deletes_their_files() {
    let sb = Arc::new(FakeSandbox::default());
    let c = cache(&sb, Duration::from_millis(20), 1 << 30);
    for (k, id) in [("a", "f1"), ("b", "f2")] {
        let _ = c
            .get_or_compile(key(k), async move { Ok(FileId(id.into())) })
            .await
            .unwrap();
    }
    assert_eq!(c.len(), 2);
    tokio::time::sleep(Duration::from_millis(40)).await;
    c.sweep();
    assert_eq!(c.len(), 0);
    settle().await;
    let mut deleted = sb.deleted.lock().unwrap().clone();
    deleted.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(deleted, vec![FileId("f1".into()), FileId("f2".into())]);
}

#[tokio::test]
async fn zero_ttl_compiles_every_time_and_releases_each_file() {
    let sb = Arc::new(FakeSandbox::default());
    let c = cache(&sb, Duration::ZERO, 1 << 30);
    let runs = AtomicUsize::new(0);
    for i in 0..3 {
        let (f, hit) = c
            .get_or_compile(key("k"), async {
                runs.fetch_add(1, Ordering::SeqCst);
                Ok(FileId(format!("f{i}")))
            })
            .await
            .unwrap();
        assert!(!hit, "a disabled cache reported a hit");
        drop(f);
    }
    assert_eq!(runs.load(Ordering::SeqCst), 3);
    settle().await;
    assert_eq!(sb.deleted.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn identical_concurrent_jobs_compile_once() {
    let sb = Arc::new(FakeSandbox::default());
    let c = Arc::new(cache(&sb, Duration::from_secs(60), 1 << 30));
    let runs = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let (c, runs, release) = (c.clone(), runs.clone(), release.clone());
        tasks.push(tokio::spawn(async move {
            let (f, hit) = c
                .get_or_compile(key("same"), async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    release.notified().await;
                    Ok(FileId("bin-1".into()))
                })
                .await
                .unwrap();
            (f.id().clone(), hit)
        }));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    release.notify_waiters();
    let mut leaders = 0;
    for t in tasks {
        let (id, hit) = t.await.unwrap();
        assert_eq!(id, FileId("bin-1".into()));
        leaders += usize::from(!hit);
    }
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(leaders, 1, "exactly one caller compiles");
    let (_, hit) = c
        .get_or_compile(key("same"), async { Ok(FileId("x".into())) })
        .await
        .unwrap();
    assert!(hit, "a later job hits the cache");
}

#[tokio::test]
async fn a_source_that_does_not_compile_is_shared_but_not_cached() {
    let sb = Arc::new(FakeSandbox::default());
    let c = Arc::new(cache(&sb, Duration::from_secs(60), 1 << 30));
    let runs = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let (c, runs, release) = (c.clone(), runs.clone(), release.clone());
        tasks.push(tokio::spawn(async move {
            c.get_or_compile(key("bad"), async move {
                runs.fetch_add(1, Ordering::SeqCst);
                release.notified().await;
                Err(Error::Compile("syntax error".into()))
            })
            .await
            .err()
        }));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    release.notify_waiters();
    for t in tasks {
        match t.await.unwrap() {
            Some(Error::Compile(s)) => assert_eq!(s, "syntax error"),
            other => panic!("want the shared compile failure, got {other:?}"),
        }
    }
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let (_, hit) = c
        .get_or_compile(key("bad"), async { Ok(FileId("fixed".into())) })
        .await
        .unwrap();
    assert!(!hit, "a failed compile must not be cached");
}

#[tokio::test]
async fn waiters_retry_when_the_leader_hits_a_transient_error() {
    let sb = Arc::new(FakeSandbox::default());
    let c = Arc::new(cache(&sb, Duration::from_secs(60), 1 << 30));
    let runs = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let leader = {
        let (c, runs, started, release) =
            (c.clone(), runs.clone(), started.clone(), release.clone());
        tokio::spawn(async move {
            c.get_or_compile(key("k"), async move {
                runs.fetch_add(1, Ordering::SeqCst);
                started.notify_one();
                release.notified().await;
                Err(Error::Other("sandbox unreachable".into()))
            })
            .await
            .map(|(f, _)| f.id().clone())
        })
    };
    started.notified().await;
    let waiter = {
        let (c, runs) = (c.clone(), runs.clone());
        tokio::spawn(async move {
            c.get_or_compile(key("k"), async move {
                runs.fetch_add(1, Ordering::SeqCst);
                Ok(FileId("bin-2".into()))
            })
            .await
            .map(|(f, _)| f.id().clone())
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    release.notify_waiters();
    assert!(
        leader.await.unwrap().is_err(),
        "the leader's error was swallowed"
    );
    assert_eq!(waiter.await.unwrap().unwrap(), FileId("bin-2".into()));
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_cancelled_leader_does_not_strand_waiters() {
    let sb = Arc::new(FakeSandbox::default());
    let c = Arc::new(cache(&sb, Duration::from_secs(60), 1 << 30));
    let leader = {
        let c = c.clone();
        tokio::spawn(async move {
            c.get_or_compile(key("k"), std::future::pending())
                .await
                .is_ok()
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    let waiter = {
        let c = c.clone();
        tokio::spawn(async move {
            c.get_or_compile(key("k"), async { Ok(FileId("mine".into())) })
                .await
                .map(|(f, _)| f.id().clone())
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    leader.abort();
    let got = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("waiter stranded")
        .unwrap()
        .unwrap();
    assert_eq!(got, FileId("mine".into()));
}

#[tokio::test]
async fn eviction_deletes_sandbox_file() {
    let sb = Arc::new(FakeSandbox::default());
    // Room for exactly one entry.
    let c = cache(&sb, Duration::from_secs(60), CompileCache::ENTRY_BYTES);
    let (a, _) = c
        .get_or_compile(key("a"), async { Ok(FileId("fa".into())) })
        .await
        .unwrap();
    drop(a);
    let (b, _) = c
        .get_or_compile(key("b"), async { Ok(FileId("fb".into())) })
        .await
        .unwrap();
    settle().await;
    assert_eq!(*sb.deleted.lock().unwrap(), vec![FileId("fa".into())]);
    assert_eq!(c.len(), 1);
    assert_eq!(b.id(), &FileId("fb".into()));
}

#[tokio::test]
async fn an_evicted_file_in_use_is_deleted_only_after_its_last_user() {
    let sb = Arc::new(FakeSandbox::default());
    let c = cache(&sb, Duration::from_secs(60), CompileCache::ENTRY_BYTES);
    let (a, _) = c
        .get_or_compile(key("a"), async { Ok(FileId("fa".into())) })
        .await
        .unwrap();
    let _b = c
        .get_or_compile(key("b"), async { Ok(FileId("fb".into())) })
        .await
        .unwrap();
    settle().await;
    assert!(
        sb.deleted.lock().unwrap().is_empty(),
        "deleted while in use"
    );
    drop(a);
    settle().await;
    assert_eq!(*sb.deleted.lock().unwrap(), vec![FileId("fa".into())]);
}

#[tokio::test]
async fn lru_keeps_the_recently_used_entry() {
    let sb = Arc::new(FakeSandbox::default());
    let c = cache(&sb, Duration::from_secs(60), 2 * CompileCache::ENTRY_BYTES);
    for (k, id) in [("a", "fa"), ("b", "fb")] {
        let _ = c
            .get_or_compile(key(k), async move { Ok(FileId(id.into())) })
            .await
            .unwrap();
    }
    // Touch "a" so "b" is the least recently used.
    let _ = c
        .get_or_compile(key("a"), async { Ok(FileId("x".into())) })
        .await
        .unwrap();
    let _ = c
        .get_or_compile(key("c"), async { Ok(FileId("fc".into())) })
        .await
        .unwrap();
    settle().await;
    assert_eq!(*sb.deleted.lock().unwrap(), vec![FileId("fb".into())]);
}
