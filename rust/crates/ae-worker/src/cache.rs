//! Content-addressed compile cache of go-judge file ids (Go: `compilecache.go`),
//! with singleflight, TTL, an LRU size cap and sandbox-file cleanup.

use crate::{Error, Result};
use ae_sandbox::{FileId, Sandbox};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// SHA-256 over the compile inputs, each field length-prefixed so boundaries
/// cannot shift between fields.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey(String);

impl CacheKey {
    /// Key of a compile with no extra files.
    pub fn new(source: &str, language: &str, flags: &[String], toolchain_version: &str) -> Self {
        Self::for_compile(source, language, &BTreeMap::new(), flags, toolchain_version)
    }

    /// Key of a compile with caller files (headers and the like), which change the binary.
    pub fn for_compile(
        source: &str,
        language: &str,
        files: &BTreeMap<String, String>,
        flags: &[String],
        toolchain_version: &str,
    ) -> Self {
        let mut h = Sha256::new();
        let mut put = |s: &str| {
            h.update((s.len() as u64).to_be_bytes());
            h.update(s.as_bytes());
        };
        put(language);
        put(source);
        for (name, content) in files {
            put(name);
            put(content);
        }
        put("\u{0}flags");
        for f in flags {
            put(f);
        }
        put(toolchain_version);
        Self(hex::encode(h.finalize()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Owns a go-judge file; deletes it when the last holder drops it.
struct FileRef {
    id: FileId,
    sandbox: Arc<dyn Sandbox>,
}

impl Drop for FileRef {
    fn drop(&mut self) {
        let (id, sb) = (self.id.clone(), self.sandbox.clone());
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move {
                if let Err(e) = sb.delete_file(id.clone()).await {
                    tracing::warn!(file_id = %id.0, error = %e, "compile cache: delete file");
                }
            });
        }
    }
}

/// A cached file in use. The sandbox file lives until the cache has evicted it
/// and every holder has dropped it.
#[derive(Clone)]
pub struct CachedFile(Arc<FileRef>);

impl CachedFile {
    /// A file owned only by its holders (deleted when they drop it).
    pub fn owned(id: FileId, sandbox: Arc<dyn Sandbox>) -> Self {
        Self(Arc::new(FileRef { id, sandbox }))
    }

    pub fn id(&self) -> &FileId {
        &self.0.id
    }
}

impl std::fmt::Debug for CachedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CachedFile").field(&self.0.id).finish()
    }
}

#[derive(Clone)]
enum Flight {
    Pending,
    Ready(CachedFile),
    /// The source does not compile; shared with waiters, never cached.
    Failed(String),
    /// The leader failed for its own reasons (or was cancelled); waiters retry.
    Retry,
}

struct Entry {
    file: CachedFile,
    created: Instant,
    last_used: u64,
}

#[derive(Default)]
struct State {
    entries: HashMap<CacheKey, Entry>,
    inflight: HashMap<CacheKey, watch::Receiver<Flight>>,
    clock: u64,
}

struct Shared {
    sandbox: Arc<dyn Sandbox>,
    ttl: Duration,
    max_entries: usize,
    state: Mutex<State>,
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Compile cache. Cheap to clone; clones share entries.
#[derive(Clone)]
pub struct CompileCache {
    shared: Arc<Shared>,
}

enum Step {
    Hit(CachedFile),
    Lead(watch::Sender<Flight>),
    Wait(watch::Receiver<Flight>),
}

impl CompileCache {
    /// Size accounted per entry. go-judge does not report cached file sizes, so
    /// the `compile_cache_max_mb` cap is enforced as a count of entries of this
    /// size (a typical dynamically linked binary or bytecode bundle is well below it).
    pub const ENTRY_BYTES: u64 = 1 << 20;

    /// A zero `ttl` disables caching: every call compiles and the file is released after use.
    pub fn new(sandbox: Arc<dyn Sandbox>, ttl: Duration, max_bytes: u64) -> Self {
        let max_entries = usize::try_from(max_bytes / Self::ENTRY_BYTES)
            .unwrap_or(usize::MAX)
            .max(1);
        Self {
            shared: Arc::new(Shared {
                sandbox,
                ttl,
                max_entries,
                state: Mutex::new(State::default()),
            }),
        }
    }

    pub fn ttl(&self) -> Duration {
        self.shared.ttl
    }

    /// Number of cached entries.
    pub fn len(&self) -> usize {
        self.shared.lock().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the cached file for `key` (hit = true), or runs `compile` once for
    /// every caller that asks while it runs. Waiters share the leader's result
    /// (and report a hit); [`Error::Compile`] is shared but not cached; any other
    /// leader error makes each waiter try again.
    ///
    /// # Errors
    /// The compile's error.
    pub async fn get_or_compile<F>(&self, key: CacheKey, compile: F) -> Result<(CachedFile, bool)>
    where
        F: Future<Output = Result<FileId>>,
    {
        let sh = &self.shared;
        if sh.ttl.is_zero() {
            let id = compile.await?;
            return Ok((CachedFile::owned(id, sh.sandbox.clone()), false));
        }
        loop {
            let step = {
                let mut st = sh.lock();
                st.clock += 1;
                let now = st.clock;
                let fresh = match st.entries.get_mut(&key) {
                    Some(e) if e.created.elapsed() <= sh.ttl => {
                        e.last_used = now;
                        Some(e.file.clone())
                    }
                    Some(_) => {
                        st.entries.remove(&key);
                        None
                    }
                    None => None,
                };
                match fresh {
                    Some(f) => Step::Hit(f),
                    None => match st.inflight.get(&key) {
                        Some(rx) => Step::Wait(rx.clone()),
                        None => {
                            let (tx, rx) = watch::channel(Flight::Pending);
                            st.inflight.insert(key.clone(), rx);
                            Step::Lead(tx)
                        }
                    },
                }
            };
            match step {
                Step::Hit(f) => return Ok((f, true)),
                Step::Lead(tx) => return self.lead(key, tx, compile).await,
                Step::Wait(mut rx) => {
                    let outcome = match rx.wait_for(|f| !matches!(f, Flight::Pending)).await {
                        Ok(f) => f.clone(),
                        Err(_) => Flight::Retry,
                    };
                    match outcome {
                        Flight::Ready(f) => return Ok((f, true)),
                        Flight::Failed(stderr) => return Err(Error::Compile(stderr)),
                        Flight::Pending | Flight::Retry => {}
                    }
                }
            }
        }
    }

    async fn lead<F>(
        &self,
        key: CacheKey,
        tx: watch::Sender<Flight>,
        compile: F,
    ) -> Result<(CachedFile, bool)>
    where
        F: Future<Output = Result<FileId>>,
    {
        // Clears the flight even if this future is dropped mid-compile.
        let mut guard = FlightGuard {
            cache: self,
            key: &key,
            tx,
            armed: true,
        };
        let out = compile.await;
        guard.armed = false;
        match out {
            Ok(id) => {
                let file = CachedFile::owned(id, self.shared.sandbox.clone());
                self.insert(&key, file.clone());
                guard.tx.send_replace(Flight::Ready(file.clone()));
                Ok((file, false))
            }
            Err(Error::Compile(stderr)) => {
                self.shared.lock().inflight.remove(&key);
                guard.tx.send_replace(Flight::Failed(stderr.clone()));
                Err(Error::Compile(stderr))
            }
            Err(e) => {
                self.shared.lock().inflight.remove(&key);
                guard.tx.send_replace(Flight::Retry);
                Err(e)
            }
        }
    }

    fn insert(&self, key: &CacheKey, file: CachedFile) {
        let mut st = self.shared.lock();
        st.inflight.remove(key);
        st.clock += 1;
        let now = st.clock;
        st.entries.insert(
            key.clone(),
            Entry {
                file,
                created: Instant::now(),
                last_used: now,
            },
        );
        while st.entries.len() > self.shared.max_entries {
            let Some(oldest) = st
                .entries
                .iter()
                .filter(|(k, _)| *k != key)
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            st.entries.remove(&oldest);
        }
    }

    /// Evicts expired entries; their files are deleted once no job holds them.
    pub fn sweep(&self) {
        let ttl = self.shared.ttl;
        let evicted: Vec<Entry> = {
            let mut st = self.shared.lock();
            let expired: Vec<CacheKey> = st
                .entries
                .iter()
                .filter(|(_, e)| e.created.elapsed() > ttl)
                .map(|(k, _)| k.clone())
                .collect();
            expired
                .iter()
                .filter_map(|k| st.entries.remove(k))
                .collect()
        };
        // Dropped outside the lock.
        drop(evicted);
    }
}

struct FlightGuard<'a> {
    cache: &'a CompileCache,
    key: &'a CacheKey,
    tx: watch::Sender<Flight>,
    armed: bool,
}

impl Drop for FlightGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.cache.shared.lock().inflight.remove(self.key);
            self.tx.send_replace(Flight::Retry);
        }
    }
}
