use crate::{Queue, Result};
use ae_core::eventlog::Event;
use base64::Engine;
use futures_util::StreamExt;
use rand::RngCore;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;

const EVENT_PREFIX: &str = "ac:events:";
pub(crate) const EVENT_LOG_TTL: Duration = Duration::from_secs(10 * 60);
pub(crate) const EVENT_LOG_MAX: isize = 500;
const STREAM_TOKEN_PREFIX: &str = "ac:stoken:";
/// Matches the SSE max lifetime with margin (Go: 30m).
const STREAM_TOKEN_TTL: Duration = Duration::from_secs(30 * 60);
/// How many events one SSE client may be behind before it is cut loose to reconnect and
/// replay from the event log.
pub const SUBSCRIBER_BUFFER: usize = 16;

pub(crate) fn channel_name(job_id: &str) -> String {
    format!("{EVENT_PREFIX}{job_id}")
}

pub(crate) fn log_key(job_id: &str) -> String {
    format!("{EVENT_PREFIX}{job_id}:log")
}

/// Appends the event-log and publish commands Go's `PublishEvent` sends.
pub(crate) fn push_event(pipe: &mut redis::Pipeline, ev: &Event) {
    let data = ev.to_json();
    let key = log_key(&ev.job_id);
    pipe.cmd("RPUSH").arg(&key).arg(&data).ignore();
    pipe.cmd("LTRIM")
        .arg(&key)
        .arg(-EVENT_LOG_MAX)
        .arg(-1)
        .ignore();
    pipe.cmd("EXPIRE")
        .arg(&key)
        .arg(EVENT_LOG_TTL.as_secs())
        .ignore();
    pipe.cmd("PUBLISH")
        .arg(channel_name(&ev.job_id))
        .arg(&data)
        .ignore();
}

type Subs = Arc<Mutex<HashMap<String, HashMap<u64, mpsc::Sender<Event>>>>>;

fn lock(
    subs: &Subs,
) -> std::sync::MutexGuard<'_, HashMap<String, HashMap<u64, mpsc::Sender<Event>>>> {
    subs.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The process-wide pattern subscription shared by every SSE client.
pub(crate) struct HubHandle {
    subs: Subs,
    next: Arc<AtomicU64>,
    stop: Arc<Notify>,
    task: JoinHandle<()>,
}

/// One SSE client's events. Dropping it unregisters the client. `recv` yields `None` when
/// the client fell behind, Redis went away or the hub stopped: reconnect and replay.
pub struct Subscription {
    rx: mpsc::Receiver<Event>,
    subs: Subs,
    job_id: String,
    id: u64,
}

impl Subscription {
    pub async fn recv(&mut self) -> Option<Event> {
        self.rx.recv().await
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let mut g = lock(&self.subs);
        if let Some(set) = g.get_mut(&self.job_id) {
            set.remove(&self.id);
            if set.is_empty() {
                g.remove(&self.job_id);
            }
        }
    }
}

fn dispatch(subs: &Subs, channel: &str, payload: &[u8]) {
    let Some(job_id) = channel.strip_prefix(EVENT_PREFIX) else {
        return;
    };
    if job_id.contains(':') {
        return;
    }
    let mut g = lock(subs);
    let Some(set) = g.get_mut(job_id) else {
        return;
    };
    let Ok(ev) = serde_json::from_slice::<Event>(payload) else {
        return;
    };
    set.retain(|_, tx| match tx.try_send(ev.clone()) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(_)) => {
            tracing::warn!(job_id, "sse: client fell behind, closing it to reconnect");
            false
        }
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    });
    if set.is_empty() {
        g.remove(job_id);
    }
}

impl Queue {
    /// Appends to `ac:events:<job>:log` (last 500, 10m TTL) and publishes on `ac:events:<job>`.
    ///
    /// # Errors
    /// Fails on a Redis error.
    pub async fn publish(&self, ev: &Event) -> Result<()> {
        let mut pipe = redis::pipe();
        push_event(&mut pipe, ev);
        pipe.query_async::<()>(&mut self.conn.clone()).await?;
        Ok(())
    }

    /// Every stored event for a job, oldest first; undecodable entries are skipped.
    ///
    /// # Errors
    /// Fails on a Redis error.
    pub async fn replay(&self, job_id: &str) -> Result<Vec<Event>> {
        let raw: Vec<Vec<u8>> = redis::cmd("LRANGE")
            .arg(log_key(job_id))
            .arg(0)
            .arg(-1)
            .query_async(&mut self.conn.clone())
            .await?;
        Ok(raw
            .iter()
            .filter_map(|v| serde_json::from_slice(v).ok())
            .collect())
    }

    /// Stores a fresh 20-byte base64url token at `ac:stoken:<job>` for 30 minutes.
    ///
    /// # Errors
    /// Fails on a Redis error.
    pub async fn issue_stream_token(&self, job_id: &str) -> Result<String> {
        let mut b = [0u8; 20];
        rand::rngs::OsRng.fill_bytes(&mut b);
        let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b);
        redis::cmd("SET")
            .arg(format!("{STREAM_TOKEN_PREFIX}{job_id}"))
            .arg(&token)
            .arg("EX")
            .arg(STREAM_TOKEN_TTL.as_secs())
            .query_async::<()>(&mut self.conn.clone())
            .await?;
        Ok(token)
    }

    /// Constant-time check of `token` against the stored one; false on any error (fail closed).
    pub async fn verify_stream_token(&self, job_id: &str, token: &str) -> bool {
        if token.is_empty() {
            return false;
        }
        let stored: redis::RedisResult<Option<String>> = redis::cmd("GET")
            .arg(format!("{STREAM_TOKEN_PREFIX}{job_id}"))
            .query_async(&mut self.conn.clone())
            .await;
        match stored {
            Ok(Some(s)) => {
                s.len() == token.len()
                    && s.bytes()
                        .zip(token.bytes())
                        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                        == 0
            }
            _ => false,
        }
    }

    /// Registers a client for `job_id`'s events. Registration completes before this
    /// returns, so no event published afterwards is missed. The first call opens the one
    /// shared `PSUBSCRIBE ac:events:*` connection and waits for Redis to confirm it.
    ///
    /// # Errors
    /// Fails when the shared subscription cannot be opened.
    pub async fn subscribe(&self, job_id: &str) -> Result<Subscription> {
        let mut hub = self.hub.lock().await;
        if hub.as_ref().is_none_or(|h| h.task.is_finished()) {
            *hub = Some(self.start_hub().await?);
        }
        let h = hub
            .as_ref()
            .ok_or_else(|| crate::Error::Corrupt("hub missing".into()))?;
        let (tx, rx) = mpsc::channel(SUBSCRIBER_BUFFER);
        let id = h.next.fetch_add(1, Ordering::Relaxed);
        lock(&h.subs)
            .entry(job_id.to_owned())
            .or_default()
            .insert(id, tx);
        Ok(Subscription {
            rx,
            subs: Arc::clone(&h.subs),
            job_id: job_id.to_owned(),
            id,
        })
    }

    async fn start_hub(&self) -> Result<HubHandle> {
        let mut ps = self.client.get_async_pubsub().await?;
        ps.psubscribe(format!("{EVENT_PREFIX}*")).await?;
        let subs: Subs = Arc::default();
        let stop = Arc::new(Notify::new());
        let (s, st) = (Arc::clone(&subs), Arc::clone(&stop));
        let task = tokio::spawn(async move {
            let mut msgs = ps.into_on_message();
            loop {
                tokio::select! {
                    () = st.notified() => break,
                    m = msgs.next() => match m {
                        Some(m) => dispatch(&s, m.get_channel_name(), m.get_payload_bytes()),
                        None => break,
                    },
                }
            }
            // Dropping every sender closes every client.
            lock(&s).clear();
        });
        Ok(HubHandle {
            subs,
            next: Arc::new(AtomicU64::new(0)),
            stop,
            task,
        })
    }

    /// Ends the shared subscription and waits for it; every client sees `None`.
    pub async fn stop_hub(&self) {
        let h = self.hub.lock().await.take();
        if let Some(h) = h {
            h.stop.notify_one();
            let _ = h.task.await;
        }
    }

    /// Registered clients for a job.
    pub async fn subscriber_count(&self, job_id: &str) -> usize {
        self.hub
            .lock()
            .await
            .as_ref()
            .map_or(0, |h| lock(&h.subs).get(job_id).map_or(0, HashMap::len))
    }
}
