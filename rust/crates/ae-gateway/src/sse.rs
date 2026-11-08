//! `GET /api/v1/stream` (port of `internal/gateway/sse.go`): stored events from
//! `ac:events:<id>:log` first, then live ones from the queue's single shared pub/sub
//! subscription, closing after VERDICT. The subscription lives in the response body
//! stream, so a client disconnect drops it at once.

use crate::auth::Identity;
use crate::handlers::{AppState, verify_token};
use ae_core::eventlog::{Event, EventType};
use ae_queue::Subscription;
use axum::Extension;
use axum::body::{Body, Bytes};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::time::Duration;
use tokio::time::{Instant, Interval, Sleep};

/// Longest a stream stays open; then a stored or synthetic VERDICT ends it.
pub const SSE_MAX_LIFETIME: Duration = Duration::from_secs(5 * 60);
/// Keepalive comment interval.
pub const SSE_HEARTBEAT: Duration = Duration::from_secs(15);
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);
const HEARTBEAT_FRAME: &[u8] = b": heartbeat\n\n";
const RECONNECT_FRAME: &[u8] = b": reconnect\n\n";

/// Go's `http.Error` with a pre-encoded JSON message: text/plain, nosniff, trailing newline.
fn plain_error(status: StatusCode, json: &str) -> Response {
    (
        status,
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            ),
            (
                header::X_CONTENT_TYPE_OPTIONS,
                HeaderValue::from_static("nosniff"),
            ),
        ],
        format!("{json}\n"),
    )
        .into_response()
}

fn parse_suffix(id: &str) -> (i64, i64) {
    let Some(i) = id.rfind(':') else {
        return (0, 0);
    };
    let suffix = &id[i + 1..];
    match suffix.split_once('-') {
        Some((e, s)) => (e.parse().unwrap_or(0), s.parse().unwrap_or(0)),
        None => (0, suffix.parse().unwrap_or(0)),
    }
}

/// Orders event ids `"<job>:<epoch>-<seq>"` (or legacy `"<job>:<seq>"`) by epoch, then seq.
#[must_use]
pub fn compare_event_ids(a: &str, b: &str) -> Ordering {
    parse_suffix(a).cmp(&parse_suffix(b))
}

fn not_after(id: &str, mark: &str) -> bool {
    !mark.is_empty() && compare_event_ids(id, mark) != Ordering::Greater
}

/// Live part of one stream.
struct Live {
    st: AppState,
    job_id: String,
    sub: Option<Subscription>,
    last_event_id: String,
    sent_up_to: String,
    deadline: Pin<Box<Sleep>>,
    heartbeat: Interval,
    done: bool,
}

impl Live {
    /// Skips events the client already has; records the newest one sent.
    fn admit(&mut self, ev: &Event) -> bool {
        if not_after(&ev.id, &self.sent_up_to) || not_after(&ev.id, &self.last_event_id) {
            return false;
        }
        self.sent_up_to.clone_from(&ev.id);
        true
    }

    async fn next_frame(&mut self) -> Option<Vec<u8>> {
        if self.done {
            return None;
        }
        let Some(mut sub) = self.sub.take() else {
            self.done = true;
            return Some(RECONNECT_FRAME.to_vec());
        };
        let frame = loop {
            tokio::select! {
                () = &mut self.deadline => {
                    self.done = true;
                    break self.deadline_frame().await;
                }
                _ = self.heartbeat.tick() => break Some(HEARTBEAT_FRAME.to_vec()),
                ev = sub.recv() => {
                    let Some(ev) = ev else {
                        // Redis went away or the client fell behind: tell it to reconnect
                        // with its Last-Event-ID; the log replays what it missed.
                        self.done = true;
                        break Some(RECONNECT_FRAME.to_vec());
                    };
                    if !self.admit(&ev) {
                        continue;
                    }
                    if ev.is_terminal() {
                        self.done = true;
                    }
                    break Some(ev.marshal_sse());
                }
            }
        };
        // Released as soon as the stream ends, not when the body is finally dropped.
        if !self.done {
            self.sub = Some(sub);
        }
        frame
    }

    /// Lifetime over: a terminal event that reached the log meanwhile, else the
    /// database verdict (the log expires after 10 min), else a synthetic
    /// `internal_error` VERDICT that is also persisted to the log.
    async fn deadline_frame(&mut self) -> Option<Vec<u8>> {
        let q = &self.st.queue;
        let latest = tokio::time::timeout(LOOKUP_TIMEOUT, q.replay(&self.job_id))
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or_default();
        if let Some(ev) = latest
            .iter()
            .find(|e| e.is_terminal() && !not_after(&e.id, &self.sent_up_to))
        {
            if not_after(&ev.id, &self.last_event_id) {
                return None;
            }
            return Some(ev.marshal_sse());
        }
        if let Ok(Ok(Some(sub))) =
            tokio::time::timeout(LOOKUP_TIMEOUT, self.st.db.get_submission(&self.job_id)).await
            && matches!(
                sub.status,
                ae_core::model::Status::Completed | ae_core::model::Status::Failed
            )
        {
            let verdict = sub
                .verdict
                .map(crate::metrics::verdict_label)
                .unwrap_or_default();
            let ev = Event::new(
                &self.job_id,
                EventType::Verdict,
                Some(serde_json::json!({
                    "verdict": verdict,
                    "tests_passed": sub.tests_passed,
                    "test_count": sub.test_count,
                })),
            );
            return Some(ev.marshal_sse());
        }
        let ev = Event::new(
            &self.job_id,
            EventType::Verdict,
            Some(serde_json::json!({
                "verdict": "internal_error",
                "error": "no verdict within timeout",
            })),
        );
        match tokio::time::timeout(LOOKUP_TIMEOUT, q.publish(&ev)).await {
            Ok(Ok(())) => {}
            _ => tracing::warn!(job_id = %self.job_id, "sse: failed to persist timeout event"),
        }
        Some(ev.marshal_sse())
    }
}

/// `GET /api/v1/stream?job_id=<uuid>[&stream_token=...]`.
pub async fn stream(
    State(st): State<AppState>,
    identity: Option<Extension<Identity>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let raw = query.get("job_id").map_or("", String::as_str);
    if raw.is_empty() {
        return plain_error(StatusCode::BAD_REQUEST, r#"{"error":"job_id is required"}"#);
    }
    let Ok(parsed) = uuid::Uuid::parse_str(raw) else {
        return plain_error(StatusCode::BAD_REQUEST, r#"{"error":"invalid job_id"}"#);
    };
    // The canonical form, never the raw query value, goes to Redis and Postgres.
    let job_id = parsed.to_string();
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();

    if let Some(Extension(id)) = identity {
        let sub = tokio::time::timeout(LOOKUP_TIMEOUT, st.db.get_submission(&job_id)).await;
        let owner = matches!(&sub, Ok(Ok(Some(s)))
            if s.tenant_id == id.tenant_id.to_string()
                && s.user_id.as_deref() == Some(id.user_id.to_string().as_str()));
        if !owner {
            return plain_error(StatusCode::NOT_FOUND, r#"{"error":"not found"}"#);
        }
    } else {
        let token = query.get("stream_token").map_or("", String::as_str);
        if !verify_token(&st.queue, &job_id, token).await {
            return plain_error(
                StatusCode::UNAUTHORIZED,
                r#"{"error":"invalid or missing stream_token"}"#,
            );
        }
    }

    // Subscribe before reading the log so nothing published in between is lost.
    let sub = match st.queue.subscribe(&job_id).await {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::error!(job_id, error = %e, "sse: subscribe failed");
            None
        }
    };
    let events = st.queue.replay(&job_id).await.unwrap_or_else(|e| {
        tracing::error!(job_id, error = %e, "sse: get event log failed");
        Vec::new()
    });
    let mut replay = Vec::new();
    let mut sent_up_to = String::new();
    let mut terminal = false;
    for ev in &events {
        if not_after(&ev.id, &last_event_id) {
            continue;
        }
        replay.extend_from_slice(&ev.marshal_sse());
        sent_up_to.clone_from(&ev.id);
        terminal |= ev.is_terminal();
    }

    let first: Option<Result<Bytes, Infallible>> =
        (!replay.is_empty()).then(|| Ok(Bytes::from(replay)));
    let body = if terminal {
        drop(sub);
        Body::from_stream(futures_util::stream::iter(first))
    } else {
        let live = Live {
            st,
            job_id,
            sub,
            last_event_id,
            sent_up_to,
            deadline: Box::pin(tokio::time::sleep(SSE_MAX_LIFETIME)),
            heartbeat: tokio::time::interval_at(Instant::now() + SSE_HEARTBEAT, SSE_HEARTBEAT),
            done: false,
        };
        let rest = futures_util::stream::unfold(live, |mut live| async move {
            let frame = live.next_frame().await?;
            Some((Ok::<_, Infallible>(Bytes::from(frame)), live))
        });
        Body::from_stream(futures_util::StreamExt::chain(
            futures_util::stream::iter(first),
            rest,
        ))
    };
    (
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/event-stream"),
            ),
            (header::CACHE_CONTROL, HeaderValue::from_static("no-cache")),
            (header::CONNECTION, HeaderValue::from_static("keep-alive")),
            (
                header::HeaderName::from_static("x-accel-buffering"),
                HeaderValue::from_static("no"),
            ),
        ],
        body,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_ids_order_by_epoch_then_seq() {
        assert_eq!(compare_event_ids("j:1-2", "j:1-10"), Ordering::Less);
        assert_eq!(compare_event_ids("j:2-1", "j:1-10"), Ordering::Greater);
        assert_eq!(compare_event_ids("j:1-3", "j:1-3"), Ordering::Equal);
        assert_eq!(
            compare_event_ids("j:5", "j:1-1"),
            Ordering::Less,
            "legacy ids have epoch 0"
        );
        assert_eq!(compare_event_ids("nocolon", "j:0-0"), Ordering::Equal);
    }
}
