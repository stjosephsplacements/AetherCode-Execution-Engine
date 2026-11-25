//! `GET /api/v1/ws`: WebSocket alternative to [`sse::stream`]; same auth, replay, and live
//! subscribe semantics; each message is JSON matching the SSE `data:` payload.

use crate::auth::Identity;
use crate::handlers::{AppState, verify_token};
use crate::sse::{LOOKUP_TIMEOUT, SSE_HEARTBEAT, SSE_MAX_LIFETIME, not_after, plain_error};
use ae_core::eventlog::{Event, EventType};
use axum::Extension;
use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{FromRequest, Query, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use tokio::time::{Instant, interval_at};

/// `GET /api/v1/ws?job_id=<uuid>[&stream_token=...]`.
/// Auth / validation run before the WebSocket upgrade (same order as Go), so plain
/// HTTP clients get 400/401 JSON instead of an upgrade rejection.
pub async fn stream(
    State(st): State<AppState>,
    identity: Option<Extension<Identity>>,
    Query(query): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    let raw = query.get("job_id").map_or("", String::as_str);
    if raw.is_empty() {
        return plain_error(StatusCode::BAD_REQUEST, r#"{"error":"job_id is required"}"#);
    }
    let Ok(parsed) = uuid::Uuid::parse_str(raw) else {
        return plain_error(StatusCode::BAD_REQUEST, r#"{"error":"invalid job_id"}"#);
    };
    let job_id = parsed.to_string();
    let last_event_id = req
        .headers()
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

    match WebSocketUpgrade::from_request(req, &st).await {
        Ok(ws) => ws.on_upgrade(move |socket| run_socket(socket, st, job_id, last_event_id)),
        Err(rej) => rej.into_response(),
    }
}

async fn run_socket(socket: WebSocket, st: AppState, job_id: String, last_event_id: String) {
    let (mut sink, mut source) = socket.split();

    let sub = match st.queue.subscribe(&job_id).await {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::error!(job_id = %job_id, error = %e, "ws: subscribe failed");
            None
        }
    };

    let events = st.queue.replay(&job_id).await.unwrap_or_else(|e| {
        tracing::error!(job_id = %job_id, error = %e, "ws: get event log failed");
        Vec::new()
    });

    let mut sent_up_to = String::new();
    let mut terminal = false;
    for ev in &events {
        if not_after(&ev.id, &last_event_id) {
            continue;
        }
        if send_event(&mut sink, ev).await.is_err() {
            return;
        }
        sent_up_to.clone_from(&ev.id);
        if ev.is_terminal() {
            terminal = true;
            break;
        }
    }
    if terminal {
        let _ = sink.close().await;
        return;
    }

    let Some(mut sub) = sub else {
        let _ = sink.close().await;
        return;
    };

    let mut heartbeat = interval_at(Instant::now() + SSE_HEARTBEAT, SSE_HEARTBEAT);
    let deadline = tokio::time::sleep(SSE_MAX_LIFETIME);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            () = &mut deadline => {
                if let Some(ev) = deadline_event(&st, &job_id, &last_event_id, &sent_up_to).await {
                    let _ = send_event(&mut sink, &ev).await;
                }
                break;
            }
            _ = heartbeat.tick() => {
                if sink.send(Message::Ping(Bytes::new())).await.is_err() {
                    break;
                }
            }
            msg = source.next() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
            ev = sub.recv() => {
                let Some(ev) = ev else {
                    break;
                };
                if not_after(&ev.id, &sent_up_to) || not_after(&ev.id, &last_event_id) {
                    continue;
                }
                sent_up_to.clone_from(&ev.id);
                if send_event(&mut sink, &ev).await.is_err() {
                    break;
                }
                if ev.is_terminal() {
                    break;
                }
            }
        }
    }
    let _ = sink.close().await;
}

async fn send_event(
    sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    ev: &Event,
) -> Result<(), ()> {
    let payload = ev.to_json();
    let text = String::from_utf8(payload).map_err(|_| ())?;
    sink.send(Message::Text(text.into())).await.map_err(|_| ())
}

async fn deadline_event(
    st: &AppState,
    job_id: &str,
    last_event_id: &str,
    sent_up_to: &str,
) -> Option<Event> {
    let q = &st.queue;
    let latest = tokio::time::timeout(LOOKUP_TIMEOUT, q.replay(job_id))
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or_default();
    if let Some(ev) = latest
        .iter()
        .find(|e| e.is_terminal() && !not_after(&e.id, sent_up_to))
    {
        if not_after(&ev.id, last_event_id) {
            return None;
        }
        return Some(ev.clone());
    }
    if let Ok(Ok(Some(sub))) =
        tokio::time::timeout(LOOKUP_TIMEOUT, st.db.get_submission(job_id)).await
        && matches!(
            sub.status,
            ae_core::model::Status::Completed | ae_core::model::Status::Failed
        )
    {
        let verdict = sub
            .verdict
            .map(crate::metrics::verdict_label)
            .unwrap_or_default();
        return Some(Event::new(
            job_id,
            EventType::Verdict,
            Some(serde_json::json!({
                "verdict": verdict,
                "tests_passed": sub.tests_passed,
                "test_count": sub.test_count,
            })),
        ));
    }
    let ev = Event::new(
        job_id,
        EventType::Verdict,
        Some(serde_json::json!({
            "verdict": "internal_error",
            "error": "no verdict within timeout",
        })),
    );
    match tokio::time::timeout(LOOKUP_TIMEOUT, q.publish(&ev)).await {
        Ok(Ok(())) => {}
        _ => tracing::warn!(job_id, "ws: failed to persist timeout event"),
    }
    Some(ev)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sse::compare_event_ids;
    use std::cmp::Ordering;

    #[test]
    fn reuses_sse_event_id_ordering() {
        assert_eq!(compare_event_ids("j:1-2", "j:1-10"), Ordering::Less);
    }
}