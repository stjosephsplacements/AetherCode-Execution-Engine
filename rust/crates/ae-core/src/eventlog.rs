use serde::Serialize;
use serde_json::Value;
use serde_json::ser::{Formatter, Serializer};
use std::io::{self, Write};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum EventType {
    #[serde(rename = "QUEUED")]
    Queued,
    #[serde(rename = "COMPILING")]
    Compiling,
    #[serde(rename = "RUNNING")]
    Running,
    #[serde(rename = "TEST_RESULT")]
    TestResult,
    #[serde(rename = "VERDICT")]
    Verdict,
}

impl EventType {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "QUEUED",
            Self::Compiling => "COMPILING",
            Self::Running => "RUNNING",
            Self::TestResult => "TEST_RESULT",
            Self::Verdict => "VERDICT",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Event {
    pub id: String,
    pub job_id: String,
    #[serde(rename = "type")]
    pub kind: EventType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    /// Unix milliseconds.
    #[serde(rename = "ts")]
    pub timestamp: i64,
}

/// JSON formatter reproducing Go's `encoding/json` string escaping, which also
/// escapes `<`, `>`, `&`, U+2028 and U+2029.
struct GoFormatter;

impl Formatter for GoFormatter {
    fn write_string_fragment<W: ?Sized + Write>(
        &mut self,
        writer: &mut W,
        fragment: &str,
    ) -> io::Result<()> {
        let mut start = 0;
        for (i, c) in fragment.char_indices() {
            let esc: &[u8] = match c {
                '<' => b"\\u003c",
                '>' => b"\\u003e",
                '&' => b"\\u0026",
                '\u{2028}' => b"\\u2028",
                '\u{2029}' => b"\\u2029",
                _ => continue,
            };
            writer.write_all(&fragment.as_bytes()[start..i])?;
            writer.write_all(esc)?;
            start = i + c.len_utf8();
        }
        writer.write_all(&fragment.as_bytes()[start..])
    }
}

static PROCESS_EPOCH: OnceLock<i64> = OnceLock::new();
static SEQ: AtomicI64 = AtomicI64::new(0);

fn unix_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
}

impl Event {
    /// Builds an event with a process-unique id `"<job>:<epoch ns>-<seq>"` and the current time.
    #[must_use]
    pub fn new(job_id: &str, kind: EventType, data: Option<Value>) -> Self {
        let epoch = *PROCESS_EPOCH.get_or_init(unix_nanos);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed) + 1;
        Self {
            id: format!("{job_id}:{epoch}-{seq}"),
            job_id: job_id.to_owned(),
            kind,
            data,
            timestamp: unix_nanos() / 1_000_000,
        }
    }

    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.kind == EventType::Verdict
    }

    /// JSON identical to Go's `json.Marshal(Event)`.
    #[must_use]
    pub fn to_json(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let mut ser = Serializer::with_formatter(&mut out, GoFormatter);
        if self.serialize(&mut ser).is_err() {
            // Event holds only strings, integers and a serde_json::Value; serialisation cannot fail.
            return b"{}".to_vec();
        }
        out
    }

    /// One Server-Sent-Events frame.
    #[must_use]
    pub fn marshal_sse(&self) -> Vec<u8> {
        let data = self.to_json();
        let mut buf = Vec::with_capacity(data.len() + self.id.len() + 32);
        buf.extend_from_slice(b"id: ");
        buf.extend_from_slice(self.id.as_bytes());
        buf.extend_from_slice(b"\nevent: ");
        buf.extend_from_slice(self.kind.as_str().as_bytes());
        buf.extend_from_slice(b"\ndata: ");
        buf.extend_from_slice(&data);
        buf.extend_from_slice(b"\n\n");
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ev(id: &str, kind: EventType, data: Option<Value>, ts: i64) -> Event {
        Event {
            id: id.into(),
            job_id: "j".into(),
            kind,
            data,
            timestamp: ts,
        }
    }

    #[test]
    fn is_terminal_only_for_verdict() {
        assert!(Event::new("job", EventType::Verdict, None).is_terminal());
        for t in [
            EventType::Queued,
            EventType::Compiling,
            EventType::Running,
            EventType::TestResult,
        ] {
            assert!(!Event::new("job", t, None).is_terminal());
        }
    }

    #[test]
    fn marshal_sse_frame_shape() {
        let out = ev("j:1-2", EventType::Queued, None, 123).marshal_sse();
        let s = String::from_utf8(out).unwrap();
        assert!(s.starts_with("id: j:1-2\nevent: QUEUED\ndata: "));
        assert!(s.ends_with("\n\n"));
        let line = s
            .split('\n')
            .nth(2)
            .unwrap()
            .strip_prefix("data: ")
            .unwrap();
        let v: Value = serde_json::from_str(line).unwrap();
        assert_eq!(v["job_id"], "j");
        assert_eq!(v["type"], "QUEUED");
    }

    #[test]
    fn nil_data_is_omitted() {
        let s = String::from_utf8(ev("j:1-3", EventType::Running, None, 5).marshal_sse()).unwrap();
        assert!(!s.contains("\"data\":"));
    }

    #[test]
    fn new_sets_fields_and_unique_ids() {
        let e = Event::new("job123", EventType::Running, Some(json!("payload")));
        assert_eq!(e.job_id, "job123");
        assert_eq!(e.kind, EventType::Running);
        assert_eq!(e.data, Some(json!("payload")));
        assert!(e.timestamp > 0);
        assert!(e.id.starts_with("job123:"));
        let a = Event::new("x", EventType::Queued, None);
        let b = Event::new("x", EventType::Queued, None);
        assert_ne!(a.id, b.id);
    }

    #[test]
    fn event_json_matches_go_golden() {
        let evs = [
            ev("j:1-1", EventType::Queued, None, 1_700_000_000_000),
            ev(
                "j:1-2",
                EventType::Running,
                Some(json!("payload")),
                1_700_000_000_001,
            ),
            ev(
                "j:1-3",
                EventType::Compiling,
                Some(json!({"z": 1, "a": [1, 2.5, null, true]})),
                5,
            ),
            ev(
                "j:1-4",
                EventType::TestResult,
                Some(json!({
                    "stdout": "a<b>&c\u{2028}\u{2029} é \u{1}\u{8}\u{c}\n\r\t \"q\" \\ \u{7f} 🚀",
                    "test_index": 0,
                    "cpu_time_ns": 9_007_199_254_740_993_i64
                })),
                6,
            ),
            ev(
                "j:1-5",
                EventType::Verdict,
                Some(
                    json!({"verdict": "accepted", "results": [{"verdict": "accepted", "exit_status": 0, "message": ""}]}),
                ),
                7,
            ),
        ];
        let mut got = Vec::new();
        for e in &evs {
            got.extend(e.marshal_sse());
        }
        let want = include_bytes!("../testdata/events.sse.golden");
        assert_eq!(String::from_utf8_lossy(&got), String::from_utf8_lossy(want));
        assert_eq!(got.as_slice(), want.as_slice());
    }
}
