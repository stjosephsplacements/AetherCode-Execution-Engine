//! Logging and tracing (Go: `internal/logging`). Records go to stderr as one JSON object
//! per line with slog's keys (`time`, `level`, `msg`), renamed for `gcp` (`severity`,
//! `message`) and `cloudwatch` (`timestamp`, `message`); `text` is human-readable.
//! Fields of the enclosing spans (`job_id`, `traceparent`, ...) are merged into the
//! record. With `AC_OTLP_ENDPOINT` spans are also exported over OTLP/HTTP.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use serde_json::{Map, Value};
use std::fmt;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::format::{JsonFields, Writer};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, FormattedFields};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

const SERVICE_NAME: &str = "aethercode-exec";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Json,
    Text,
    Gcp,
    CloudWatch,
}

impl Format {
    /// `AC_LOG_FORMAT`; empty means json.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "" | "json" => Some(Self::Json),
            "text" => Some(Self::Text),
            "gcp" => Some(Self::Gcp),
            "cloudwatch" => Some(Self::CloudWatch),
            _ => None,
        }
    }
}

/// `AC_LOG_LEVEL` as Go's `parseLogLevel` reads it: anything unknown is info.
pub fn level(s: &str) -> LevelFilter {
    match s {
        "debug" => LevelFilter::DEBUG,
        "warn" => LevelFilter::WARN,
        "error" => LevelFilter::ERROR,
        _ => LevelFilter::INFO,
    }
}

/// Handle to the OTLP exporter; [`Telemetry::shutdown`] flushes it.
pub struct Telemetry {
    provider: Option<SdkTracerProvider>,
}

impl Telemetry {
    /// Flushes and stops the OTLP exporter, if any.
    pub fn shutdown(self) {
        if let Some(p) = self.provider
            && let Err(e) = p.shutdown()
        {
            tracing::warn!(error = %e, "telemetry: flush failed");
        }
    }
}

/// Installs the global subscriber and the W3C trace-context propagator.
///
/// # Errors
/// The OTLP exporter cannot be built from `otlp_endpoint`.
pub fn init(format: Format, level: LevelFilter, otlp_endpoint: &str) -> Result<Telemetry, String> {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let provider = if otlp_endpoint.is_empty() {
        None
    } else {
        let endpoint = if otlp_endpoint.ends_with("/v1/traces") {
            otlp_endpoint.to_owned()
        } else {
            format!("{}/v1/traces", otlp_endpoint.trim_end_matches('/'))
        };
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .build()
            .map_err(|e| format!("otlp exporter: {e}"))?;
        Some(
            SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(
                    opentelemetry_sdk::Resource::builder()
                        .with_service_name(SERVICE_NAME)
                        .build(),
                )
                .build(),
        )
    };
    let otel = provider
        .as_ref()
        .map(|p| tracing_opentelemetry::layer().with_tracer(p.tracer(SERVICE_NAME)));
    let out = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
    let fmt = if format == Format::Text {
        out.with_ansi(false).boxed()
    } else {
        out.fmt_fields(JsonFields::new())
            .event_format(GoJson(format))
            .boxed()
    };
    tracing_subscriber::registry()
        .with(level)
        .with(otel)
        .with(fmt)
        .try_init()
        .map_err(|e| format!("logging: {e}"))?;
    Ok(Telemetry { provider })
}

/// slog's JSON handler record shape.
struct GoJson(Format);

impl<S, N> FormatEvent<S, N> for GoJson
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut w: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let (time_key, level_key, msg_key) = match self.0 {
            Format::Gcp => ("time", "severity", "message"),
            Format::CloudWatch => ("timestamp", "level", "message"),
            Format::Json | Format::Text => ("time", "level", "msg"),
        };
        let mut rec = Map::new();
        rec.insert(
            time_key.into(),
            chrono::Utc::now()
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
                .into(),
        );
        rec.insert(
            level_key.into(),
            level_name(self.0, *event.metadata().level()).into(),
        );
        rec.insert(msg_key.into(), Value::String(String::new()));
        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                let ext = span.extensions();
                if let Some(f) = ext.get::<FormattedFields<N>>()
                    && let Ok(Value::Object(m)) = serde_json::from_str::<Value>(f)
                {
                    rec.extend(m);
                }
            }
        }
        event.record(&mut Fields {
            rec: &mut rec,
            msg_key,
        });
        writeln!(w, "{}", Value::Object(rec))
    }
}

fn level_name(f: Format, l: Level) -> &'static str {
    match (f, l) {
        (_, Level::ERROR) => "ERROR",
        (Format::Gcp, Level::WARN) => "WARNING",
        (_, Level::WARN) => "WARN",
        (_, Level::INFO) => "INFO",
        _ => "DEBUG",
    }
}

struct Fields<'a> {
    rec: &'a mut Map<String, Value>,
    msg_key: &'static str,
}

impl Fields<'_> {
    fn put(&mut self, field: &Field, v: Value) {
        let key = if field.name() == "message" {
            self.msg_key
        } else {
            field.name()
        };
        self.rec.insert(key.to_owned(), v);
    }
}

impl Visit for Fields<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, value.into());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.put(field, value.into());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field, value.into());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.put(field, value.into());
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.put(field, value.into());
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.put(field, value.to_string().into());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.put(field, format!("{value:?}").into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_and_levels_match_go() {
        assert_eq!(Format::parse(""), Some(Format::Json));
        assert_eq!(Format::parse("gcp"), Some(Format::Gcp));
        assert_eq!(Format::parse("xml"), None);
        assert_eq!(level("debug"), LevelFilter::DEBUG);
        assert_eq!(level("bogus"), LevelFilter::INFO);
        assert_eq!(level_name(Format::Gcp, Level::WARN), "WARNING");
        assert_eq!(level_name(Format::Json, Level::WARN), "WARN");
        assert_eq!(level_name(Format::Json, Level::TRACE), "DEBUG");
    }

    #[test]
    fn records_use_go_keys() {
        use std::sync::{Arc, Mutex};
        #[derive(Clone, Default)]
        struct Buf(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Buf {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        for (format, keys) in [
            (Format::Json, ["time", "level", "msg"]),
            (Format::Gcp, ["time", "severity", "message"]),
            (Format::CloudWatch, ["timestamp", "level", "message"]),
        ] {
            let buf = Buf::default();
            let w = buf.clone();
            let sub = tracing_subscriber::registry().with(
                tracing_subscriber::fmt::layer()
                    .with_writer(move || w.clone())
                    .fmt_fields(JsonFields::new())
                    .event_format(GoJson(format)),
            );
            tracing::subscriber::with_default(sub, || {
                let span = tracing::info_span!("job", job_id = "j1");
                let _e = span.enter();
                tracing::warn!(n = 3, "hello");
            });
            let line = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
            let v: Value = serde_json::from_str(line.trim()).unwrap();
            for k in keys {
                assert!(v.get(k).is_some(), "{format:?} missing {k}: {v}");
            }
            assert_eq!(v[keys[2]], "hello");
            assert_eq!(v["job_id"], "j1");
            assert_eq!(v["n"], 3);
        }
    }
}
