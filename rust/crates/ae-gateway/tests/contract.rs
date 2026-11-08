//! `openapi_contract`: drives every operation in `api/openapi.yaml` against the running
//! router and validates each documented status code's response body against the
//! schema in the spec (a small validator over the subset of JSON Schema the spec uses).

mod common;

use common::{StackOpts, http, post_execute, run_body, stack};
use serde_json::Value;
use std::collections::BTreeSet;

fn spec() -> Value {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../api/openapi.yaml");
    let text = std::fs::read_to_string(path).expect("read api/openapi.yaml");
    let y: serde_yaml::Value = serde_yaml::from_str(&text).unwrap();
    serde_json::to_value(y).unwrap()
}

fn resolve<'a>(root: &'a Value, s: &'a Value) -> &'a Value {
    match s.get("$ref").and_then(Value::as_str) {
        Some(r) => {
            let p = r.strip_prefix('#').unwrap();
            resolve(
                root,
                root.pointer(p).unwrap_or_else(|| panic!("bad ref {r}")),
            )
        }
        None => s,
    }
}

/// Languages the engine serves (as the Go engine does) that the spec's `Language`
/// enum predates. The spec is not edited here; any other unknown id still fails.
const LANGUAGE_ENUM_DRIFT: &[&str] = &["php", "ruby"];

/// Validates `v` against `schema`; returns every violation with its JSON path.
fn validate(root: &Value, schema: &Value, v: &Value, at: &str, errs: &mut Vec<String>) {
    if schema.get("$ref").and_then(Value::as_str) == Some("#/components/schemas/Language")
        && v.as_str().is_some_and(|l| LANGUAGE_ENUM_DRIFT.contains(&l))
    {
        return;
    }
    let s = resolve(root, schema);
    if let Some(e) = s.get("enum").and_then(Value::as_array)
        && !e.contains(v)
    {
        errs.push(format!("{at}: {v} not in enum {e:?}"));
    }
    let Some(ty) = s.get("type").and_then(Value::as_str) else {
        return;
    };
    let ok = match ty {
        "string" => v.is_string(),
        "integer" => v.is_i64() || v.is_u64(),
        "number" => v.is_number(),
        "boolean" => v.is_boolean(),
        "array" => v.is_array(),
        "object" => v.is_object(),
        _ => true,
    };
    if !ok {
        errs.push(format!("{at}: expected {ty}, got {v}"));
        return;
    }
    match ty {
        "string" => {
            let sv = v.as_str().unwrap();
            match s.get("format").and_then(Value::as_str) {
                Some("uuid") if uuid::Uuid::parse_str(sv).is_err() => {
                    errs.push(format!("{at}: {sv:?} is not a uuid"));
                }
                Some("date-time") if chrono::DateTime::parse_from_rfc3339(sv).is_err() => {
                    errs.push(format!("{at}: {sv:?} is not RFC 3339"));
                }
                _ => {}
            }
            if let Some(max) = s.get("maxLength").and_then(Value::as_u64)
                && sv.chars().count() as u64 > max
            {
                errs.push(format!("{at}: longer than {max}"));
            }
        }
        "array" => {
            if let Some(items) = s.get("items") {
                for (i, x) in v.as_array().unwrap().iter().enumerate() {
                    validate(root, items, x, &format!("{at}[{i}]"), errs);
                }
            }
        }
        "object" => {
            let o = v.as_object().unwrap();
            for r in s
                .get("required")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let r = r.as_str().unwrap();
                if !o.contains_key(r) {
                    errs.push(format!("{at}: missing required {r}"));
                }
            }
            let props = s.get("properties").and_then(Value::as_object);
            for (k, x) in o {
                let sub = props
                    .and_then(|p| p.get(k))
                    .or_else(|| s.get("additionalProperties").filter(|a| a.is_object()));
                if let Some(sub) = sub {
                    validate(root, sub, x, &format!("{at}.{k}"), errs);
                }
            }
        }
        _ => {}
    }
}

struct Checker {
    root: Value,
    seen: BTreeSet<(String, String, String)>,
}

impl Checker {
    /// Asserts the status is documented for the operation and the body matches its schema.
    async fn check(&mut self, path: &str, method: &str, resp: reqwest::Response) -> Value {
        let code = resp.status().as_u16().to_string();
        let ct = resp
            .headers()
            .get("content-type")
            .map(|h| h.to_str().unwrap().to_owned())
            .unwrap_or_default();
        let retry_after = resp.headers().get("retry-after").cloned();
        let text = resp.text().await.unwrap();
        let op = &self.root["paths"][path][method];
        assert!(op.is_object(), "{method} {path} not in the spec");
        let doc = &op["responses"][&code];
        assert!(
            doc.is_object(),
            "{method} {path}: status {code} not documented: {text}"
        );
        if doc.pointer("/headers/Retry-After").is_some() {
            let ra = retry_after.expect("documented Retry-After header missing");
            ra.to_str()
                .unwrap()
                .parse::<u64>()
                .expect("Retry-After is an integer");
        }
        let content = doc["content"].as_object().expect("documented content");
        let body = if content.contains_key("text/event-stream") {
            assert!(ct.starts_with("text/event-stream"), "{ct}");
            Value::String(text)
        } else {
            let schema = &content["application/json"]["schema"];
            let v: Value = serde_json::from_str(&text)
                .unwrap_or_else(|e| panic!("{method} {path} {code}: not JSON ({e}): {text}"));
            let mut errs = Vec::new();
            validate(&self.root, schema, &v, "$", &mut errs);
            assert!(errs.is_empty(), "{method} {path} {code}: {errs:#?}\n{v}");
            v
        };
        self.seen.insert((path.into(), method.into(), code));
        body
    }
}

#[tokio::test]
async fn openapi_contract() {
    let st = stack(StackOpts {
        auth: true,
        ..StackOpts::default()
    })
    .await;
    let mut c = Checker {
        root: spec(),
        seen: BTreeSet::new(),
    };
    let exec = "/api/v1/execute";
    let tok = st.token("contract");

    // POST /api/v1/execute
    let r = post_execute(&st, &run_body("3", ""), None).await;
    let created = c.check(exec, "post", r).await;
    let (id, stoken) = (
        created["job_id"].as_str().unwrap().to_owned(),
        created["stream_token"].as_str().unwrap().to_owned(),
    );
    let idem = r#"{"language":"python","source_code":"print(1)","idempotency_key":"k","tests":[{"input":"","expected_output":"1"}]}"#;
    c.check(exec, "post", post_execute(&st, idem, Some(&tok)).await)
        .await;
    c.check(exec, "post", post_execute(&st, idem, Some(&tok)).await)
        .await; // 200
    c.check(exec, "post", post_execute(&st, "{}", None).await)
        .await; // 400
    c.check(
        exec,
        "post",
        post_execute(&st, &run_body("3", ""), Some("bad")).await,
    )
    .await; // 401
    let r = http()
        .post(format!("{}{exec}", st.url))
        .header("content-type", "text/plain")
        .body("x")
        .send()
        .await
        .unwrap();
    c.check(exec, "post", r).await; // 415
    let held = st
        .state
        .ingest
        .clone()
        .acquire_many_owned(512)
        .await
        .unwrap();
    c.check(
        exec,
        "post",
        post_execute(&st, &run_body("3", ""), None).await,
    )
    .await; // 429
    drop(held);
    // 429 from admission: the queue at capacity.
    let full = stack(StackOpts {
        max_depth: 1,
        ..StackOpts::default()
    })
    .await;
    full.queue.enqueue("ac:submissions:run", "x").await.unwrap();
    let r = post_execute(&full, &run_body("3", ""), None).await;
    assert_eq!(r.status(), 429);
    let v = c.check(exec, "post", r).await;
    assert_eq!(v["error"], "server is busy, try again later");

    // GET /api/v1/stream
    let s = "/api/v1/stream";
    c.check(
        s,
        "get",
        http().get(format!("{}{s}", st.url)).send().await.unwrap(),
    )
    .await;
    let r = http()
        .get(format!("{}{s}?job_id={id}", st.url))
        .send()
        .await
        .unwrap();
    c.check(s, "get", r).await; // 401
    let r = http()
        .get(format!("{}{s}?job_id={}", st.url, uuid::Uuid::new_v4()))
        .bearer_auth(&tok)
        .send()
        .await
        .unwrap();
    c.check(s, "get", r).await; // 404
    st.queue
        .publish(&ae_core::eventlog::Event::new(
            &id,
            ae_core::eventlog::EventType::Verdict,
            Some(serde_json::json!({"verdict": "accepted"})),
        ))
        .await
        .unwrap();
    let r = http()
        .get(format!("{}{s}?job_id={id}&stream_token={stoken}", st.url))
        .send()
        .await
        .unwrap();
    let raw = c.check(s, "get", r).await;
    // Every data line is an SSE event object of the documented shape.
    let schemas = &c.root["components"]["schemas"];
    for ev in common::parse_sse(raw.as_str().unwrap()) {
        let name = match ev["type"].as_str().unwrap() {
            "QUEUED" => "SSEEventQueued",
            "VERDICT" => "SSEEventVerdict",
            other => panic!("unexpected {other}"),
        };
        let mut errs = Vec::new();
        validate(&c.root, &schemas[name], &ev, "$", &mut errs);
        assert!(errs.is_empty(), "{errs:?}");
    }

    // GET /api/v1/submissions/{id}
    let p = "/api/v1/submissions/{id}";
    let base = format!("{}/api/v1/submissions", st.url);
    let r = http()
        .get(format!("{base}/{id}?stream_token={stoken}"))
        .send()
        .await
        .unwrap();
    c.check(p, "get", r).await;
    c.check(
        p,
        "get",
        http().get(format!("{base}/nope")).send().await.unwrap(),
    )
    .await;
    c.check(
        p,
        "get",
        http().get(format!("{base}/{id}")).send().await.unwrap(),
    )
    .await;
    let r = http()
        .get(format!("{base}/{}", uuid::Uuid::new_v4()))
        .send()
        .await
        .unwrap();
    c.check(p, "get", r).await;

    // GET /api/v1/languages
    let l = "/api/v1/languages";
    c.check(
        l,
        "get",
        http().get(format!("{}{l}", st.url)).send().await.unwrap(),
    )
    .await;

    // GET /api/v1/health, healthy then unhealthy.
    let h = "/api/v1/health";
    c.check(
        h,
        "get",
        http().get(format!("{}{h}", st.url)).send().await.unwrap(),
    )
    .await;
    st.pg.stop().await.unwrap();
    c.check(
        h,
        "get",
        http().get(format!("{}{h}", st.url)).send().await.unwrap(),
    )
    .await;

    // Every documented (operation, status) was exercised, except 500, which needs a
    // failing database mid-request.
    let mut missing = Vec::new();
    for (path, ops) in c.root["paths"].as_object().unwrap() {
        for (method, op) in ops.as_object().unwrap() {
            for code in op["responses"].as_object().unwrap().keys() {
                let k = (path.clone(), method.clone(), code.clone());
                if code != "500" && !c.seen.contains(&k) {
                    missing.push(k);
                }
            }
        }
    }
    assert!(missing.is_empty(), "not exercised: {missing:?}");
}
