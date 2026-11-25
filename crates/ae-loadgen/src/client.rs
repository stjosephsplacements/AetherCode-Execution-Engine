use crate::report::{Report, Sample, build_report};
use crate::templates::{Template, pick};
use rand::RngExt;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct Config {
    pub url: String,
    pub token: Option<String>,
    pub students: usize,
    pub languages: Vec<String>,
    /// Students start new submissions until this long after the start.
    pub duration: Duration,
    /// Students are started evenly across this window.
    pub ramp: Duration,
    pub think_min: Duration,
    pub think_max: Duration,
    /// Per-job wait for the VERDICT.
    pub timeout: Duration,
}

struct Ctx {
    cfg: Config,
    post: reqwest::Client,
    sse: reqwest::Client,
    seen: Mutex<HashSet<(String, String)>>,
    samples: Mutex<Vec<Sample>>,
}

/// POST attempts per submission (the first plus retries after 429).
const MAX_POST_ATTEMPTS: u32 = 8;
/// Longest a `Retry-After` is honoured, so a bad header cannot stall a student.
const MAX_RETRY_AFTER_SECS: u64 = 30;

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Runs the simulation and reports.
///
/// # Errors
/// When `students` is zero, the HTTP clients cannot be built, or a student task panics.
pub async fn run(cfg: Config) -> Result<Report, String> {
    if cfg.students == 0 {
        return Err("students must be at least 1".to_owned());
    }
    let post = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5))
        .pool_max_idle_per_host(1024)
        .build()
        .map_err(|e| e.to_string())?;
    let sse = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .pool_max_idle_per_host(1024)
        .build()
        .map_err(|e| e.to_string())?;
    let ctx = Arc::new(Ctx {
        cfg,
        post,
        sse,
        seen: Mutex::default(),
        samples: Mutex::default(),
    });
    let start = Instant::now();
    let end = start + ctx.cfg.duration;
    let n = ctx.cfg.students;
    let mut tasks = Vec::with_capacity(n);
    for i in 0..n {
        let c = ctx.clone();
        let delay = c.cfg.ramp.mul_f64(i as f64 / n as f64);
        tasks.push(tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            while Instant::now() < end {
                let (t, think) = {
                    let mut rng = rand::rng();
                    let t = pick(&mut rng, &c.cfg.languages);
                    let (lo, hi) = (c.cfg.think_min, c.cfg.think_max.max(c.cfg.think_min));
                    let think = if hi > lo { rng.random_range(lo..=hi) } else { lo };
                    (t, think)
                };
                let s = submit(&c, &t).await;
                c.samples.lock().unwrap_or_else(|e| e.into_inner()).push(s);
                tokio::time::sleep(think).await;
            }
        }));
    }
    for t in tasks {
        t.await.map_err(|e| format!("student task failed: {e}"))?;
    }
    let wall = start.elapsed();
    let samples = ctx.samples.lock().unwrap_or_else(|e| e.into_inner());
    Ok(build_report(&samples, wall))
}

async fn submit(c: &Ctx, t: &Template) -> Sample {
    let mut s = Sample {
        language: t.language.to_owned(),
        expected: t.expected_verdict.to_owned(),
        actual: None,
        ingest_ms: 0.0,
        verdict_ms: 0.0,
        cold: false,
        retries: 0,
        error: None,
    };
    match drive(c, t, &mut s).await {
        Ok(v) => s.actual = Some(v),
        Err(e) => s.error = Some(e),
    }
    s
}

fn authed(c: &Ctx, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    match &c.cfg.token {
        Some(t) => rb.bearer_auth(t),
        None => rb,
    }
}

async fn drive(c: &Ctx, t: &Template, s: &mut Sample) -> Result<String, String> {
    let payload = json!({"language": t.language, "source_code": t.source, "tests": t.tests});
    let t0 = Instant::now();
    let mut resp = None;
    let mut post_start = t0;
    for attempt in 0..MAX_POST_ATTEMPTS {
        post_start = Instant::now();
        let r = authed(c, c.post.post(format!("{}/api/v1/execute", c.cfg.url)))
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("POST failed: {}", e.without_url()))?;
        // Only 429 is retried: admission rejects before anything is enqueued. A 5xx may
        // follow an accepted job, and a retry would create a duplicate.
        if r.status().as_u16() == 429 {
            s.retries += 1;
            let wait = r
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.trim().parse::<u64>().ok())
                .map_or_else(
                    || Duration::from_millis(100 << attempt.min(5)),
                    |secs| Duration::from_secs(secs.min(MAX_RETRY_AFTER_SECS)),
                );
            if attempt + 1 < MAX_POST_ATTEMPTS {
                tokio::time::sleep(wait).await;
            }
            continue;
        }
        resp = Some(r);
        break;
    }
    let resp = resp.ok_or("POST failed: all retries exhausted")?;
    // Ingestion is the accepted attempt only; retry waits are counted in `retries`.
    s.ingest_ms = ms(post_start.elapsed());
    if resp.status().as_u16() != 201 {
        return Err(format!("POST status {}", resp.status().as_u16()));
    }
    // Cold means the engine has not yet seen this pair; only an accepted submission
    // counts as having shown it.
    s.cold = c
        .seen
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert((t.language.to_owned(), t.source.to_owned()));
    let body: Value = resp.json().await.map_err(|_| "invalid POST response")?;
    let job = body["job_id"].as_str().ok_or("invalid POST response")?;
    let mut url = format!("{}/api/v1/stream?job_id={job}", c.cfg.url);
    if let Some(tok) = body["stream_token"].as_str() {
        url.push_str("&stream_token=");
        url.push_str(tok);
    }
    let wait = async {
        let mut r = authed(c, c.sse.get(&url))
            .send()
            .await
            .map_err(|e| format!("SSE connect failed: {}", e.without_url()))?;
        let mut buf: Vec<u8> = Vec::new();
        while let Some(chunk) = r
            .chunk()
            .await
            .map_err(|e| format!("SSE read failed: {}", e.without_url()))?
        {
            buf.extend_from_slice(&chunk);
            while let Some(nl) = buf.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = buf.drain(..=nl).collect();
                let line = String::from_utf8_lossy(&line);
                let Some(data) = line.trim_end().strip_prefix("data: ") else {
                    continue;
                };
                if let Ok(ev) = serde_json::from_str::<Value>(data)
                    && ev["type"] == "VERDICT"
                {
                    return Ok(ev["data"]["verdict"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned());
                }
            }
        }
        Err::<String, String>("SSE closed - no VERDICT".into())
    };
    let verdict = tokio::time::timeout(c.cfg.timeout, wait)
        .await
        .map_err(|_| "SSE timeout - no VERDICT".to_owned())??;
    s.verdict_ms = ms(t0.elapsed());
    Ok(verdict)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn zero_students_is_rejected_not_rounded_up() {
        let cfg = Config {
            url: "http://127.0.0.1:1".into(),
            token: None,
            students: 0,
            languages: vec!["c".into()],
            duration: Duration::from_secs(1),
            ramp: Duration::ZERO,
            think_min: Duration::ZERO,
            think_max: Duration::ZERO,
            timeout: Duration::from_secs(1),
        };
        assert!(run(cfg).await.is_err());
    }
}
