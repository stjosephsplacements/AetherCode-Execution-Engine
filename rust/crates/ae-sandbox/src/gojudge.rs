//! go-judge REST client.

use crate::{Cmd, Error, FileId, RunResult, Sandbox};
use ae_core::Config;
use async_trait::async_trait;
use reqwest::{Client, RequestBuilder, Response};
use serde::Serialize;
use std::future::Future;
use std::time::Duration;

const RUN_RESPONSE_MAX: usize = 16 << 20;
const SMALL_RESPONSE_MAX: usize = 1 << 16;
const ERROR_BODY_MAX: usize = 4096;

tokio::task_local! {
    static REQUEST_ID: String;
}

/// Runs `fut` with `id` sent as `X-Request-Id` on every go-judge call it makes.
pub async fn with_request_id<F: Future>(id: String, fut: F) -> F::Output {
    REQUEST_ID.scope(id, fut).await
}

/// go-judge backend: pooled keep-alive connections (128 idle), no compression.
#[derive(Clone)]
pub struct GoJudge {
    base_url: String,
    token: String,
    http: Client,
}

impl std::fmt::Debug for GoJudge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoJudge")
            .field("base_url", &self.base_url)
            .field("token", &"<redacted>")
            .finish_non_exhaustive()
    }
}

#[derive(Serialize)]
struct RunRequest<'a> {
    cmd: &'a [Cmd],
}

impl GoJudge {
    /// Builds a client for `AC_JUDGE_URL` authenticated with `AC_JUDGE_TOKEN`.
    pub fn new(cfg: &Config) -> Result<Self, Error> {
        // No overall timeout: batched /run calls vary in duration; callers bound them.
        let http = Client::builder()
            .pool_max_idle_per_host(128)
            .pool_idle_timeout(Duration::from_secs(90))
            .connect_timeout(Duration::from_secs(5))
            .tcp_keepalive(Duration::from_secs(30))
            .tcp_nodelay(true)
            .build()?;
        Ok(Self {
            base_url: cfg.judge_url.trim_end_matches('/').to_owned(),
            token: cfg.judge_token.clone(),
            http,
        })
    }

    fn authed(&self, rb: RequestBuilder) -> RequestBuilder {
        let rb = rb.bearer_auth(&self.token);
        match REQUEST_ID.try_with(Clone::clone) {
            Ok(id) if !id.is_empty() => rb.header("X-Request-Id", id),
            _ => rb,
        }
    }

    async fn send(&self, rb: RequestBuilder, limit: usize) -> Result<Vec<u8>, Error> {
        let mut resp = self.authed(rb).send().await?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = read_prefix(&mut resp, ERROR_BODY_MAX).await;
            return Err(Error::Status {
                status,
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }
        read_limited(&mut resp, limit).await
    }
}

async fn read_limited(resp: &mut Response, limit: usize) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::new();
    while let Some(chunk) = resp.chunk().await? {
        if buf.len() + chunk.len() > limit {
            return Err(Error::TooLarge(limit));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

async fn read_prefix(resp: &mut Response, limit: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    while let Ok(Some(chunk)) = resp.chunk().await {
        let take = chunk.len().min(limit - buf.len());
        buf.extend_from_slice(&chunk[..take]);
        if buf.len() == limit {
            break;
        }
    }
    buf
}

/// go-judge file ids are alphanumeric; anything else could alter the request path.
fn valid_file_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[async_trait]
impl Sandbox for GoJudge {
    async fn run(&self, cmds: Vec<Cmd>) -> Result<Vec<RunResult>, Error> {
        let rb = self
            .http
            .post(format!("{}/run", self.base_url))
            .json(&RunRequest { cmd: &cmds });
        let body = self.send(rb, RUN_RESPONSE_MAX).await?;
        let results: Vec<RunResult> = serde_json::from_slice(&body)?;
        if results.len() != cmds.len() {
            return Err(Error::ResultCount {
                got: results.len(),
                want: cmds.len(),
            });
        }
        Ok(results)
    }

    async fn cache_file(&self, name: &str, bytes: &[u8]) -> Result<FileId, Error> {
        let part = reqwest::multipart::Part::bytes(bytes.to_vec()).file_name(name.to_owned());
        let form = reqwest::multipart::Form::new().part("file", part);
        let rb = self
            .http
            .post(format!("{}/file", self.base_url))
            .multipart(form);
        let body = self.send(rb, SMALL_RESPONSE_MAX).await?;
        let id: String = serde_json::from_slice(&body)?;
        if !valid_file_id(&id) {
            return Err(Error::InvalidFileId(id));
        }
        Ok(FileId(id))
    }

    async fn delete_file(&self, id: FileId) -> Result<(), Error> {
        if !valid_file_id(&id.0) {
            return Err(Error::InvalidFileId(id.0));
        }
        let rb = self.http.delete(format!("{}/file/{}", self.base_url, id.0));
        self.send(rb, SMALL_RESPONSE_MAX).await?;
        Ok(())
    }

    async fn version(&self) -> Result<String, Error> {
        #[derive(serde::Deserialize)]
        struct Ver {
            #[serde(rename = "buildVersion")]
            build_version: String,
        }
        let rb = self.http.get(format!("{}/version", self.base_url));
        let body = self.send(rb, SMALL_RESPONSE_MAX).await?;
        Ok(serde_json::from_slice::<Ver>(&body)?.build_version)
    }
}
