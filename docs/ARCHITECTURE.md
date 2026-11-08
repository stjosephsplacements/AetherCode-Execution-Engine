# Architecture

## Overview

AetherCode Execution Engine is a stateless, horizontally scalable code execution service. It accepts HTTP submissions, fans them out to an isolated sandbox via a Redis Streams queue, and delivers verdicts back to the caller over Server-Sent Events.

```
Client
  │
  ▼
HTTP Gateway (Go net/http)
  ├── POST /api/v1/execute    → validate → write DB → enqueue Redis Stream
  ├── GET  /api/v1/stream     → SSE: replay log + subscribe pubsub channel
  ├── GET  /api/v1/health     → DB + Redis + sandbox liveness
  └── GET  /api/v1/languages  → static language catalogue
  │
  ▼
Redis Streams (dual-consumer weighted fair queuing)
  ├── ac:submissions:submit   (weight 3 — legacy stream, drained only; no new jobs are enqueued here)
  └── ac:submissions:run      (weight 1 — all new jobs)
  │
  ▼
Worker Pool (N goroutines, configurable via AC_WORKER_COUNT)
  ├── Fetches submission from PostgreSQL
  ├── Loads the job's test cases from Redis
  ├── Compiles source (compiled langs) → go-judge file cache
  ├── Runs all tests in batched parallel /run calls (up to 32 per batch)
  └── Writes verdict + test results → PostgreSQL + publishes SSE event
  │
  ▼
go-judge sandbox (separate process / container)
  └── Executes each test inside cgroup + namespace + seccomp isolation
```

---

## Components

### HTTP Gateway (`internal/gateway`)

- Single `http.ServeMux` wrapped in middleware layers (outermost first): logging → CORS → OIDC auth → mux.
- `HandleExecute` validates the request, inserts a submission row, stores the job's test data in Redis, and enqueues the job ID atomically in a single Redis pipeline (`PrepareAndEnqueue`).
- `HandleStream` replays stored events from a Redis list (`ac:events:<jobID>:log`) then subscribes to a Redis Pub/Sub channel for live events. Unauthenticated callers must present a short-lived `stream_token` (20-byte `crypto/rand`, TTL 30 min) issued when the job is created.
- `HandleHealth` performs synchronous DB ping, go-judge `/version`, and Redis ping within a 5-second timeout.
- Rate limiting: per-user sliding window (authenticated) + optional per-IP window (unauthenticated, `AC_RATE_LIMIT_UNAUTH_PER_MINUTE`).
- Admission control: rejects submissions when the target stream exceeds `AC_ADMISSION_MAX_QUEUE_DEPTH`.
- Concurrency cap: a semaphore of 512 (`ingestSem`) limits simultaneous handler goroutines to prevent PG pool exhaustion on traffic spikes.

### Queue (`internal/queue`)

- **`DualConsumer`**: weighted fair queuing over two Redis Streams. Each worker goroutine tries `submitWeight` (default 3) messages from the submit stream before one from the run stream. When both are empty, it blocks for `AC_BLOCK_READ_TIMEOUT` (default 500 ms).
- **Reclaimer**: every 30 seconds, `XAUTOCLAIM` re-claims messages idle for >30 seconds and reprocesses them. Messages exceeding `maxDeliveryCount=5` deliveries are dead-lettered (ACKed without processing) and counted in `ac_poison_messages_total`.
- **Legacy drain**: on startup, any messages remaining in the old single stream (`AC_STREAM_NAME`) are drained once and then the goroutine exits.
- **PubSub**: Redis Pub/Sub + list-backed event log for SSE delivery. `PrepareAndEnqueue` pipelines test storage + QUEUED event + stream enqueue in one round trip.

### Worker (`internal/worker`)

- `Start` launches N goroutines. Each is wrapped in a panic-recovery loop that restarts the goroutine after a 1-second backoff.
- **Compile cache**: in-process LRU-like map keyed on SHA-256(language + source). Cache entries expire after `AC_COMPILE_CACHE_TTL` (default 60 s). Avoids recompiling identical source in repeated submissions.
- **Batched execution**: up to 32 tests per `/run` call to go-judge (the `maxBatchSize` constant). go-judge executes the batch in parallel internally, reducing wall-clock time from N sequential round trips to ≈1.
- **Source file caching**: for interpreted languages with >1 test, the source is uploaded once (`BuildCacheSourceCmd`) and subsequent test commands reference it by file ID.
- **Verdict logic**: `JudgeOutput` compares actual and expected output using the request's `checker`: `exact` (default; strips trailing whitespace per line and trailing blank lines, normalises CRLF/CR to LF), `token` (whitespace-separated tokens) or `float` (numeric tokens within an absolute 1e-6).
- **Graceful shutdown**: when the context is cancelled, workers finish their current job then exit. Main waits up to 120 seconds for all workers before proceeding with HTTP server shutdown.

### Sandbox Client (`internal/sandbox`)

- Thin HTTP client wrapping go-judge's `/run` and `/version` REST endpoints.
- Connection pool: 128 idle connections, per-request context deadlines, compression disabled (binary sandbox I/O benefits from no compression overhead).
- `BuildCompileCmd`, `BuildRunCmd`, `BuildCacheSourceCmd` construct go-judge `Cmd` objects from the language config. All resource limits (CPU ns, memory bytes, proc count, clock limit = 2×CPU + 1s) are hardcoded per language with optional per-request limit overrides carried with the job's test set.

### Auth (`internal/auth`)

- OIDC JWT validation via `keyfunc` (automatic JWKS refresh). Validates `iss`, `aud`, `exp`, `sub` claims.
- Tenant resolution: maps Zitadel org ID claim (`urn:zitadel:iam:org:id`) → `tenants.id`. Cached for 5 minutes.
- User auto-provision: upserts `users` row on first login. Cached for 5 minutes.
- Role mapping from `urn:zitadel:iam:org:project:roles` claim → `admin` / `faculty` / `student`.
- Optional `AC_DATABASE_APP_URL`: if set, auth middleware uses a second PG pool (connecting as the `aethercode_app` role) for tenant/user queries so RLS policies are enforced.

### Metrics (`internal/metrics`)

All metrics are Prometheus counters/histograms exposed on `127.0.0.1:6060/metrics` (not the public gateway port).

| Metric | Type | Labels |
|--------|------|--------|
| `ac_http_requests_total` | Counter | method, path, code |
| `ac_http_request_duration_seconds` | Histogram | method, path |
| `ac_jobs_processed_total` | Counter | verdict |
| `ac_job_processing_duration_seconds` | Histogram | language, verdict |
| `ac_compile_duration_seconds` | Histogram | language |
| `ac_active_workers` | Gauge | — |
| `ac_queue_depth` | Gauge | stream |
| `ac_rate_limit_rejections_total` | Counter | type (user/ip) |
| `ac_poison_messages_total` | Counter | — |

---

## Submission lifecycle (sequence)

```mermaid
sequenceDiagram
    participant C as Client
    participant G as Gateway
    participant PG as PostgreSQL
    participant R as Redis
    participant W as Worker
    participant J as go-judge

    C->>G: POST /api/v1/execute
    G->>PG: INSERT submissions (status=queued)
    G->>R: HSET ac:tests:<id> + RPUSH ac:events:<id>:log + XADD stream
    G-->>C: 201 {job_id, stream_token?}

    C->>G: GET /api/v1/stream?job_id=<id>
    G->>R: LRANGE ac:events:<id>:log (replay)
    G->>R: SUBSCRIBE ac:events:<id>
    G-->>C: SSE: event: QUEUED

    W->>R: XREADGROUP (dequeue)
    W->>PG: UPDATE status=running
    W->>R: PUBLISH → SSE: event: RUNNING
    W->>J: POST /run [batch of test cmds]
    J-->>W: [{status, time, memory, stdout, stderr}, ...]
    W->>PG: batch INSERT test_results + UPDATE verdict
    W->>R: PUBLISH → SSE: event: VERDICT
    W->>R: XACK
    G-->>C: SSE: event: VERDICT (stream closes)
```

---

## Deployment topologies

### Single node (systemd)
```
[nginx/Cloudflare] → [aethercode-exec :5100] → [go-judge :5050]
                                              → [postgres :5432]
                                              → [redis :6379]
```
See `deploy/systemd/aethercode-exec.service` and `deploy/systemd/env.example`.

### Kubernetes (multi-replica)
```
[Ingress] → [Service :5100] → [Deployment (2–10 replicas)]
                                    → [go-judge DaemonSet or Deployment]
                                    → [PostgreSQL StatefulSet / RDS]
                                    → [Redis Deployment / ElastiCache]
```
See `deploy/k8s/` for manifests. Workers are stateless — scale the Deployment replicas to add execution capacity. go-judge must run on every node (use a DaemonSet) or as a shared service; in shared-service mode, all workers point at the same `AC_JUDGE_URL`.

---

## Security model

- **Sandbox isolation**: user code executes inside go-judge's cgroup + Linux namespace + seccomp sandbox. The engine never executes user code directly.
- **Tenant isolation**: every submission and test result is scoped to a `tenant_id` (UUID). The auth middleware resolves the caller's Zitadel org to a tenant. Row-Level Security policies in PostgreSQL enforce tenant boundaries at the DB layer when `AC_DATABASE_APP_URL` is configured.
- **Auth**: OIDC JWT (Zitadel). JWT auth is optional. When a valid token is presented, the engine enforces tenant and user ownership of jobs. Unauthenticated callers receive a `stream_token` with each job, which gates both SSE streaming and cancel.
- **Rate limiting**: per-user sliding window (`AC_RATE_LIMIT_PER_MINUTE`) + optional per-IP window for unauthenticated callers. Admission control caps the Redis queue depth.
- **Input validation**: source code ≤ 64 KB, body ≤ 64 MB, test data ≤ 8 MB per case, idempotency key ≤ 255 bytes.

---

## Rust engine

A drop-in rewrite of the Go engine in `rust/` (binary `engine`, crate `ae-engine`), built from `rust/Dockerfile`. It keeps the HTTP API, Redis stream layout, Postgres schema (same embedded migrations) and Prometheus metric names of the Go engine, and runs beside it through shadow and cutover.

### Components

| Crate | Role |
|-------|------|
| `ae-core` | Config, models, judge/output normalisation, event log |
| `ae-gateway` | Axum router, auth (OIDC), rate limit, admission, SSE, metrics |
| `ae-queue` | Redis Streams, weighted consumer, reclaimer, poison dead-lettering |
| `ae-db` | Postgres access and advisory-locked embedded migrations |
| `ae-sandbox` | go-judge client |
| `ae-worker` | Compile cache, scheduler budget, batched test execution |
| `ae-engine` | Startup, supervision, roles, shadow mode, graceful drain |
| `ae-loadgen`, `ae-shadowdiff` | Load generator and shadow-vs-Go verdict comparison |

### Roles

`--role all|gateway|worker` (default `AC_ROLE`, `all`). `gateway` serves HTTP and enqueues; `worker` consumes and runs jobs and serves **no public HTTP**. In Kubernetes they are separate Deployments (`deploy/k8s-overlays/rust`).

### Shadow mode

`--role worker --shadow` reads the Go engine's streams through its own consumer group (`<AC_CONSUMER_GROUP>-rust`), runs the real worker, but writes nothing to Postgres, emits no events or webhooks, and stores its verdict in `ac:shadow:<job_id>` (1 h TTL; dead letters go to `ac:dlq:poison:rust`). `ae-shadowdiff` compares those records with the Go rows. Migrations stay with the authoritative engine. Compose: `--profile rust-shadow`.

### Admission and 429 semantics

`POST /api/v1/execute` answers `429` with `Retry-After` when the queue is at capacity (`AC_ADMISSION_MAX_QUEUE_DEPTH`; 0 = 4 x workers), the per-user or per-IP rate limit is hit, or the ingest concurrency limit is exhausted. `Retry-After` follows the measured job drain rate. `503` + `Retry-After: 5` means the engine is not ready (starting up or draining); nothing is enqueued.

### Readiness and drain

`/livez` is up as soon as the listener is bound; `/readyz` is 503 until startup (DB, Redis, sandbox and language self-tests, pre-warm) finishes. On SIGTERM/SIGINT: `/readyz` goes 503 and `execute` returns 503, workers stop reading and get up to 120 s for in-flight jobs, the HTTP server keeps serving all other routes (results, SSE, health) until the drain ends, telemetry is flushed, exit 0. A second signal exits 1. Use `terminationGracePeriodSeconds >= 150`; no `preStop` is needed.

### Metrics

Served on `127.0.0.1:${AC_DEBUG_PORT}/metrics` only (loopback, no flag to change the bind). All metrics in the table above plus `ac_stage_duration_seconds{stage}`, `ac_queue_age_seconds{stream}` (age of the oldest entry; the autoscaling signal), `ac_compile_cache_hits_total` (misses = `ac_compile_duration_seconds_count`) and `ac_scheduler_budget_used{resource}`. Because the port is loopback-only, in Kubernetes every pod runs an nginx sidecar (`metrics-proxy`, port 9090) that forwards `/metrics`; Prometheus scrapes the pod annotation `prometheus.io/port: 9090`. On a VM, scrape `127.0.0.1:6060` from a local agent. Alert rules: `deploy/monitoring/alerts.yml` (group `aethercode-exec-rs`).

### Autoscaling

The worker HPA targets `ac_queue_age_seconds{stream="ac:submissions:submit"}` through the external metrics API. This needs Prometheus Adapter (external rule using `max by (stream)`) or KEDA; the rule and the KEDA alternative are in `deploy/k8s-overlays/rust/hpa.yaml`. The gateway HPA uses CPU.

### Configuration

Same `AC_*` variables as the Go engine (database, Redis, go-judge, OIDC, streams, rate limits) plus:

| Variable | Default | Meaning |
|----------|---------|---------|
| `AC_ROLE` | `all` | `all`, `gateway` or `worker` |
| `AC_WORKERS` | `0` | Worker concurrency budget; 0 = a quarter of the cores |
| `AC_COMPILE_CACHE_MAX_MB` | `1024` | Compile cache size cap |
| `AC_PREWARM` | `16` | Sandbox slots warmed before readiness |
| `AC_OTLP_ENDPOINT` | empty | OTLP/HTTP trace endpoint; empty disables export |
| `AC_DEBUG_PORT` | `6060` | Loopback metrics port |
| `AC_LANGUAGES_FILE` | empty | TOML language table replacing the built-in one |

### Deployment assets

`rust/Dockerfile` (distroless, uid 65532, no shell), `docker-compose.yml` (profiles `rust`, `rust-shadow`), `deploy/k8s-overlays/rust/` (gateway, worker and shadow-worker Deployments, HPA, PDB, NetworkPolicy), `deploy/systemd/aethercode-exec-rs.service` with `env-rs.example`.
