# AetherCode Execution Engine

On-premise distributed code execution engine. Accepts code submissions over HTTP, runs them in isolated sandboxes, and streams verdicts back via Server-Sent Events.

![CI](https://github.com/stjosephsplacements/AetherCode-Execution-Engine/actions/workflows/ci.yml/badge.svg)
![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)
![Go](https://img.shields.io/badge/Go-1.25%2B-00ADD8?logo=go&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-ready-2496ED?logo=docker&logoColor=white)

**Status:** Production-hardened. Zero lost jobs verified at 5,000 concurrent connections / 15,000 submissions.

> **License:** open source under the [GNU AGPL v3](LICENSE). If you offer the
> engine as a network service, the AGPL's network-copyleft terms require you to
> share the engine's source — see [License](#license).

---

## Table of Contents

- [Quickstart](#quickstart)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Operations](#operations)
- [Performance](#performance)
- [Contributing](#contributing)
- [Security](#security)
- [License](#license)

---

## Quickstart

### Prerequisites

| Component | Version | Notes |
|---|---|---|
| Go | 1.25+ | Build toolchain |
| PostgreSQL | 18 | Dedicated instance on `:5433` |
| Redis | 7+ | On `127.0.0.1:6379` |
| go-judge | v1.13.0 | Sandbox engine on `127.0.0.1:5050` |

### Quickstart (Docker Compose)

The fastest way to run the whole stack — engine, PostgreSQL, Redis, and the
go-judge sandbox — is Docker Compose:

```bash
make up              # docker compose up --build
curl http://127.0.0.1:5100/api/v1/health   # wait until it returns 200
make down            # stop the stack when you're done
```

> The go-judge container runs with `privileged: true` in the dev stack because
> its sandbox needs cgroups + namespaces. That is fine for a machine you control;
> use the systemd deployment with explicit limits in production.

### Build and run (from source)

```bash
# Build
go build -o aethercode-exec ./cmd/stj-exec

# Configure (copy and edit)
cp deploy/env.example /etc/aethercode-exec/env
# Set AC_DATABASE_URL, AC_JUDGE_TOKEN at minimum

# Run
./aethercode-exec
```

### Verify

```bash
# Health check
curl http://127.0.0.1:5100/api/v1/health

# End-to-end test (all languages, all verdicts)
bash scripts/test_e2e.sh

# Load test
go build -o /tmp/loadgen ./cmd/loadgen
/tmp/loadgen -concurrency 400 -total 1200 -timeout 120s
```

### Install as a systemd service

```bash
sudo cp deploy/aethercode-exec.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now aethercode-exec
```

---

## Architecture

```
Browser / Web Client
       │
       │  POST /api/v1/execute  ──────────────────────────────┐
       │                                                        │
       ▼                                                        ▼
 ┌─────────────────────────────────────────────────────────────────┐
 │                    Go HTTP Gateway (:5100)                       │
 │  LoggingMiddleware → CORS → OIDC Auth → HandleExecute/Stream    │
 │  Ingest semaphore (64) · Rate limiter · Admission control       │
 └────────────┬─────────────────────────────┬────────────────────┘
              │                             │
              │ INSERT submission           │ XADD
              ▼                             ▼
        ┌──────────┐               ┌──────────────────┐
        │ PG 18    │               │  Redis 7 Streams  │
        │ (system  │               │  submit / run     │
        │  of      │               │  (weighted 3:1)   │
        │  record) │               └────────┬─────────┘
        └──────────┘                        │ XREADGROUP
              ▲                             ▼
              │               ┌────────────────────────┐
              │               │  Go Workers (×64)       │
              │ InsertResults │  DualConsumer           │
              └───────────────┤  compile → run tests   │
                              │  → verdict             │
                              └────────────┬───────────┘
                                           │
                                           │ Redis pub/sub
                                           ▼
                              ┌────────────────────────┐
                              │  SSE Hub (HandleStream) │
                              │  subscribe-before-replay│
                              │  heartbeat 15s · 5min  │
                              │  deadline · resume from│
                              │  Last-Event-ID         │
                              └────────────────────────┘
```

### Hot path (run mode)

1. `POST /api/v1/execute` — gateway validates, inserts PG row, stores tests + publishes QUEUED event + enqueues in a single Redis pipeline.
2. `GET /api/v1/stream?job_id=…` — client subscribes to SSE. Gateway subscribes to Redis pub/sub **first**, then replays event log (subscribe-before-replay prevents a race).
3. Worker picks up the message via `XREADGROUP`. For compiled languages: compile → cache binary → run each test. For interpreted: run each test directly.
4. Worker writes results to PG and publishes VERDICT event to Redis.
5. SSE hub delivers the event to the client and closes the stream.

### Fair queuing

Two Redis streams: `ac:submissions:submit` (graded problems) and `ac:submissions:run` (IDE mode). `DualConsumer` reads 3 submit messages for every 1 run message, preventing IDE traffic from starving grading during contests.

### Reliability

- **Orphan reaper** — scans PG every 60s for submissions stuck in `queued` for > 2 minutes and re-enqueues them.
- **Reclaimer** — `XAUTOCLAIM` every 30s picks up messages that a crashed worker never ACKed. Messages that fail 5+ times are dead-lettered.
- **CAS writes** — `UPDATE … WHERE status NOT IN ('completed','failed')` prevents double-writes on redelivery.
- **Worker restart** — panicked workers auto-restart with a 1s delay; the reclaimer does too.

---

## Configuration

All configuration is via environment variables. Copy `deploy/env.example` as a starting point.

| Variable | Default | Description |
|---|---|---|
| `AC_GATEWAY_ADDR` | `127.0.0.1:5100` | HTTP listen address |
| `AC_DATABASE_URL` | `postgres://aethercode:aethercode@127.0.0.1:5433/aethercode_exec` | PostgreSQL DSN |
| `AC_REDIS_ADDR` | `127.0.0.1:6379` | Redis address |
| `AC_JUDGE_URL` | `http://127.0.0.1:5050` | go-judge base URL |
| `AC_JUDGE_TOKEN` | *(required)* | Bearer token for go-judge |
| `AC_WORKER_COUNT` | `8` | Parallel worker goroutines |
| `AC_JOB_TIMEOUT` | `90s` | Per-job deadline (covers compile + all test runs) |
| `AC_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `AC_LOG_FORMAT` | `json` | `json` (production) or `text` (development) |
| `AC_AUTH_ENABLED` | `false` | Enable Zitadel OIDC JWT validation |
| `AC_OIDC_ISSUER` | `https://sso.example.com` | OIDC issuer URL |
| `AC_OIDC_JWKS_URL` | `http://127.0.0.1:8087/oauth/v2/keys` | JWKS endpoint (internal) |
| `AC_OIDC_AUDIENCE` | *(required when auth enabled)* | Expected `aud` claim |
| `AC_RATE_LIMIT_PER_MINUTE` | `30` | Max submissions per user per minute |
| `AC_RATE_LIMIT_BURST` | `10` | Max submissions per user per 10 seconds |
| `AC_ADMISSION_MAX_QUEUE_DEPTH` | `5000` | Reject new jobs when queue exceeds this depth |
| `AC_SUBMIT_WEIGHT` | `3` | Submit-stream priority weight vs run-stream |
| `AC_TRUST_PROXY` | `false` | Read client IP from `X-Forwarded-For` |
| `AC_CORS_ALLOWED_ORIGINS` | *(empty = CORS disabled)* | Comma-separated allowed origins |
| `AC_STREAM_NAME` | `ac:submissions` | Legacy stream (drained on startup, then unused) |
| `AC_SUBMIT_STREAM` | `ac:submissions:submit` | Submit-mode stream key |
| `AC_RUN_STREAM` | `ac:submissions:run` | Run-mode stream key |
| `AC_CONSUMER_GROUP` | `workers` | Redis consumer group name |

### Production checklist

- [ ] Set `AC_JUDGE_TOKEN` to a secret value (the go-judge default token is publicly known).
- [ ] Set `AC_AUTH_ENABLED=true` and `AC_OIDC_AUDIENCE` to your application's client ID.
- [ ] Set `AC_WORKER_COUNT` to match go-judge's `parallelism` setting (64 for production).
- [ ] Set `AC_ADMISSION_MAX_QUEUE_DEPTH` to `20000` for 5,000-concurrency bursts.
- [ ] Set `AC_CORS_ALLOWED_ORIGINS` if the browser frontend is on a different origin.

---

## API Reference

Base URL: `http://127.0.0.1:5100` (or your configured `AC_GATEWAY_ADDR`)

All responses are `application/json`. Every request receives an `X-Request-Id` header for tracing.

### POST /api/v1/execute

Submit code for execution.

**Request headers**

| Header | Required | Description |
|---|---|---|
| `Content-Type` | Yes | Must be `application/json` |
| `Authorization` | Conditional | `Bearer <token>` — required when auth is enabled and mode is `submit` |
| `Idempotency-Key` | No | Deduplicate retries (any string, max 255 chars) |

**Request body**

```json
{
  "language": "python",
  "source_code": "n=int(input())\nprint(n*2)",
  "mode": "run",
  "tests": [
    { "input": "21\n", "expected_output": "42\n" }
  ]
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `language` | string | Yes | See [Supported Languages](#supported-languages) |
| `source_code` | string | Yes | Max 64 KB |
| `mode` | string | No | `"run"` (default) or `"submit"` |
| `tests` | array | For `run` mode | Max 100 tests; each input/expected_output max 64 KB |
| `problem_version_id` | string (UUID) | For `submit` mode | Server-side test suite to grade against |
| `idempotency_key` | string | No | Client-chosen key; same key + user + mode returns the original job ID |

**Response `201 Created`**

```json
{ "job_id": "019741b2-3c1a-7d4e-8f0a-1234567890ab" }
```

**Error responses**

| Status | Meaning |
|---|---|
| `400` | Invalid request (bad JSON, unsupported language, missing field) |
| `401` | Authentication required |
| `404` | Problem version not found |
| `415` | Content-Type must be `application/json` |
| `429` | Rate limit or queue depth exceeded — retry after `Retry-After` seconds |
| `500` | Internal error |

---

### GET /api/v1/stream

Subscribe to job events via Server-Sent Events.

**Query parameters**

| Parameter | Required | Description |
|---|---|---|
| `job_id` | Yes | UUID returned by `POST /execute` |

**Request headers**

| Header | Optional | Description |
|---|---|---|
| `Last-Event-ID` | No | Resume from this event ID (sent automatically by browser `EventSource` on reconnect) |
| `Authorization` | Conditional | Required when `AC_AUTH_ENABLED=true` |

**Response** — `text/event-stream`

The stream sends one SSE event per lifecycle stage, then closes. Events are persisted for 10 minutes so a reconnecting client gets the full history replayed via `Last-Event-ID`.

**Event: QUEUED**
```
id: <job_id>:<epoch>-<seq>
event: QUEUED
data: {"id":"...","job_id":"...","type":"QUEUED","ts":1727947200000}
```

**Event: COMPILING** *(compiled languages only)*
```
id: <job_id>:<epoch>-<seq>
event: COMPILING
data: {"id":"...","job_id":"...","type":"COMPILING","ts":...}
```

**Event: RUNNING**
```
id: <job_id>:<epoch>-<seq>
event: RUNNING
data: {"id":"...","job_id":"...","type":"RUNNING","ts":...}
```

**Event: VERDICT** *(terminal — stream closes after this)*
```
id: <job_id>:<epoch>-<seq>
event: VERDICT
data: {
  "id": "...",
  "job_id": "...",
  "type": "VERDICT",
  "ts": 1727947201234,
  "data": {
    "verdict": "accepted",
    "tests_passed": 2,
    "test_count": 2,
    "cpu_time_ns": 12340000,
    "memory_bytes": 4194304,
    "results": [
      {
        "test_index": 0,
        "verdict": "accepted",
        "cpu_time_ns": 6000000,
        "memory_bytes": 2097152,
        "stdout_preview": "42\n",
        "stderr_preview": ""
      }
    ]
  }
}
```

> **Note:** In `submit` mode, `stdout_preview` and `stderr_preview` are omitted for hidden (non-sample) tests.

**Verdict values**

| Verdict | Meaning |
|---|---|
| `accepted` | All tests passed |
| `wrong_answer` | Output did not match expected |
| `time_limit` | Exceeded CPU time limit (2s per test) |
| `memory_limit` | Exceeded memory limit (512 MB per test) |
| `output_limit` | Stdout exceeded 64 KB |
| `runtime_error` | Non-zero exit code or signal |
| `compilation_error` | Compiler rejected the source — `compile_stderr` present in VERDICT data |
| `internal_error` | System-level failure (sandbox unreachable, no tests found) |

**Heartbeat** — the server sends a comment line every 15 seconds:
```
: heartbeat
```

**Timeout** — if no verdict arrives within 5 minutes, the server sends a synthetic VERDICT with `"verdict": "internal_error"` and closes.

**Reconnect hint** — if the Redis pub/sub connection drops mid-stream, the server sends `": reconnect"` and closes. The browser `EventSource` reconnects automatically with `Last-Event-ID`; the event log replays any missed events.

---

### GET /api/v1/health

Returns the health of all subsystems.

**Response `200 OK`**

```json
{ "database": "ok", "sandbox": "ok", "redis": "ok" }
```

Returns `503` if any subsystem is degraded.

---

### Supported Languages

| Language key | Runtime | Compile | CPU limit | Memory limit |
|---|---|---|---|---|
| `c` | gcc 13.3 | Yes | 2s | 512 MB |
| `cpp` | g++ 13.3 (C++17) | Yes | 2s | 512 MB |
| `java` | OpenJDK 21 | Yes | 2s | 512 MB |
| `go` | Go 1.22 | Yes | 2s | 512 MB |
| `python` | Python 3.12 | No | 2s | 512 MB |
| `javascript` | Node.js 24 | No | 2s | 512 MB |
| `sqlite` | SQLite 3.45 | No | 2s | 512 MB |

Compile time limits: C/C++ 10s, Java 15s, Go 30s (each with their own memory caps).

---

## Operations

### Logs

Structured JSON on stderr (default). Parse with `jq`:

```bash
journalctl -u aethercode-exec -f | jq .
```

Every HTTP request includes `request_id`, `method`, `path`, `status`, `duration_ms`, and `remote_ip`.
Set `AC_LOG_FORMAT=text` for human-readable output in development.

### Metrics

Prometheus metrics on `http://127.0.0.1:6060/metrics`:

| Metric | Type | Description |
|---|---|---|
| `ac_http_requests_total{method,path,code}` | Counter | HTTP requests by method, path, status |
| `ac_http_request_duration_seconds{method,path}` | Histogram | Request latency |
| `ac_jobs_processed_total{verdict}` | Counter | Jobs completed by verdict |
| `ac_job_processing_duration_seconds` | Histogram | Time from dequeue to verdict |
| `ac_active_workers` | Gauge | Workers currently executing a job |
| `ac_queue_depth{stream}` | Gauge | Messages in each stream (sampled every 15s) |
| `ac_rate_limit_rejections_total{type}` | Counter | Rate-limit rejections by user or IP |
| `ac_poison_messages_total` | Counter | Messages dead-lettered after 5 failed attempts |

### Profiling

pprof on `http://127.0.0.1:6060/debug/pprof/` (localhost only):

```bash
go tool pprof http://127.0.0.1:6060/debug/pprof/profile
```

### Graceful shutdown

`SIGTERM` or `SIGINT` triggers a two-phase shutdown:

1. Stop accepting new jobs from Redis streams.
2. Wait up to 120s for in-flight jobs to complete (covers 90s job timeout + 15s write window + margin).
3. Drain background goroutines (queue-depth scraper, orphan reaper).
4. Shut down HTTP servers (5s window for SSE clients to receive final events).
5. Wait for all Redis pub/sub goroutines to exit.
6. Close Redis and PostgreSQL connections.

### Database migrations

Migrations run automatically at startup. They are embedded in the binary and applied in order. The migration runner holds a PostgreSQL advisory lock so concurrent restarts are safe.

---

## Performance

Benchmarked on a dual-socket, 256-thread Linux host (go-judge parallelism 64, 64 workers):

| Workload | Wall time | Throughput | Lost jobs |
|---|---|---|---|
| 100 concurrent / 400 total | 5.6s | 71/s | 0 |
| 400 concurrent / 1,200 total | 12.2s | 99/s | 0 |
| 800 concurrent / 2,400 total | 21.6s | 111/s | 0 |
| **5,000 concurrent / 15,000 total** | **2m 4s** | **121/s** | **0** |

See `docs/benchmark-results.md` for full latency breakdowns and `docs/spike-results.md` for sandbox selection rationale.

---

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) for the
development workflow, code style, and the design invariants that keep the
engine reliable, and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for community
norms.

## Security

To report a vulnerability, see [SECURITY.md](SECURITY.md). The engine runs
untrusted submissions inside a go-judge sandbox; reports about that sandbox
boundary are especially valuable.

## License

This project is licensed under the
[GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0).

The engine is built to back coding platforms, LMS/ERP integrations, and contest
systems. If you offer it as a network service, the AGPL's network-copyleft
terms (Section 13) require you to make the corresponding source available to
your users — see the license for the full terms.
