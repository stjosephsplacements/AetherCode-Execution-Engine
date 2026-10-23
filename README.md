# AetherCode Execution Engine

<p align="center">
  <img src="AetherCode.png" alt="AetherCode Execution Engine" width="100%">
</p>

An enterprise-grade, high-throughput, on-premise distributed code execution and evaluation engine. AetherCode ingests untrusted code submissions over HTTP, safely runs them inside hardware-isolated Linux kernel sandboxes (cgroups v2, seccomp-bpf, network namespaces, and tmpfs overlays), and streams real-time execution lifecycle events and test verdicts back to clients via Server-Sent Events (SSE).

<p align="center">
  <a href="https://github.com/stjosephsplacements/AetherCode-Execution-Engine/actions/workflows/ci.yml"><img src="https://github.com/stjosephsplacements/AetherCode-Execution-Engine/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-AGPL%20v3-blue.svg" alt="License: AGPL-3.0"></a>
  <img src="https://img.shields.io/badge/Go-1.27%2B-00ADD8?logo=go&logoColor=white" alt="Go">
  <img src="https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white" alt="Docker">
  <img src="https://img.shields.io/badge/Architecture-Distributed%20%26%20Zero--Allocation-success" alt="Architecture">
</p>

**Operational Status:** Battle-tested and production-hardened. Zero lost jobs verified under 5,000 concurrent sustained connections and 15,000 burst submissions.

> **License:** Licensed under the [GNU Affero General Public License v3.0 (AGPLv3)](LICENSE). If you operate or integrate this engine as a network-accessible service, Section 13 requires providing complete corresponding source code to network users. Please review [License](#license) for complete compliance details.

---

## Table of Contents

- [Key Architectural Highlights](#key-architectural-highlights)
- [Quickstart Guide](#quickstart-guide)
  - [Prerequisites](#prerequisites)
  - [Docker Compose Deployment](#docker-compose-deployment)
  - [Local Source Compilation](#local-source-compilation)
  - [Systemd Production Service](#systemd-production-service)
  - [Verification and Benchmarking](#verification-and-benchmarking)
- [System Architecture](#system-architecture)
  - [Execution Flow](#execution-flow)
  - [Weighted Fair Queuing](#weighted-fair-queuing)
  - [Reliability and Self-Healing Invariants](#reliability-and-self-healing-invariants)
  - [Zero-Allocation Judge Evaluator](#zero-allocation-judge-evaluator)
- [Configuration Reference](#configuration-reference)
  - [Environment Variables](#environment-variables)
  - [Production Tuning Checklist](#production-tuning-checklist)
- [API Reference](#api-reference)
  - [POST /api/v1/execute](#post-apiv1execute)
  - [GET /api/v1/stream](#get-apiv1stream)
  - [GET /api/v1/health](#get-apiv1health)
  - [GET /api/v1/languages](#get-apiv1languages)
  - [GET /api/v1/submissions/{id}](#get-apiv1submissionsid)
  - [DELETE /api/v1/submissions/{id}](#delete-apiv1submissionsid)
- [Supported Languages and Runtime Limits](#supported-languages-and-runtime-limits)
- [Operations and Observability](#operations-and-observability)
  - [Structured Logging](#structured-logging)
  - [Prometheus Metrics](#prometheus-metrics)
  - [Continuous Profiling (pprof)](#continuous-profiling-pprof)
  - [Graceful Two-Phase Shutdown](#graceful-two-phase-shutdown)
  - [Automated Database Migrations](#automated-database-migrations)
- [Performance Benchmarks](#performance-benchmarks)
- [Release and Deployment Pipeline](#release-and-deployment-pipeline)
- [Contributing](#contributing)
- [Security Model](#security-model)
- [License](#license)

---

## Key Architectural Highlights

- **Multi-Tenant Isolation and Sandbox Boundary:** Submissions execute within secure, unprivileged containers backed by `go-judge`. Every execution is bounded by Linux cgroups v2 (CPU time, memory quotas, swap isolation), PID limits, wall-clock watchdog timers, read-only root filesystems, and syscall filters.
- **Real-Time Streaming Lifecycle via SSE:** Clients receive immediate feedback across state changes (`QUEUED`, `COMPILING`, `RUNNING`, `TEST_RESULT`, `VERDICT`) over an efficient HTTP Server-Sent Events channel with automatic reconnect resilience and replay buffers.
- **Zero-Allocation Stream Output Evaluation:** Output evaluation (`exact`, `token`, `float`) runs through an in-place whitespace scanner that executes with zero heap allocations and 0 B/op memory footprint.
- **Interactive Problems Support:** Full bidirectional inter-process communication (`pipeMapping`) connects student solutions with reactive interactor binaries live in the sandbox.
- **Multi-File Projects & Whitelisted Compiler Flags:** Supports multi-file project archives with strict path traversal validation and fine-grained compiler optimization flags (`-O3`, `-std=c++20`).
- **Custom Interactive Checkers:** Supports special judges and `testlib.h`-style checkers in C++, Python, or Go with SHA-256 binary caching.
- **HMAC-Signed Webhooks:** Provides automated asynchronous webhook callbacks (`X-AetherCode-Signature`) with exponential retries for bulk evaluations.
- **Prepared Statement Caching & Query Pipelining:** Configured with `pgx.QueryExecModeCacheDescribe` on all database pools, eliminating redundant query planning and parsing round-trips.
- **Dead-Letter Queue (DLQ) Stream Routing:** Failed submissions exceeding maximum retry thresholds are automatically extracted by background reclaimers and deposited into a dedicated `ac:dlq:poison` Redis stream for forensic inspection.
- **Compilation Artifact Caching:** Compiled binaries (C, C++, Go, Java) are deduplicated by cryptographic SHA-256 hashes and cached across worker nodes, eliminating redundant compile cycles and reducing p95 latency by up to 1,200 ms.
- **3-Tier Weighted Fair Queuing (WFQ):** Redis 7 Streams utilize a weighted consumer model (`ac:submissions:run`, `ac:submissions:submit`, and `ac:submissions:bulk`) preventing background batch workloads from starving interactive user test runs.
- **Atomic State Persistence:** PostgreSQL 18 state transitions use Compare-And-Swap (CAS) semantics, preventing race conditions, duplicate grading runs, and inconsistent terminal states.

---

## Quickstart Guide

### Prerequisites

| Component | Minimum Version | Default Network Address | Function |
|---|---|---|---|
| Go | 1.27+ | Localhost | Compilation toolchain and test suite |
| PostgreSQL | 18 | `127.0.0.1:5433` | Submissions, results, and RLS tenant records |
| Redis | 7+ | `127.0.0.1:6379` | Priority message streams, pub/sub, rate limiting, and cache |
| go-judge | v1.13.0 | `127.0.0.1:5050` | Isolated containerized execution sandbox |

### Docker Compose Deployment

The quickest path to spinning up the entire infrastructure stack (Gateway, Workers, PostgreSQL, Redis, and go-judge sandbox) is using Docker Compose:

```bash
# Build images and launch services in detached mode
make up

# Wait for healthy cluster response
curl -fsSL http://127.0.0.1:5100/api/v1/health | jq .

# Stop the stack and cleanup containers when finished
make down
```

> **Security Note:** In the local development compose file, the sandbox container runs with `privileged: true` solely to mount cgroups and configure nested namespaces on dev kernels. For bare-metal or cloud production deployments, run go-judge directly as a dedicated unprivileged system daemon or Kubernetes pod with configured cgroups v2 controllers.

### Local Source Compilation

To build and run directly on a host machine:

```bash
# 1. Compile binary
go build -o aethercode-exec ./cmd/stj-exec

# 2. Setup configuration file
sudo mkdir -p /etc/aethercode-exec
sudo cp deploy/env.example /etc/aethercode-exec/env

# 3. Configure mandatory environment variables (database credentials, judge auth token)
sudo nano /etc/aethercode-exec/env

# 4. Start execution engine
./aethercode-exec
```

### Systemd Production Service

For persistent host deployment under Linux systemd:

```bash
# Copy systemd unit file and environment configuration
sudo cp deploy/aethercode-exec.service /etc/systemd/system/
sudo mkdir -p /etc/aethercode-exec
sudo cp deploy/env.example /etc/aethercode-exec/env

# Reload systemd daemon and activate service
sudo systemctl daemon-reload
sudo systemctl enable --now aethercode-exec

# Inspect live status
sudo systemctl status aethercode-exec
```

### Verification and Benchmarking

```bash
# 1. Service health check
curl -s http://127.0.0.1:5100/api/v1/health | jq .

# 2. Comprehensive end-to-end multi-language test suite
bash scripts/smoke_test.sh

# 3. Execute load generator (400 concurrent clients across 1,200 jobs)
go build -o /tmp/loadgen ./cmd/loadgen
/tmp/loadgen -concurrency 400 -total 1200 -timeout 120s
```

---

## System Architecture

<p align="center">
  <img src="Architecture.png" alt="AetherCode Execution Engine System Architecture" width="100%">
</p>

### Execution Flow

1. **Ingestion (`POST /api/v1/execute`):** The HTTP gateway checks rate limits and queue depths, validates schema constraints, generates a UUIDv7 job identifier, inserts the submission into PostgreSQL, and enqueues the payload into Redis alongside test case structures in an optimized Redis pipeline.
2. **Event Subscription (`GET /api/v1/stream?job_id=...`):** Clients establish an SSE connection. The gateway subscribes to the Redis pub/sub channel before replaying historical event logs to eliminate message-delivery race conditions.
3. **Worker Processing:** Available worker goroutines dequeue jobs using `XREADGROUP`. For compiled languages, source code is checked against the compilation hash cache. If missing, it compiles the binary, stores the artifact file ID, and caches it.
4. **Sandboxed Test Runner:** The worker batches test cases into `/run` execution payloads against go-judge. Tests run with strict cgroup CPU and memory boundaries.
5. **Output Evaluation:** Actual output streams through the zero-allocation judge engine against expected test case outputs.
6. **Verdict Persistence and Event Dispatch:** The worker commits test verdicts and aggregate statistics to PostgreSQL via CAS updates, then broadcasts the terminal `VERDICT` event over Redis pub/sub to close client streams.

### Weighted Fair Queuing

The execution engine organizes execution traffic across Redis 7 Streams to prevent starvation:

- `ac:submissions:run`: Interactive code runs initiated by active users (P0 priority).
- `ac:submissions:submit`: Formal graded contest and assessment submissions (P1 priority).
- `ac:submissions:bulk`: Bulk re-evaluations and backfills (P2 priority).

The `DualConsumer` driver executes weighted fair queuing: it processes configured weight ratios of interactive jobs before processing submit jobs. When all streams run dry, it invokes an efficient multi-stream blocking read (`XREADGROUP ... BLOCK`), avoiding busy-wait CPU spinning.

### Reliability and Self-Healing Invariants

- **Orphan Job Reaper:** A background monitor periodically inspects PostgreSQL for submissions remaining in `queued` state longer than two minutes, re-enqueuing lost jobs into Redis.
- **Automatic Stream Reclaimer (`XAUTOCLAIM`):** If a worker node experiences sudden hardware failure or crash, unacknowledged pending messages idle for over 30 seconds are reclaimed and transferred to surviving workers.
- **Poison Message Dead-Letter Queue (DLQ):** Messages exceeding 5 failed processing attempts are flagged as poison messages, acknowledged out of the main stream, and published to `ac:dlq:poison` with complete diagnostic metadata.
- **Compare-And-Swap (CAS) Writes:** All PostgreSQL updates employ `WHERE status NOT IN ('completed', 'failed')` conditional queries, safeguarding state integrity against double execution.
- **Crash Recovery:** Worker threads and reclaimer loops wrap top-level handlers with panic recovery and backoff restart procedures.

### Zero-Allocation Judge Evaluator

The output evaluation subsystem (`internal/worker/judge.go`) provides three grading strategies:

- `exact`: Normalizes line endings (`\r\n` to `\n`) and trims trailing whitespace on every line using zero-allocation streaming cursors without allocating intermediate normalized string buffers or slices.
- `token`: Evaluates tokens delimited by whitespace using cursor offset indices, comparing tokens directly from input buffers.
- `float`: Scans numeric tokens without allocating string arrays, parsing tokens via `strconv.ParseFloat` and validating differences against an absolute epsilon of `1e-6`.

Microbenchmarks demonstrate sustained 0 B/op and 0 allocs/op across `exact`, `token`, and `float` comparison passes during continuous evaluation cycles.

---

## Configuration Reference

### Environment Variables

Configuration parameters are loaded from environment variables on startup. Reference template: `deploy/env.example`.

| Variable | Default Value | Description |
|---|---|---|
| `AC_GATEWAY_ADDR` | `127.0.0.1:5100` | Gateway HTTP service listen address |
| `AC_DATABASE_URL` | `postgres://aethercode:aethercode@127.0.0.1:5433/aethercode_exec` | PostgreSQL connection DSN |
| `AC_REDIS_ADDR` | `127.0.0.1:6379` | Redis host address and port |
| `AC_JUDGE_URL` | `http://127.0.0.1:5050` | go-judge sandbox REST API endpoint |
| `AC_JUDGE_TOKEN` | *(Required)* | Secret bearer token authenticating requests to go-judge |
| `AC_WORKER_COUNT` | `8` | Total concurrent worker execution goroutines |
| `AC_JOB_TIMEOUT` | `90s` | Maximum default execution budget per submission |
| `AC_LOG_LEVEL` | `info` | Logging verbosity: `debug`, `info`, `warn`, `error` |
| `AC_LOG_FORMAT` | `json` | Logging format: `json` (production) or `text` (development) |
| `AC_AUTH_ENABLED` | `false` | Enable Zitadel OIDC JWT token verification |
| `AC_OIDC_ISSUER` | `https://sso.example.com` | Expected OIDC identity issuer URL |
| `AC_OIDC_JWKS_URL` | `http://127.0.0.1:8087/oauth/v2/keys` | Internal JWKS public key endpoint |
| `AC_OIDC_AUDIENCE` | *(Required if auth enabled)* | Target application client ID |
| `AC_RATE_LIMIT_PER_MINUTE` | `30` | Maximum submissions allowed per authenticated user per minute |
| `AC_RATE_LIMIT_BURST` | `10` | Maximum token burst capacity per user |
| `AC_RATE_LIMIT_UNAUTH_PER_MINUTE` | `0` | Per-IP rate limit for unauthenticated callers (0 disables) |
| `AC_RATE_LIMIT_UNAUTH_BURST` | `5` | Burst allowance for unauthenticated IP calls |
| `AC_ADMISSION_MAX_QUEUE_DEPTH` | `5000` | Stream depth ceiling beyond which new jobs are rejected with 429 |
| `AC_SUBMIT_WEIGHT` | `3` | Fair queuing weight of submit queue relative to run queue |
| `AC_CORS_ALLOWED_ORIGINS` | *(Empty, disabled)* | Comma-delimited list of accepted browser origins |
| `AC_STREAM_NAME` | `ac:submissions` | Legacy single-stream fallback key |
| `AC_SUBMIT_STREAM` | `ac:submissions:submit` | Redis stream key for submit jobs |
| `AC_RUN_STREAM` | `ac:submissions:run` | Redis stream key for run jobs |
| `AC_CONSUMER_GROUP` | `workers` | Redis Stream consumer group name |
| `AC_TEST_PARALLELISM` | `4` | Concurrency factor for batched test case runs |
| `AC_COMPILE_CACHE_TTL` | `60s` | In-memory compilation artifact cache expiration |
| `AC_BLOCK_READ_TIMEOUT` | `500ms` | Timeout for blocking reads on empty streams |
| `AC_GOGC` | `200` | Garbage collection target percentage for Go runtime |
| `AC_SANDBOX_STDOUT_MAX_BYTES` | `8388608` (8 MB) | Maximum standard output captured per test |
| `AC_SANDBOX_STDERR_MAX_BYTES` | `1048576` (1 MB) | Maximum standard error captured per test |

### Production Tuning Checklist

- [ ] **Secret Authentication:** Change `AC_JUDGE_TOKEN` to a cryptographically secure random string.
- [ ] **Identity Integration:** Activate `AC_AUTH_ENABLED=true` and configure matching `AC_OIDC_AUDIENCE` credentials.
- [ ] **Worker Sizing:** Align `AC_WORKER_COUNT` with host CPU core density and go-judge container parallelism (for example, 64 workers on 64 physical cores).
- [ ] **Queue Capacity:** Adjust `AC_ADMISSION_MAX_QUEUE_DEPTH` to `20000` or higher when handling high-concurrency contest events.
- [ ] **Cross-Origin Protection:** Restrict `AC_CORS_ALLOWED_ORIGINS` strictly to authorized web app domains.

---

## API Reference

Default Base URL: `http://127.0.0.1:5100`

All endpoints return JSON responses. Every request emits a unique `X-Request-Id` response header for distributed tracing.

---

### POST /api/v1/execute

Submit code for evaluation against test cases.

**Request Headers:**
- `Content-Type: application/json` (Required)
- `Authorization: Bearer <token>` (Optional; provides caller identity for user rate-limiting and access policies)
- `Idempotency-Key: <string>` (Optional; deduplicates retry attempts up to 255 characters)

**Request Body Schema:**

```json
{
  "language": "python",
  "source_code": "import sys\nlines = sys.stdin.read().split()\nif lines:\n    print(sum(map(int, lines)))",
  "tests": [
    {
      "input": "10 20 30\n",
      "expected_output": "60\n"
    },
    {
      "input": "5 -2\n",
      "expected_output": "3\n"
    }
  ],
  "time_limit_ms": 2000,
  "memory_limit_kb": 262144,
  "checker": "token"
}
```

**Field Specifications:**

| Field | Type | Required | Description |
|---|---|---|---|
| `language` | string | Yes | Supported language key: `c`, `cpp`, `java`, `go`, `python`, `javascript`, `sqlite`, etc. |
| `source_code` | string | Yes | Primary source text up to 64 KB |
| `tests` | array | Yes | Array of 1 to 100 test objects |
| `tests[].input` | string | Yes | Test standard input (max 8 MB per test) |
| `tests[].expected_output` | string | Yes | Expected test standard output (max 8 MB per test) |
| `checker` | string | No | Output comparison mode: `exact` (default), `token`, `float`, or `custom` |
| `checker_code` | string | No | Custom checker source code (required when `checker: "custom"`, max 64 KB) |
| `checker_language` | string | No | Custom checker language (defaults to `python`) |
| `interactor_code` | string | No | Interactor source code for bidirectional interactive problems (max 64 KB) |
| `interactor_language` | string | No | Interactor language (defaults to `python`) |
| `files` | object | No | Multi-file submission map `{"helper.h": "...", "sol.cpp": "..."}` (max 10 files, 128 KB total) |
| `compiler_flags` | array | No | Whitelisted optimization/standard flags (`["-O3", "-std=c++20"]`) |
| `callback_url` | string | No | Optional HTTP/HTTPS webhook URL for asynchronous completion callback |
| `callback_secret` | string | No | Optional HMAC-SHA256 signature secret for webhook validation |
| `priority` | string | No | Priority tier: `run` (P0 default), `submit` (P1), or `bulk` (P2) |
| `time_limit_ms` | integer | No | Per-test CPU time ceiling in milliseconds (100 to 20000) |
| `memory_limit_kb` | integer | No | Per-test memory ceiling in kilobytes (16384 to 2097152) |
| `idempotency_key` | string | No | Client key; duplicate submissions return the original job identifier |

**Successful Response (`201 Created`):**

```json
{
  "job_id": "019481a5-8cd3-7b8f-a912-9876543210ef",
  "stream_token": "a8f3b49910c82de471b802"
}
```

*Note:* `stream_token` is generated for unauthenticated clients to authorize subsequent calls to `GET /api/v1/stream` and cancellation requests.

**HTTP Error Codes:**
- `400 Bad Request`: Payload validation failure (unsupported language, empty source, missing tests, invalid limits).
- `401 Unauthorized`: Invalid or expired Bearer token.
- `415 Unsupported Media Type`: `Content-Type` header is not `application/json`.
- `429 Too Many Requests`: User rate limit exceeded or system admission queue depth reached. Check `Retry-After` header.
- `500 Internal Server Error`: Internal system failure.

---

### GET /api/v1/stream

Subscribe to real-time execution lifecycle events using Server-Sent Events (SSE).

**Query Parameters:**
- `job_id`: Target job UUID returned by `POST /api/v1/execute` (Required).
- `stream_token`: Token returned by execute endpoint (Required for unauthenticated callers).

**Request Headers:**
- `Last-Event-ID`: Resume stream playback from specific event ID (Optional; handled automatically by browser `EventSource`).
- `Authorization: Bearer <token>`: Required when authentication is active and no stream token is present.

**Response Type:** `text/event-stream`

#### Lifecycle Events

**1. QUEUED Event:**
```
id: 019481a5-8cd3-7b8f-a912-9876543210ef:1729680000000-1
event: QUEUED
data: {"id":"...","job_id":"019481a5-8cd3-7b8f-a912-9876543210ef","type":"QUEUED","ts":1729680000000}
```

**2. COMPILING Event (Compiled languages only):**
```
id: 019481a5-8cd3-7b8f-a912-9876543210ef:1729680000100-2
event: COMPILING
data: {"id":"...","job_id":"019481a5-8cd3-7b8f-a912-9876543210ef","type":"COMPILING","ts":1729680000100}
```

**3. RUNNING Event:**
```
id: 019481a5-8cd3-7b8f-a912-9876543210ef:1729680000350-3
event: RUNNING
data: {"id":"...","job_id":"019481a5-8cd3-7b8f-a912-9876543210ef","type":"RUNNING","ts":1729680000350}
```

**4. VERDICT Event (Terminal event, stream closes immediately after):**
```
id: 019481a5-8cd3-7b8f-a912-9876543210ef:1729680000720-4
event: VERDICT
data: {
  "id": "...",
  "job_id": "019481a5-8cd3-7b8f-a912-9876543210ef",
  "type": "VERDICT",
  "ts": 1729680000720,
  "data": {
    "verdict": "accepted",
    "test_count": 2,
    "tests_passed": 2,
    "cpu_time_ns": 45100000,
    "memory_bytes": 14221312,
    "results": [
      {
        "test_index": 0,
        "verdict": "accepted",
        "cpu_time_ns": 23000000,
        "memory_bytes": 14221312,
        "stdout_preview": "60\n",
        "stderr_preview": ""
      },
      {
        "test_index": 1,
        "verdict": "accepted",
        "cpu_time_ns": 22100000,
        "memory_bytes": 13959168,
        "stdout_preview": "3\n",
        "stderr_preview": ""
      }
    ]
  }
}
```

#### Verdict Status Reference

| Verdict Name | Description |
|---|---|
| `accepted` | All test cases executed and matched expected output successfully. |
| `wrong_answer` | Execution finished within limits, but actual output did not match expected output. |
| `time_limit` | Process exceeded allocated CPU duration limit or wall-clock watchdog ceiling. |
| `memory_limit` | Process was terminated by cgroup memory quota enforcement. |
| `output_limit` | Standard output generated exceeded 8 MB buffer threshold. |
| `runtime_error` | Process exited with non-zero status code or received an unhandled POSIX signal. |
| `compilation_error` | Compiler returned non-zero status. Diagnostic details provided in `compile_stderr`. |
| `cancelled` | Submission was cancelled via HTTP DELETE request prior to execution completion. |
| `internal_error` | Sandbox environment failure, database communication disruption, or system timeout. |

**Stream Maintenance Signals:**
- **Heartbeat:** To keep intermediate proxies and firewalls alive, a comment ping (`: heartbeat`) is emitted every 15 seconds.
- **Auto-Reconnection (`: reconnect`):** If backend communication drops temporarily, the server issues a reconnect directive before closing, prompting browser `EventSource` clients to reconnect with stored `Last-Event-ID`.

---

### GET /api/v1/health

Comprehensive health check querying PostgreSQL, Redis, and sandbox subsystems.

**Response Example (`200 OK`):**

```json
{
  "status": "healthy",
  "version": "v0.1.0",
  "go_version": "go1.27.2",
  "uptime_seconds": 18450,
  "checks": {
    "database": "ok",
    "sandbox": "ok",
    "redis": "ok"
  },
  "sandbox_version": "1.13.0",
  "workers": {
    "active": 4,
    "configured": 16
  },
  "queues": {
    "ac:submissions:run": 0,
    "ac:submissions:submit": 2
  }
}
```

---

### GET /api/v1/languages

Returns metadata, compiler versions, and default constraints for supported language environments.

---

### GET /api/v1/submissions/{id}

Fetches the current persistent database record and verdict summary for a submission ID.

---

### DELETE /api/v1/submissions/{id}

Cancels an active or queued submission. Marks the submission record as `failed` with verdict `cancelled`. Requires bearer authorization or valid query parameter `?stream_token=...`.

---

## Supported Languages and Runtime Limits

| Language Identifier | Compiler / Runtime Environment | Compilation Step | Default CPU Limit | Default Memory Quota |
|---|---|---|---|---|
| `c` | GCC 13.3 (`-O2 -pipe -lm`) | Yes | 2.0 s | 512 MB |
| `cpp` | G++ 13.3 (`-O2 -pipe -std=c++17`) | Yes | 2.0 s | 512 MB |
| `java` | OpenJDK 21 (TieredCompilation level 1) | Yes | 2.0 s | 512 MB (Min 256 MB) |
| `go` | Go 1.22 (`go build -o main`) | Yes | 2.0 s | 512 MB |
| `python` | CPython 3.12 | No | 2.0 s | 512 MB |
| `javascript` | Node.js 24 LTS | No | 2.0 s | 512 MB |
| `sqlite` | SQLite 3.45 (`sqlite3 -batch`) | No | 2.0 s | 512 MB |

Compilation phase limits: C and C++ allow 10 s CPU and 256 MB RAM; Java allows 15 s CPU and 512 MB RAM; Go allows 30 s CPU and 1 GB RAM.

---

## Operations and Observability

### Structured Logging

The engine outputs structured logs formatted as single-line JSON records on standard error:

```bash
# Monitor live systemd logs with jq formatting
journalctl -u aethercode-exec -f -o cat | jq .
```

Standard log attributes include `timestamp`, `level`, `submission_id`, `request_id`, `method`, `path`, `status`, `duration_ms`, and `remote_ip`.

### Prometheus Metrics

Instrumentation metrics are served on a separate administrative HTTP port at `http://127.0.0.1:6060/metrics`:

| Metric Name | Type | Description |
|---|---|---|
| `ac_http_requests_total{method,path,code}` | Counter | Total HTTP requests classified by route and status |
| `ac_http_request_duration_seconds{method,path}` | Histogram | Latency histogram for HTTP gateway requests |
| `ac_jobs_processed_total{verdict}` | Counter | Total completed evaluation jobs by verdict category |
| `ac_job_processing_duration_seconds` | Histogram | Execution time from dequeue to completion |
| `ac_compile_duration_seconds{language}` | Histogram | Time spent in compilation step per language |
| `ac_compile_cache_hits_total` | Counter | Compilation requests satisfied from in-memory cache |
| `ac_compile_cache_misses_total` | Counter | Compilation requests requiring compiler execution |
| `ac_active_workers` | Gauge | Count of worker goroutines currently executing jobs |
| `ac_queue_depth{stream}` | Gauge | Live message depth sampled every 15 seconds across streams |
| `ac_rate_limit_rejections_total{type}` | Counter | Rate limit denials categorized by user or IP address |
| `ac_poison_messages_total` | Counter | Cumulative messages routed to dead-letter queue |

### Continuous Profiling (pprof)

Go runtime profiling endpoints are exposed on localhost at `http://127.0.0.1:6060/debug/pprof/`:

```bash
# Analyze CPU profile
go tool pprof http://127.0.0.1:6060/debug/pprof/profile?seconds=30

# Inspect heap memory allocation
go tool pprof http://127.0.0.1:6060/debug/pprof/heap
```

### Graceful Two-Phase Shutdown

Upon receiving `SIGTERM` or `SIGINT`, the engine executes an ordered two-phase shutdown:

1. **Stop Ingestion:** Suspends reading new messages from Redis streams and stops accepting incoming execute requests.
2. **Drain In-Flight Workloads:** Allows running worker threads up to 120 seconds to finalize active test executions and persist verdicts.
3. **Terminate Background Jobs:** Halts orphan reapers and queue depth monitors.
4. **Flush SSE Streams:** Waits 5 seconds for connected clients to receive terminal events.
5. **Close Connection Pools:** Shuts down Redis clients and releases PostgreSQL pool connections cleanly.

### Automated Database Migrations

Embedded database migrations (`migrations/`) run automatically at service initialization. Concurrent instances coordinate via PostgreSQL transactional advisory locks, eliminating migration race conditions across auto-scaled replicas.

---

## Performance Benchmarks

Conducted on a dual-socket, 256-thread Linux host with 512 GB RAM and NVMe storage (go-judge parallelism 64, 64 engine workers):

| Concurrency Profile | Total Submissions | Total Duration | Mean Throughput | Jobs Lost |
|---|---|---|---|---|
| 100 concurrent clients | 400 jobs | 5.6 s | 71.4 jobs/sec | 0 (0.00%) |
| 400 concurrent clients | 1,200 jobs | 12.2 s | 98.4 jobs/sec | 0 (0.00%) |
| 800 concurrent clients | 2,400 jobs | 21.6 s | 111.1 jobs/sec | 0 (0.00%) |
| **5,000 concurrent clients** | **15,000 jobs** | **124.0 s** | **121.0 jobs/sec** | **0 (0.00%)** |

Review `docs/benchmark-results.md` for in-depth percentiles (p50, p95, p99) and latency profiles.

---

## Release and Deployment Pipeline

The repository follows a trunk-based promotion model with strict branch protection rules:

```
feature/* / fix/*  ──>  development (CI Gate)  ──>  production (Protected Gate)
```

1. **Development Branch (`development`):** Active baseline branch. Topic branches (`feat/*`, `fix/*`, `docs/*`, `perf/*`) require full green status on CodeQL security scanning, unit testing, formatting checks, and benchmark validation before squashing.
2. **Production Branch (`production`):** Stable branch representing production deployments. Pull requests from `development` into `production` are verified by full CI test matrices and branch protection rulesets.
3. **Automated Bots:** Automated workflows handle issue assignment (FCFS with active assignment limits), pull request triage, merge conflict notifications, and stale assignment reclamation.

---

## Contributing

Contributions are welcome. Please consult:
- [CONTRIBUTING.md](CONTRIBUTING.md): Workflow guidelines, development standards, and testing rules.
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md): Community engagement standards.
- [AGENTS.md](AGENTS.md): Architectural instructions for automated coding agents.

Before submitting pull requests:
```bash
# Format code
gofmt -s -w .

# Run test suite
go test -v -race ./...

# Run static linter
golangci-lint run
```

---

## Security Model

Security vulnerability reporting procedures are detailed in [SECURITY.md](SECURITY.md).

### Sandbox Mount Security

Untrusted user code executes in an isolated environment governed by `docker/go-judge/mount.yaml`. Host directories like `/home`, `/root`, and cloud credential paths are strictly omitted from mounts. Only immutable system runtime libraries (`/lib`, `/usr/lib`, `/bin`) are exposed in read-only mode (`READONLY_DIR`), backed by ephemeral tmpfs scratch mounts.

---

## License

This project is licensed under the [GNU Affero General Public License v3.0 (AGPL-3.0)](LICENSE).

AetherCode is engineered to power coding platforms, competitive programming portals, educational LMS systems, and enterprise assessments. If you modify or host this software to deliver network-accessible execution services, Section 13 mandates that you provide complete corresponding source code to all network users. Please review the [full AGPL-3.0 license text](LICENSE) for terms.
