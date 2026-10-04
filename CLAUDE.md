# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) and other AI coding
assistants when working with code in this repository.

## What this is

AetherCode is a distributed code-execution engine. A Go HTTP gateway accepts
code submissions, persists them, enqueues them on Redis Streams, a worker pool
compiles and runs them inside a [go-judge](https://github.com/criyle/go-judge)
sandbox, writes verdicts to PostgreSQL, and streams the results back to the
client over Server-Sent Events.

It is designed to back coding platforms, LMS/ERP integrations, and contest
systems. It executes **untrusted user code**, so the sandbox boundary and the
reliability invariants below are load-bearing — change them with care and call
out the reasoning.

## Build & test

`make help` lists every target.

```bash
make build      # build the engine binary (aethercode-exec)
make test       # unit tests — hermetic (in-process miniredis, no services)
make lint       # golangci-lint (v1 config schema; pinned to v1.64.x)
make vet
make fmt
make e2e        # end-to-end suite against a running stack on :5100
make up         # start the full docker-compose stack
make down       # stop the stack
```

Docker Compose brings up engine + PostgreSQL 18 + Redis 7 + go-judge. The
go-judge container is `privileged: true` (its sandbox needs cgroups +
namespaces) — fine on a dev box; use the systemd deployment with explicit
limits in production (`deploy/aethercode-exec.service`).

The load generator is a throwaway benchmark tool (excluded from the strict
linters on purpose — it uses a fast non-crypto PRNG):

```bash
make loadgen
/tmp/loadgen -concurrency 400 -total 1200 -timeout 120s
```

## Configuration

All config is via `AC_`-prefixed environment variables — see
`deploy/env.example` for the full list and `README.md → Configuration` for
defaults. Minimum to run: `AC_DATABASE_URL` and `AC_JUDGE_TOKEN`. Auth is
optional OIDC (set `AC_AUTH_ENABLED=true` and `AC_OIDC_AUDIENCE`).

## Default addresses

| Service | Default address |
|---|---|
| go-judge (sandbox) | `127.0.0.1:5050` |
| PostgreSQL 18 | `127.0.0.1:5433` |
| Redis 7 | `127.0.0.1:6379` |
| engine gateway | `127.0.0.1:5100` |
| metrics / pprof | `127.0.0.1:6060` |

## Key design decisions (don't reintroduce without reason)

- **Insert before enqueue.** PG row exists before the Redis XADD. This is what makes "zero lost jobs" provable and what the orphan reaper relies on.
- **Subscribe before replay.** SSE handler calls `pubsub.Subscribe` before `GetEventLog`. Reversing this creates a race where a VERDICT published between LRANGE and SUBSCRIBE is silently dropped.
- **writeCtx uses `context.WithoutCancel`.** The write context for DB/publish operations is intentionally detached from the shutdown signal so in-flight jobs survive SIGTERM.
- **XAck uses `context.WithoutCancel` + 3s timeout.** The signal context is cancelled at shutdown; using it for XAck silently drops ACKs and leaves messages in the PEL.
- **No Kafka.** Redis Streams is the only queue. Revisit only for wide event fan-out.
- **Tests never inside the sandbox.** Expected outputs live in the worker. Hidden test inputs never enter the sandbox environment.
- **Compile inside the sandbox.** Compilation also runs with its own CPU/memory limits inside go-judge.

## Architecture

```
POST /execute → gateway → PG insert → Redis XADD → worker XREADGROUP
                                                         ↓
                                              compile (cached) + run tests
                                                         ↓
                                              PG verdict write (CAS)
                                                         ↓
GET /stream → SSE hub ← Redis pub/sub ←────────── publish VERDICT event
```

## Code map

```
cmd/stj-exec/main.go          — startup, shutdown, background goroutines (binary: aethercode-exec)
internal/
  gateway/
    handler.go                — HandleExecute, HandleHealth, ingest semaphore
    sse.go                    — HandleStream, subscribe-before-replay, ownership check
    middleware.go             — LoggingMiddleware, X-Request-Id
    cors.go                   — CORS middleware
    clientip.go               — X-Forwarded-For parsing
  queue/
    stream.go                 — Stream, DualConsumer, StartReclaimer, DrainLegacyStream
    pubsub.go                 — PrepareAndEnqueue, PublishEvent, Subscribe, GetTests
  worker/
    worker.go                 — process(), loadTests(), compile(), runTest(), failSubmission()
    judge.go                  — JudgeOutput (output comparison with trailing-whitespace trim)
  db/
    db.go                     — Connect, Migrate (advisory lock, embedded SQL)
    queries.go                — InsertSubmission, UpdateSubmissionStatus/Verdict, InsertTestResults
    problems.go               — GetProblemVersion, GetTestCases
  sandbox/
    client.go                 — HTTP client for go-judge /run and /version
    language.go               — LangConfig per language, BuildCompileCmd, BuildRunCmd
  auth/
    middleware.go             — OIDC JWT validation, user auto-provisioning
    cache.go                  — TTL cache for tenant/user resolution
    claims.go                 — Identity type, context helpers
  config/config.go            — Load, Validate, env helpers
  eventlog/event.go           — Event type, New, MarshalSSE, IsTerminal
  metrics/metrics.go          — Prometheus gauges/counters/histograms
  model/submission.go         — Submission, Status, Verdict, Mode, TestResult types
  ratelimit/limiter.go        — Sliding-window rate limiter, AdmissionCheck
migrations/
  001_initial.sql             — Core schema
  002_step3.sql               — Tenants, RLS, idempotency index
  003_unique_schema_version.sql
  004_orphan_reaper_index.sql
  005_idempotency_mode.sql    — Adds mode to idempotency unique index
deploy/
  aethercode-exec.service     — systemd unit
  env.example                 — All AC_* config vars with safe defaults
scripts/
  test_e2e.sh                 — Full language/verdict e2e test suite
docs/
  benchmark-results.md        — Load test results
  spike-results.md            — go-judge sandbox evaluation
```

## Roadmap

1. ✅ Sandbox spike — go-judge v1.13.0
2. ✅ Hot path MVP — 7 languages, SSE delivery, insert-before-enqueue
3. ✅ Correctness & security — auth, fair queuing, rate limiting, idempotency, submit mode
4. ✅ Load generator & benchmark — 121 verdicts/s, 0 lost jobs at 5,000 concurrent
5. **Chaos testing** — workers/Redis killed mid-burst
6. Elastic Governor — scale sandbox pool 8 → 96 based on queue depth
7. Product features — LeetCode harnesses, rejudge, contest mode, leaderboards
8. Post-execution analytics — JPlag, AI review, LMS/ERP sync

## Scaling & runtime notes

- Match `AC_WORKER_COUNT` to go-judge's `parallelism` setting; raising one
  without the other just queues work.
- go-judge requires **cgroup v2** on modern hosts (e.g. Ubuntu 24.04).
- For high concurrency, raise `AC_ADMISSION_MAX_QUEUE_DEPTH` and go-judge
  `parallelism` together, and give the engine a dedicated PostgreSQL instance.
