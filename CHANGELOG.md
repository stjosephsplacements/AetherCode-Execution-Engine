# Changelog

All notable changes to the AetherCode Execution Engine are documented here.

This project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and uses [semantic versioning](https://semver.org/).

## [0.1.0] — 2026-10-04

First open-source release.

### Added

- **Gateway** — HTTP ingest (`POST /api/v1/execute`), SSE delivery
  (`GET /api/v1/stream`), logging / CORS / client-IP middleware, and
  Prometheus metrics + pprof.
- **Queue** — Redis 7 Streams with a `DualConsumer` (weighted submit:run),
  a reclaimer via `XAUTOCLAIM`, poison-message dead-lettering, and an
  orphan reaper.
- **Worker** — language-aware compile (with caching) and per-test run for
  C, C++, Java 21, Go, Python 3, JavaScript, and SQLite; verdict
  normalization with trailing-whitespace tolerance.
- **Sandbox** — go-judge client with per-language compile/run command
  builders and CPU/memory/output limits.
- **Persistence** — PostgreSQL 18 as the system of record, embedded
  migrations guarded by an advisory lock, tenant RLS, and idempotency.
- **Auth** — optional OIDC (Zitadel) JWT validation with tenant/user
  auto-provisioning and a TTL resolution cache.
- **Rate limiting & admission** — sliding-window per-user limits plus a
  queue-depth admission gate.
- **Releases & tooling** — AGPL-3.0 license, `Makefile`, root and
  go-judge `Dockerfile`s, `docker-compose.yml` full dev stack,
  `deploy/` systemd unit + env example, and `.github/workflows/ci.yml`
  (build, vet, gofmt, `go test -race`, golangci-lint).
- **Tests** — hermetic unit tests for the judge, event log, client-IP
  parsing, config validation, and the rate limiter (in-process miniredis).

### Verified

- Zero lost jobs at up to 5,000 concurrent connections / 15,000 submissions
  (see `docs/benchmark-results.md`).

## [Unreleased]

### Added

- Per-request `checker` field on `POST /api/v1/execute` (`exact` | `token` | `float`).

### Removed — BREAKING

- Migration 007 drops legacy problem bank tables (`problems`, `problem_versions`, `test_cases`), obsolete columns `submissions.problem_version_id` and `submissions.mode`, and the legacy 4-column idempotency index `idx_submissions_idempotency`.
- `POST /api/v1/execute` no longer accepts `mode` or `problem_version_id`; the engine is a pure executor. Requests with `mode` other than `run`, or with any `problem_version_id`, now get `400` instead of being silently ignored; a leftover `"mode":"run"` is tolerated.
- `tests` is always required (at least one). Callers that used `mode: submit` must now send their own tests.
- Idempotency keys are no longer scoped by mode: the same key and user returns the original job ID.
- Every new job receives a `stream_token` (previously only unauthenticated run-mode jobs).
- Per-test results now always include full `stdout`/`stderr` and previews (no hidden-test masking).
- Migration 006 clears the idempotency key on later duplicates that reused a key across run/submit (the oldest row keeps it).
- Upgrade order: move callers off `mode: submit` (send `tests`) before deploying this release.
- Unauthenticated `DELETE /api/v1/submissions/{id}` requires `stream_token`; the `404` for an unknown problem version is gone.

### Changed

- Requests accept `time_limit_ms` and `memory_limit_kb`; runs also get a wall-clock limit (2x CPU + 1 s).
- VERDICT results include the complete `stdout`/`stderr`; test data up to 8 MB, output up to 8 MB.
- Job deadline grows to the worst case of its tests and limits instead of cutting off late tests.
- Unauthenticated requests are no longer rate limited by IP; `AC_TRUST_PROXY` is removed.
- Output comparison treats CRLF and lone CR as line breaks.
