# AGENTS.md

Operating rules and reference guide for autonomous/AI agents contributing to **AetherCode Execution Engine** (`stjosephsplacements/AetherCode-Execution-Engine`).
This follows the [agents.md](https://agents.md) convention. `CLAUDE.md` references this file.

---

## Project Snapshot

A high-performance, multi-tenant code execution engine written in Go, acting as the pure execution backend for the AetherCode platform:

- **Ingest (`internal/gateway`)**: Exposes `POST /api/v1/execute`, `GET /api/v1/stream` (SSE), `GET /api/v1/submissions/{id}`, `DELETE /api/v1/submissions/{id}`, and `GET /api/v1/health`.
- **Queue (`internal/queue`)**: Redis 7 Streams with a `DualConsumer` (weighted fair queue), poison-message dead-lettering, stream reclaimer, and orphan reaper.
- **Worker (`internal/worker`)**: Language-aware compilation with an in-memory SHA-256 compile cache and batched test execution (up to 32 tests per `/run`).
- **Sandbox (`internal/sandbox`)**: Go client communicating with `go-judge` (cgroups v2 + Linux namespaces + seccomp sandbox).
- **Persistence (`internal/db`)**: PostgreSQL 18 with embedded migrations (`migrations/`), advisory locking, row-level security (RLS), and idempotency.

---

## Prime Directives

1. **Ship working code only** — zero placeholders, no mock data, no unverified `TODO`s.
2. **Pure Executor Architecture** — the engine never stores or owns problems, problem versions, or curriculum test cases. The caller always passes tests in the request body.
3. **Commit Authorship** — always ensure commits are authored by the repository owner (`Shreesh <shreesh.exe22@gmail.com>`).
4. **Branch Synchronization** — `development` and `production` must remain in sync with 0 lingering branches and 0 open PRs.
5. **Code Quality & CI Audits** — all PRs must pass `lint`, `test`, `CodeQL`, `secrets-scan`, and race detector checks before squash-merging.

---

## Development & Test Commands

```bash
# Run all unit tests
go test ./...

# Run tests with race detector and coverage
go test -race -count=1 -timeout=120s ./...

# Run fuzz tests
go test -fuzz=FuzzNormalizeOutput -fuzztime=30s ./internal/worker
go test -fuzz=FuzzJudgeOutput -fuzztime=30s ./internal/worker

# Run benchmarks
go test -bench=. -benchmem ./internal/worker/...

# Linting & Formatting
gofmt -s -w .
golangci-lint run ./...
```

---

## Deployment & Production

- **Docker Compose**: `docker-compose.yml` runs Postgres 18, Redis 7, go-judge, and engine.
- **Kubernetes**: `deploy/k8s/` contains Kustomize manifests including `aethercode-exec` Deployment, `go-judge` DaemonSet with mount config, and isolating NetworkPolicies.
