# AGENTS.md

Operating rules and reference guide for autonomous/AI agents contributing to **AetherCode Execution Engine** (`stjosephsplacements/AetherCode-Execution-Engine`).
This follows the [agents.md](https://agents.md) convention. `CLAUDE.md` references this file.

---

## Project Snapshot

A high-performance, multi-tenant code execution engine (Rust + Go dual stack after the Pure-Go migration), acting as the pure execution backend for the AetherCode platform:

- **Rust (`crates/`, `Dockerfile.rs`)**: Forward-path rewrite (`ae-engine` binary `engine`) — same HTTP API, Redis streams, Postgres migrations, and Prometheus names; runs beside Go via shadow/cutover.
- **Go ingest (`internal/gateway`)**: `POST /api/v1/execute`, `GET /api/v1/stream` (SSE), `GET /api/v1/submissions/{id}`, `DELETE /api/v1/submissions/{id}`, `GET /api/v1/health`.
- **Queue (`internal/queue` / `ae-queue`)**: Redis 7 Streams with weighted fair consume, poison-message dead-lettering, stream reclaimer, and orphan reaper.
- **Worker (`internal/worker` / `ae-worker`)**: Language-aware compilation with SHA-256 compile cache and batched test execution (up to 32 tests per `/run`).
- **Sandbox (`internal/sandbox` / `ae-sandbox`)**: Client for `go-judge` (cgroups v2 + Linux namespaces + seccomp).
- **Persistence (`internal/db` / `ae-db`)**: PostgreSQL 18 with shared embedded migrations (`migrations/`), advisory locking, RLS, and idempotency.

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

### Rust engine (workspace at repo root; toolchain pinned in `rust-toolchain.toml`)

Dual-engine repo after the Pure-Go → Rust+Go migration: Rust (`crates/`, `Dockerfile.rs`) is the forward path; Go (`cmd/`, `internal/`, `Dockerfile`) stays live through shadow and cutover — do not remove or break it.

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace            # integration tests start Postgres/Redis via Docker (testcontainers)
cargo deny check && cargo audit

# Fuzz (cargo-fuzz, nightly): targets in fuzz/fuzz_targets
cargo +nightly fuzz run normalize_output -- -max_total_time=30
cargo +nightly fuzz run judge_output -- -max_total_time=30

docker build -f Dockerfile.rs -t aethercode-exec-rs:dev .
docker compose --profile rust up --build engine-rs
```

---

## Deployment & Production

- **Docker Compose**: `docker-compose.yml` runs Postgres 18, Redis 7, go-judge, and engine.
- **Kubernetes**: `deploy/k8s/` contains Kustomize manifests including `aethercode-exec` Deployment, `go-judge` DaemonSet with mount config, and isolating NetworkPolicies.
