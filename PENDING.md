# Pending Work: Rust Execution Engine

Status as of 2026-11-09. Branch `feat/rust-engine`, cut from `development` (782fc2a).
The Go engine is still authoritative. Nothing in this branch has been cut over.

## Verification gaps (not yet run)

- [ ] Task 11 fix round 1 scoped re-review was interrupted by an API rate limit. Re-run it against `review-f5f20a2..57bb6fb.diff`.
- [x] Full workspace `cargo test --workspace --all-targets` (359 tests) and `clippy -D warnings` green on 2026-11-19.
- [ ] `cargo deny check` and `cargo audit` not run locally (`deny.toml` unverified). CI installs both.
- [ ] Go tests not run against migration 008 (`webhook_delivered_at`). Go was not installed on the build host.
- [ ] Probe argv for `go version`, `kotlinc -version`, `mcs --version`, `bun --version`, `sqlite3 --version` is untested against a real go-judge image.
- [x] `rust/Dockerfile` builds on the server (2026-11-19). Compose and kustomize configs render; nothing deployed. Image is not pushed to a registry.
- [ ] Load tests not run. The runbook `docs/benchmarks/rust-vs-go.md` is an owner-filled template. No numbers exist yet.

## Cutover (Task 12) — needs owner approval

- [ ] Run the Rust shadow worker (`rust-shadow` overlay) beside Go for an agreed soak period.
- [ ] Run `shadowdiff` over the soak. Required: exit 0 with zero diffs.
- [ ] Owner runs the 200- and 1000-student loads on the 256-core server. Targets are in the design spec.
- [ ] If compiled-language cold paths miss the targets, plan the native sandbox as phase 2 before cutover.
- [ ] Switch traffic to Rust (`rust` overlay). Keep the Go image for one release for rollback.
- [ ] Separate PR: promote `rust/` to repo root, delete Go (`cmd/`, `internal/`, `go.mod`, `go.sum`), update `AGENTS.md`.

## Known defects / follow-ups

- [x] Custom checker fixed in both engines (2026-11-19): it runs for every test that completed normally. Rust has a test; the Go change has none yet.
- [ ] Reaper re-enqueues stuck submissions without `expires_at`. No deadline column exists. Add one with an additive migration.
- [ ] Cache TTL counts from creation, not last use. Confirm intent.
- [ ] Compile cache size cap is an estimate (1 MiB per entry). Replace with real sizes if go-judge reports them.
- [ ] A compile that exceeds the clock limit while go-judge is still running leaves its binary (the id never reaches the engine). Outputs returned with a failed compile are now deleted; the timed-out case still needs a go-judge file listing sweep or a TTL.
- [ ] SSRF blocklist lacks 6to4 (`2002::/16`), Teredo and IPv4 multicast `224/4`. Go has the same gaps.
- [ ] IPv6 /64 rotation can give one client many rate-limit buckets. Key on the /64 prefix.
- [ ] `RateLimitLayer` tower impl is dead code outside tests. Remove it.
- [ ] `Registry` is no longer `Clone`. Callers must hold it in an `Arc`.
- [ ] `openapi.yaml`: `Language` enum lacks `php` and `ruby`. `DELETE /api/v1/submissions/{id}` is missing. Both are served by Go.
- [ ] Case-sensitive JSON field names. Go decodes case-insensitively. Document in migration notes.
- [ ] No TLS for go-judge (`https://` rejected at startup). Confirm sidecar-only deployment.
- [ ] Exit code 1 vs 2 for shadowdiff tool errors (only 1 is tested).
- [ ] `--min-compared` is an absolute count, not a ratio. Document the operator guidance.
- [ ] Shadow workers can dead-letter into the shared poison stream when the DLQ name is misconfigured. Confirm `ac:dlq:poison:rust` in every overlay.
- [ ] Startup steps (`Queue::connect`, `AuthLayer::from_config`, listener binds) not wrapped in the shutdown signal, so SIGTERM is seen only at the next wrapped step.
- [ ] Flaky under container load: `reaper_reenqueues_stuck_queued_submissions_on_the_run_stream`, `corrupt_args_fail_job`, `auth_pg`, `tiered_weights_come_from_config`.
- [ ] Metric name `ac_queue_age_seconds_max` (HPA and Prometheus Adapter) must match the engine's real series. Verify with a live scrape.
- [ ] `ac_compile_duration_seconds` is observed on misses only. The cache-hit alert derives misses from it. Confirm against a live engine.
- [ ] Image name `ghcr.io/stjosephsplacements/aethercode-exec-rs` and `SET_ME` tag placeholder must be set before deploy.
- [ ] Read-only DB role for the shadow worker: SQL is in `docs/ARCHITECTURE.md`. RLS interaction not verified against a live database.
- [ ] `deploy/k8s-overlays/rust` needs `deploy/k8s/.env.secret` (not committed).

## Not committed by design

- `docs/superpowers/` (design spec and plan) is git-ignored by the repo's `.gitignore`. The spec is at `docs/superpowers/specs/2026-11-08-rust-engine-design.md` and the plan at `docs/superpowers/plans/2026-11-08-rust-engine.md` in the main checkout only.
- `.superpowers/` (SDD ledger, briefs, reports, review diffs) is local scratch.

## Rulings made without owner sign-off

See `.superpowers/sdd/2026-11-08-rust-engine/progress.md` (local). Summary:

1. Wire, Redis and Postgres compatibility kept through the shadow period.
2. Additive, nullable migrations only before cutover (migration 008 `webhook_delivered_at`).
3. `Job` derived from the `ac:tests:<id>` hash plus Postgres for language and source.
4. Expired jobs finish with `internal_error`.
5. `finish_with_results` returns `bool`.
6. Stream token key `ac:stoken:<job_id>` (matches Go).
7. Basic or empty Bearer returns 401, not anonymous.
8. Admission shed on `/execute` returns 429 + `Retry-After`.
9. Custom checker parity kept with Go (bug, tracked above).
10. Metrics bind address is configurable via `AC_DEBUG_ADDR`. Default stays loopback.
11. Nginx sidecar removed. Overlays split into `rust-shadow` and `rust`.

## Added 2026-11-19

- [x] Admission counts the engine's own consumer-group lag plus pending entries, not `XLEN` (retained history). With `XLEN`, a full stream made every request 429 with an idle queue (it stalled the platform judge). Fixed in Rust and Go.
- [x] Rust admission default is `2 * workers`; the `rust` overlay sets `AC_ADMISSION_MAX_QUEUE_DEPTH=64` (2 pods x 16 workers x 2). Raise it with the worker minimum.
- [x] Opt-in `rust-gvisor` overlay and `docs/GVISOR.md`. Not verified: the node has no `runsc`, and go-judge runs privileged, which gVisor ignores. Test on one node first.
- [ ] Shadow parity (`ae-shadowdiff`) was not run: it needs the shadow worker deployed beside the production Go engine and a read-only database role. Needs owner approval.
- [ ] Go-vs-Rust throughput and gVisor overhead benchmark not run; the numbers in `docs/benchmarks/rust-vs-go.md` are still the owner's to fill.
- [x] Shadow parity run on 2026-11-19 against the production Go engine: 21 compared, 21 matched, 0 diffs (C, C++, Python, Java; accepted, wrong answer, compile error, runtime error, time limit). Deploying it found and fixed: SQLx built without TLS (could not reach a `sslmode=require` database), `AC_REDIS_DB` ignored (the Rust engine used database 0), and collectors sent without `"max": 0`, which go-judge rejects.
- [x] Live `go-judge-config` was missing the `/etc/java-21-openjdk` mount that the repo's configmap has, so Java failed in both engines (`Error loading java.security file`). Applied the repo's configmap; Java now runs.
- [ ] The shadow overlay's go-judge ingress policy isolates go-judge when no base policy exists (it cut the Go engine off for about 18 minutes). Warning added; consider making it a patch of the base policy instead.
