# Rust vs Go engine: load benchmark runbook

Status: runbook. No numbers have been recorded yet. go-judge needs a privileged
container with cgroups v2 and namespaces; it was not run on the macOS development
machine, so no Go-versus-Rust figures exist. The repo owner fills the results table on
the 256-core server.

## Tools

- `loadgen` (`rust/crates/ae-loadgen`): simulated students; each submits, waits for the
  VERDICT on `/api/v1/stream`, thinks, repeats. Works against either engine.
- `shadowdiff` (`rust/crates/ae-shadowdiff`): compares `engine --shadow` records with the
  Go engine's Postgres rows (parity gate, see below).

Cold vs warm: the verdict payload has no compile-cache flag, so `loadgen` classifies the
first submission of each `(language, source)` pair as COLD and every repeat as WARM.
A pair counts as seen only after its POST was accepted. Students that submit a new pair
at the same moment can inflate WARM slightly while the first copy compiles. The compile
cache survives between runs, so restart the engine before every repeat or the next run
starts warm. The compile-cache hit rate comes from the engine's `ac_compile_cache_hits_total`
metric (divide by compile requests), not from `loadgen`. POST retries after 429/5xx are
reported as `post_retries`, apart from `error_rate`, and are excluded from ingestion latency.

## Targets (256-core server)

| Students | p50 verdict | p95 verdict | Other |
|---|---|---|---|
| 200 | < 500 ms | < 1 s | p50 API latency < 5 ms |
| 1000 | < 1 s | < 2 s | compile-cache hit rate > 90% for multi-test jobs |
| 5000+ | must stay up | n/a | concurrent students |

Go baseline for reference (owner-reported, from the design spec): p95 verdicts of about 3 s at 200 and 5 s at 1000 students.

## Build

```bash
source "$HOME/.cargo/env"
cd rust && cargo build --release -p ae-loadgen -p ae-shadowdiff -p ae-engine
# binaries: rust/target/release/{loadgen,shadowdiff,engine}
```

## Run: Go engine

```bash
# append to .env if one exists (do not overwrite it); choose real values
printf "POSTGRES_PASSWORD=...\nAC_JUDGE_TOKEN=...\n" >> .env
docker compose up --build -d
curl -fsS http://127.0.0.1:5100/api/v1/health
```

## Run: Rust engine

Stop the Go engine's `engine` service (keep `db`, `redis`, `go-judge` up) and start the
Rust one on the same port with the same environment as the compose `engine` service:

```bash
docker compose stop engine
export AC_GATEWAY_ADDR=0.0.0.0:5100
export AC_DATABASE_URL="postgres://aethercode:$POSTGRES_PASSWORD@127.0.0.1:5433/aethercode_exec"
export AC_REDIS_ADDR=127.0.0.1:6379
export AC_JUDGE_URL=http://127.0.0.1:5050 AC_JUDGE_TOKEN=...   # as in .env
export AC_AUTH_ENABLED=false AC_ALLOW_UNAUTH=true
rust/target/release/engine
```

## Load

Run each load three times per engine, restarting the engine between engines. Use `--json`
for the record and the table for reading.

```bash
LG=rust/target/release/loadgen

# 200 students
$LG --url http://127.0.0.1:5100 --students 200 --languages c,cpp,python,java \
    --duration 5m --ramp 30s --json > results-200-<engine>.json

# 1000 students
$LG --url http://127.0.0.1:5100 --students 1000 --languages c,cpp,python,java \
    --duration 5m --ramp 60s --json > results-1000-<engine>.json

# 5000 students
$LG --url http://127.0.0.1:5100 --students 5000 --languages c,cpp,python,java \
    --duration 5m --ramp 120s --json > results-5000-<engine>.json
```

Flags: `--token` (or `AC_LOADGEN_TOKEN`) when auth is enabled, `--think-min/--think-max`
(default 1s to 3s), `--timeout` per-job verdict wait (default 120s). Raise the open-file
limit (`ulimit -n 65536`) before 1000 students.

Read `verdict_cold` and `verdict_warm` separately: the targets apply to WARM. A run is a
failure if `error_rate` is above 0 or `mismatch_rate` differs between engines.

## Parity gate (shadow mode)

With the Go engine authoritative, run the Rust engine beside it on the same streams:

```bash
rust/target/release/engine --role worker --shadow
# drive load against the Go gateway with loadgen, then:
rust/target/release/shadowdiff --redis redis://127.0.0.1:6379/ \
    --pg "postgres://aethercode:$POSTGRES_PASSWORD@127.0.0.1:5433/aethercode_exec" --since 1h
```

`--pg` must connect as the owner role or one that bypasses row-level security; under RLS
every row looks missing. The URL can come from `SHADOWDIFF_PG` (then `AC_DATABASE_URL`) and
Redis from `SHADOWDIFF_REDIS`: command-line flags are visible in `ps`, so prefer the
environment when a URL holds a password.

The tool fails closed. Exit 1 on any diff, any unreadable shadow record, or when nothing was
compared while jobs are pending; `--fail-on-pending` also fails on any pending job and lists
their ids. It also refuses to report parity when fewer than `--min-compared` (default 1) records were
compared, so an empty or mis-pointed run, or one where everything was ignored, exits 1;
`--allow-empty` accepts such a run. `--since` counts the records it drops as
`skipped_since`, and a record without a `recorded_at` is `unreadable`. Exit 0 with
`diff=0 unreadable=0` and `compared` above zero is required. `pending` (Go not finished) and `ignored` (the tests hash
had expired) are reported separately and are not diffs; rerun until `pending=0`.

## Results (to be filled by the repo owner on the 256-core server)

| Students | Engine | Cache | p50 ms | p95 ms | p99 ms | Error rate | Ingest p50 ms | Hit rate |
|---|---|---|---|---|---|---|---|---|
| 200 | Go | cold | | | | | | |
| 200 | Go | warm | | | | | | |
| 200 | Rust | cold | | | | | | |
| 200 | Rust | warm | | | | | | |
| 1000 | Go | cold | | | | | | |
| 1000 | Go | warm | | | | | | |
| 1000 | Rust | cold | | | | | | |
| 1000 | Rust | warm | | | | | | |

Shadow parity result (`compared/matched/diff/pending/ignored/unreadable/skipped_since`):

## Smoke test (not a benchmark)

`cargo test -p ae-engine --test loadgen_smoke -- --nocapture` runs the real `engine` binary
against Postgres, Redis and an in-process fake go-judge, only to prove `loadgen` works end
to end. The sandbox is stubbed, so its latencies say nothing about the targets above.
