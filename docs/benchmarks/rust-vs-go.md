# Rust vs Go engine: load benchmark runbook

Status: runbook, plus one recorded result: a single 200-student run per engine
(section "Results: 2026-11-19, 200 students" at the end, which is the authoritative record).
That run missed the 200-student targets below for both engines. The 1000- and 5000-student
loads, the three-repeat variance runs and the cold/warm table in "Results (to be filled...)"
have not been run, so that table stays empty. go-judge needs a privileged container with
cgroups v2 and namespaces; it was not run on the macOS development machine.

## Tools

- `loadgen` (`rust/crates/ae-loadgen`): simulated students; each submits, waits for the
  VERDICT on `/api/v1/stream`, thinks, repeats. Works against either engine.
- `shadowdiff` (`rust/crates/ae-shadowdiff`): compares `engine --shadow` records with the
  Go engine's Postgres rows (parity gate, see below).

Cold vs warm: the verdict payload has no compile-cache flag, so `loadgen` classifies the
first submission of each `(language, source)` pair as COLD and every repeat as WARM.
A pair counts as seen only after its POST was accepted. Students that submit a new pair
at the same moment can inflate WARM slightly while the first copy compiles. The compile
cache survives between runs, so restart the engine before every run (each repeat, and between
engines) or the next run starts warm. The compile-cache hit rate comes from the engine's `ac_compile_cache_hits_total`
metric (divide by compile requests), not from `loadgen`. POST retries after 429 (honouring `Retry-After`; 5xx is not retried, a retry could duplicate a
job) are
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
# Put POSTGRES_PASSWORD and AC_JUDGE_TOKEN in .env with an editor (do not overwrite an existing
# file, and keep secrets off command lines: they show up in `ps` and shell history).
docker compose up --build -d
curl -fsS http://127.0.0.1:5100/api/v1/health
```

## Run: Rust engine

Stop the Go engine's `engine` service (keep `db`, `redis`, `go-judge` up) and start the
Rust one on the same port with the same environment as the compose `engine` service:

```bash
docker compose stop engine
set -a; . ./.env; set +a                      # exports POSTGRES_PASSWORD and AC_JUDGE_TOKEN
: "${POSTGRES_PASSWORD:?set POSTGRES_PASSWORD in .env}"
export AC_GATEWAY_ADDR=0.0.0.0:5100
export AC_DATABASE_URL="postgres://aethercode:$POSTGRES_PASSWORD@127.0.0.1:5433/aethercode_exec"
export AC_REDIS_ADDR=127.0.0.1:6379
export AC_JUDGE_URL=http://127.0.0.1:5050
export AC_AUTH_ENABLED=false AC_ALLOW_UNAUTH=true
rust/target/release/engine
```

## Load

Run each load three times per engine, restarting the engine before every run. Use `--json`
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
export SHADOWDIFF_REDIS=redis://127.0.0.1:6379/
export SHADOWDIFF_PG="postgres://aethercode:$POSTGRES_PASSWORD@127.0.0.1:5433/aethercode_exec"
rust/target/release/shadowdiff --since 1h
```

`--pg` must connect as the owner role or one that bypasses row-level security; under RLS
every row looks missing. The URL comes from `SHADOWDIFF_PG` (then `AC_DATABASE_URL`) and
Redis from `SHADOWDIFF_REDIS`: command-line flags are visible in `ps`, so pass URLs that hold a
password through the environment, as above.

The tool fails closed. Exit 1 on any diff, any unreadable shadow record, or when nothing was
compared while jobs are pending; `--fail-on-pending` also fails on any pending job and lists
their ids. It also refuses to report parity when fewer than `--min-compared` (default 1) records were
compared, so an empty or mis-pointed run, or one where everything was ignored, exits 1;
`--allow-empty` accepts such a run. `--since` counts the records it drops as
`skipped_since`, and a record without a `recorded_at` is `unreadable`. Exit 0 with
`diff=0 unreadable=0` and `compared` above zero is required. `pending` (Go not finished) and `ignored` (the tests hash
had expired) are reported separately and are not diffs; rerun until `pending=0`.

## Results (to be filled by the repo owner on the 256-core server)

Not yet run: the measured 200-student comparison is in "Results: 2026-11-19, 200 students" below.

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

Shadow parity result (`compared/matched/diff/pending/ignored/unreadable/skipped_since`): the 2026-11-19 run against the production Go engine compared 21 and matched 21 with 0 diffs (C, C++, Python, Java); the other counters were not recorded.

## Smoke test (not a benchmark)

`cargo test -p ae-engine --test loadgen_smoke -- --nocapture` runs the real `engine` binary
against Postgres, Redis and an in-process fake go-judge, only to prove `loadgen` works end
to end. The sandbox is stubbed, so its latencies say nothing about the targets above.

## Results: 2026-11-19, 200 students

Run by `loadgen --students 200 --languages c,cpp,python --duration 3m --ramp 20s` (a 30-student, 40 s warm-up first), one
engine at a time against the same go-judge, each with its own empty Postgres 18 and Redis 7 on the 256-core server.
Go ran as the host binary with `AC_WORKER_COUNT=16`; Rust ran in its production image with `AC_WORKERS=16`.

| | Go | Rust |
|---|---|---|
| Submissions completed / lost | 7322 / 0 | 7369 / 0 |
| Error rate, verdict mismatch rate | 0, 0 | 0, 0 |
| Throughput (jobs/s) | 38.5 | 39.2 |
| Ingest (POST) p50 / p95 / p99 | 1.04 / 1.53 / 1.93 ms | 0.96 / 1.48 / 1.83 ms |
| Verdict p50 / p95 / p99 (warm) | 2.56 / 5.87 / 8.14 s | 2.64 / 5.42 / 7.68 s |
| Verdict mix | 6677 accepted, 273 CE, 67 RE, 183 TLE, 122 WA | 6717, 272, 66, 186, 128 |

**Target missed.** The runbook target for 200 students is p50 < 500 ms and p95 < 1 s. Warm verdicts were
2.56 s p50 / 5.87 s p95 on Go and 2.64 s p50 / 5.42 s p95 on Rust, so both engines missed it by roughly 5x at p50
and 5x or more at p95. The shared go-judge (below) is the likely cause, but this run did not isolate it, so the
target is not shown to be reachable on this hardware.

This is one run per engine: the runbook asks for three, so there is no run-to-run variance and no noise estimate.

What this shows and does not show:

- The two engines did not differ measurably in this single run, at a load where both were far over target. Both were limited by the shared go-judge sandbox (about 39 jobs/s,
  with several seconds of queueing for a verdict), not by the engine. A tool that is not the bottleneck cannot be faster.
  Ingest is about 1 ms on both.
- The verdict mix agrees within noise, which is what the parity run (21 of 21 identical verdicts) showed deterministically.
- The only cold verdicts were 9 per run, so the cold-path comparison has no weight.
- **Not measured**: 1000 or 5000 students, a go-judge with more parallelism, gVisor overhead (no `runsc` on the node), and
  anything about memory or CPU of the engine itself. A fair test of the engines needs a go-judge that is not the limit.
- The go-judge was the production cluster's, shared with a Playwright run during the benchmark, so absolute times carry
  that noise. The conditions were uncontrolled: the engines ran minutes apart and the competing load was not measured,
  so they did not necessarily see identical load.
