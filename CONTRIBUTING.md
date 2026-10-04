# Contributing

Thanks for contributing to the AetherCode Execution Engine. This project
executes **untrusted user code** at scale, so the bar for correctness and
security is high. Please read this before opening a PR.

## Prerequisites

- **Go 1.25+**
- **Docker + Docker Compose** (for the full dev stack)
- Optionally **golangci-lint** (v2.12.2 — the version CI pins; config in `.golangci.yml`) if you don't want to lean on CI

## Getting started

```bash
git clone https://github.com/stjosephsplacements/AetherCode-Execution-Engine.git
cd AetherCode-Execution-Engine

make help        # list all targets
make build       # build the engine binary
make test        # run unit tests
make lint        # run golangci-lint
```

### Full local stack

The fastest way to run everything (engine + PostgreSQL + Redis + go-judge
sandbox) is Docker Compose:

```bash
make up          # docker compose up --build
curl http://127.0.0.1:5100/api/v1/health
make e2e         # run the end-to-end suite against the stack
make down        # stop the stack
```

> The go-judge container runs with `privileged: true` in the dev stack because
> its sandbox needs cgroups + namespaces. That is fine for a machine you
> control; use the systemd deployment with explicit limits in production.

## Development workflow

```bash
# branch
git checkout -b feat/my-change

# edit, then verify the full local gate matches CI:
make build
make test
make lint
make fmt          # only if you want gofmt applied for you
```

Run the same checks CI runs before pushing. CI fails on any of:
`go build`, `go vet`, `gofmt -s`, `go test -race ./...`, or `golangci-lint run`.

## Branches & release flow

This project uses a four-branch, environment-mapped model. A change moves
**one branch at a time**, and each promotion is a pull request that merges only
when CI is green — so a release reaches `production` only by passing all tests
on every branch in sequence.

```
development  →  testing  →  staging  →  production   (default branch)
```

| Branch | Role |
|---|---|
| `development` | Day-to-day feature work. Merge here with a green PR. |
| `testing` | Integrated, buildable candidate. Promote from `development`. |
| `staging` | Pre-release; mirrors the staging environment. Promote from `testing`. |
| `production` | The default branch, what is served. Promote from `staging`. |

Rules (enforced by CI + branch protection):

- **No direct pushes** to any of the four branches — everything goes through a
  pull request.
- A PR merges **only when the `test` and `lint` checks are green**.
- Promotions are **linear**: `development → testing → staging → production`.
  Never skip a stage.
- **Hot-fixes follow the same path** so each environment's tests run against
  the fix. If a regression is found on `production`, branch from
  `production`, fix, and back-port to the earlier branches.

## Code style

- **gofmt-clean** — run `gofmt -s -w .` (or `make fmt`) before committing.
- **errcheck matters.** This engine has *intentional* fail-open paths (best-effort
  logging, connection reuse, advisory-lock release). Those are marked with a
  `//nolint:errcheck // reason` comment. A bare ignored error without a reason is
  a review blocker.
- **Keep the linters honest.** The enabled set is in `.golangci.yml`. Don't
  weaken it to silence a finding; fix the finding, or add a scoped, reasoned
  `//nolint`.
- Follow the existing structure in `CLAUDE.md` → **Code map**.

## Tests

- **Unit tests must be hermetic.** They run in CI with no external services. The
  rate-limiter tests use an in-process `miniredis`; follow that pattern if you
  need Redis in a test.
- **Never run real submissions in a test or in the sandbox from a test.** Expected
  outputs live in the worker, not in the sandbox environment.
- Add tests for new behavior. `make e2e` exercises the full pipeline but requires
  a running stack — it is not part of the hermetic unit suite.

## Design invariants (do not reintroduce the old behavior)

Several invariants in `CLAUDE.md` exist to make "zero lost jobs" provable. If a
change touches them, call it out explicitly in the PR description:

- **Insert before enqueue** — the PG row exists before the Redis `XADD`.
- **Subscribe before replay** — SSE subscribes before reading the event log.
- **Cancel-safe writes** — DB/publish and `XAck` use `context.WithoutCancel`.
- **CAS verdict writes** — `UPDATE … WHERE status NOT IN ('completed','failed')`.

## Commit messages

Use imperative, scoped messages: `worker: deduplicate test-case output trims`.
Keep the body focused; reference the invariant you touched if relevant.

## Pull requests

1. Small, focused diffs. One concern per PR.
2. Fill in the PR template (what changed, why, how it was tested).
3. For anything touching the sandbox boundary, auth, or the queue, explain the
   security/correctness reasoning in the description — reviewers will look for it.
4. CI must be green (build, vet, fmt, tests, lint).
