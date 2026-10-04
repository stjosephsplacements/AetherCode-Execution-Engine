# Contributing

Thanks for contributing to the AetherCode Execution Engine. This project
executes **untrusted user code** at scale, so the bar for correctness and
security is high. Please read this before opening a PR.

## Prerequisites

- **Go 1.25+**
- **Docker + Docker Compose** (for the full dev stack)
- Optionally **golangci-lint** (v2.12.2 — the version CI pins; config in `.golangci.yml`) if you don't want to lean on CI

## Before you start

For **bug fixes and small improvements**, go ahead and open a PR — no prior
discussion needed.

For **new features, API changes, or anything that touches the sandbox boundary,
auth, or the queue**, open an issue first. Describing the problem and your
proposed approach before writing code prevents wasted effort and helps
maintainers give early feedback on the design.

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
# Branch from development (the default branch)
git checkout development
git pull
git checkout -b feat/my-change

# Edit, then verify the full local gate matches CI:
make build
make test
make lint
make fmt          # only if you want gofmt applied for you
```

Run the same checks CI runs before pushing. CI fails on any of:
`go build`, `go vet`, `gofmt -s`, `go test -race ./...`, or `golangci-lint run`.

## Branches & release flow

This project uses a four-branch, environment-mapped model. All work starts on
`development` (the default branch) and moves **one branch at a time** toward
`production`. Every promotion is driven by the CI automation described below —
no manual cherry-picks.

```
development  →  testing  →  staging  →  production
(default)
```

| Branch | Role |
|---|---|
| `development` | Day-to-day feature work. All PRs target this branch. |
| `testing` | Integrated, buildable candidate. Auto-promoted from `development` when CI is green. |
| `staging` | Pre-release; mirrors the staging environment. Auto-promoted from `testing` when CI is green. |
| `production` | What is served. Promoted from `staging` — requires one approving review before merge. |

### Promotion rules

- **No direct pushes** to any of the four branches — everything goes through a
  pull request.
- A PR merges only when the `test` and `lint` CI checks are green.
- `development → testing → staging` promotions happen **automatically**: once
  CI is green on `development`, the CI workflow opens a PR into `testing`, waits
  for its checks, and merges it. The same happens for `testing → staging`.
- `staging → production` is the **only human gate**: the CI workflow opens the
  PR automatically, but an org member must approve it before it can be merged.
  Organization admins have bypass access for emergency situations.
- **Topic branches** (`feat/*`, `fix/*`, `security/*`, `ci/*`, `docs/*`,
  `chore/*`, `refactor/*`) are also handled: once CI is green on the topic
  branch, a PR into `development` is opened and merged automatically.
- **Merge policy:** all promotions use a standard merge commit. Squash and
  rebase merges are disabled to keep a linear, auditable history across the
  pipeline.

### Hotfixes

If a regression reaches `production`, do **not** skip stages:

```bash
# Branch from production so the fix contains exactly what is live
git checkout production
git pull
git checkout -b fix/my-hotfix

# Fix, test, push
# Open a PR into production — an org admin can approve and merge it

# Back-port: open a PR from your fix branch into staging, then testing, then development
# (or let the promotion pipeline carry it forward once CI is green on production)
```

Every environment's tests still run against the fix. The extra few minutes are
worth it.

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

## Design invariants

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

A good PR description covers:

1. **What changed** — one sentence summary.
2. **Why** — the motivation (links to an issue if one exists).
3. **How it was tested** — unit tests added, `make e2e` run, manual steps.
4. **Invariants touched** — if any of the four design invariants above apply,
   say which one and why the change is still safe.
5. **Security/correctness reasoning** — required for anything touching the
   sandbox boundary, auth, the queue, or rate limiting.

Other guidelines:

- Small, focused diffs. One concern per PR.
- CI must be green (build, vet, fmt, tests, lint) before requesting review.
- PRs into `development` need **one approving review**. The same rule applies
  at every stage through to `production`.
