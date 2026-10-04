# Security Policy

The AetherCode Execution Engine is a code-execution service: it runs
**untrusted user submissions** inside a [go-judge](https://github.com/criyle/go-judge)
sandbox. The sandbox boundary (cgroups + namespaces + a read-only system view)
is the core of its security model, so we take reports about it very seriously.

## Supported versions

| Version | Supported |
|---|---|
| `0.1.x` (latest) | ✅ yes |
| < `0.1` | ❌ no |

## Reporting a vulnerability

**Please do not open a public issue for a security concern.**

1. **Preferred:** use GitHub's
   [private vulnerability reporting](https://docs.github.com/en/code-security/repository-security-advisories/publishing-a-security-advisory)
   for this repository (repo → *Security* → *Reporting a vulnerability*).
   This creates a private advisory and notifies the maintainers.
2. Alternatively, contact a project maintainer directly through the repository
   contact channels referenced in the issue tracker.

Please include, where you can:

- A minimal reproduction or the class of problem
- The affected component (gateway, queue, worker, sandbox client, auth)
- The version you tested on

## What we consider in scope

- **Sandbox escape** — a submission reading or writing outside the intended
  work dir, exfiltrating host data, or escaping the go-judge namespace/cgroup
  confinement.
- **Authentication bypass** — bypassing OIDC JWT validation or tenant
  isolation.
- **Tenant isolation failure** — one user reading another user's submissions,
  event streams, or results.
- **Denial of service** — a submission or request pattern that reliably
  degrades or takes down the engine beyond the intended resource limits.
- **Injection** — request or config values that alter intended engine behavior
  (SQL, command, or stream-key injection).

## Out of scope

- Misconfiguration of a *self-hosted* deployment (e.g. exposing the engine
  without auth, using the default dev judge token, running the sandbox without
  the intended limits).
- Attack vectors that require access to host credentials or a compromised host.
- Issues already known and tracked.

## Response

- We aim to **acknowledge** reports within 2 business days and triage within 5.
- We disclose responsibly: we coordinate a fix before any public disclosure and
  credit the reporter (unless they prefer to remain anonymous).
- If a fix is not feasible, we document the limitation in this file and in the
  release notes.

## Note on the sandbox

The engine relies on go-judge for isolation. Reports about the go-judge
engine itself are best sent to its own project; we will coordinate with its
maintainers on anything that affects this engine's guarantees.
