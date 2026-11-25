# Campus secrets (names only — never values)

Secrets are created out of band on the server (`kubectl -n aethercode create secret …`) and mirrored to Infisical by the owner. This file lists **names and keys only**.

## `aethercode-exec-secrets`

| Key | Consumer | Purpose | Rotation |
|---|---|---|---|
| `AC_DATABASE_URL` | `aethercode-exec` pods | App role DSN via `pg-main` → PgBouncer (`aether_exec_app` / DB `aethercode_exec`) | Rotate app password; update secret; rollout restart |
| `AC_DATABASE_APP_URL` | `aethercode-exec` (optional RLS pool) | App-role URL when auth/RLS enabled | Same as above |
| `AC_REDIS_ADDR` | `aethercode-exec` | In-cluster Redis Service host:port | Redeploy Redis / bump clients |
| `AC_REDIS_PASSWORD` | `aethercode-exec`, Redis | Redis AUTH | Rotate Redis + secret together |
| `AC_JUDGE_TOKEN` | `aethercode-exec`, `go-judge` | Bearer token engine ↔ sandbox | Rotate both consumers; rolling restart |
| `AC_OIDC_ISSUER` | `aethercode-exec` | Zitadel / IdP issuer | Config change + restart |
| `AC_OIDC_JWKS_URL` | `aethercode-exec` | JWKS endpoint | Config change + restart |
| `AC_OIDC_AUDIENCE` | `aethercode-exec` | Expected audience | Config change + restart |
| `AC_WEBHOOK_MASTER_KEY` | `aethercode-exec` | 32-byte hex AES key for callback secrets | Rotate carefully (re-encrypt stored secrets) |
| `AC_ALLOW_UNAUTH` | `aethercode-exec` | Must be explicit when auth off | Prefer auth on in prod |

## Migration Job secret (PreSync)

| Secret | Keys | Notes |
|---|---|---|
| `aethercode-exec-migrator` | `AC_DATABASE_URL` | `aether_exec_migrator` role only; used by migrate Job, never by long-running pods |

## Preflight

A PreSync Job must fail the sync if any required key above is missing from `aethercode-exec-secrets` / `aethercode-exec-migrator`.

## Owner action after secret changes

Run the Infisical sync from the workstation (`/data/production/_ops/infisical-sync/`). Do not copy values into git, chat, or `DEPLOY-LOG.md`.
