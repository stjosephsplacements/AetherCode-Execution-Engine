# Campus production overlay (St. Joseph's)

Path: `deploy/k8s-overlays/campus-production`.

Deploys the Execution Engine into namespace `aethercode` alongside the AetherCode platform.

## Contract

| Resource | Name / address |
|---|---|
| Engine Service | `aethercode-exec-svc.aethercode.svc.cluster.local:5100` |
| Sandbox | `go-judge-svc:5050` (DaemonSet; **privileged** — owner approval required) |
| Redis | `aethercode-exec-redis:6379` (StatefulSet, `local-production` PVC) |
| Postgres | `pg-main:5432` → host PgBouncer `10.1.55.99:6432`; DB `aethercode_exec` |
| Metrics | ServiceMonitor label `release: kube-prometheus` |

## Images

Built on the server (Phase A: `docker build` + `k3s ctr import`). Tags = git SHA. Never `:latest`. `imagePullPolicy: IfNotPresent`.

## Do not apply until

1. Owner reviews this overlay (§11.2).
2. Owner approves `privileged: true` on go-judge (§4).
3. Secrets created out of band (`deploy/SECRETS.md`).
4. DB/roles created on central Postgres (`aethercode_exec` + migrator/app/owner).
5. Fresh backup of central Postgres before migrations.
