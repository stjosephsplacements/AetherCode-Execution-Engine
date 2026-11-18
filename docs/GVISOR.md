# gVisor for the go-judge pods

Student code runs inside **go-judge**, not in `ae-worker`: the worker only sends HTTP requests to
go-judge (`AC_JUDGE_URL`). go-judge isolates each run with namespaces, cgroups and seccomp, all of which
share the host Linux kernel. gVisor (`runsc`) puts a user-space kernel between the pod and the host, so a
kernel exploit in a student binary lands in the Sentry, not on the node.

`deploy/k8s-overlays/rust-gvisor` applies `runtimeClassName: gvisor` to the go-judge DaemonSet and
creates the RuntimeClass. It is opt-in and not part of the `rust` overlay, because a pod that names a
RuntimeClass its node cannot run is never scheduled.

## Before applying

1. Install `runsc` and `containerd-shim-runsc-v1` on every node that runs go-judge.
2. Register the handler with containerd (k3s: add a `[plugins."io.containerd.cri.v1.runtime".containerd.runtimes.runsc]`
   block with `runtime_type = "io.containerd.runsc.v1"` to `config.toml.tmpl`, then restart k3s).
3. `kubectl apply -f deploy/k8s/runtimeclass-gvisor.yaml` and check `kubectl get runtimeclass gvisor`.

## Compatibility checklist (do this on one node first)

gVisor ignores `privileged: true` (the current go-judge pod), and go-judge needs mount namespaces and cgroup v2
writes. Verify, in a staging namespace, before relying on it:

- go-judge starts and `GET /version` answers.
- Smoke: run the C, C++, Python and Java hello-worlds through `/run`; all return `Accepted`.
- A fork bomb, a 2 GiB allocation and an infinite loop end with `Nonzero Exit`/`Memory Limit Exceeded`/
  `Time Limit Exceeded` and leave no process behind (`runsc ps`).
- Memory and CPU accounting still report plausible numbers (compare against the same program under runc).
- `ae-shadowdiff` against the Go engine reports zero diffs.

If go-judge does not run under runsc, keep the default overlay and rely on go-judge's own sandbox plus the
network policies; do not enable the overlay.

## Cost

Syscall-heavy runs (many small file reads, process spawns) are slower under gVisor, and compile steps
(`gcc`, `javac`) spawn many processes. Measure compile and run latency with `ae-loadgen` on the same node with
and without the overlay and record both in `docs/benchmarks/rust-vs-go.md`.
