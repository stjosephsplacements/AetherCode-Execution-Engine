#!/usr/bin/env bash
# Start go-judge for the STJ-Exec spike.
# Must be run as root (needs cgroups + namespaces).
# Binds to localhost:5050 only.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

exec "$SCRIPT_DIR/go-judge" \
  -http-addr     127.0.0.1:5050 \
  -mount-conf    "$SCRIPT_DIR/mount.yaml" \
  -enable-cpu-rate \
  -enable-metrics \
  -enable-debug \
  -auth-token    "stj-spike-2024" \
  -parallelism   8 \
  -pre-fork      4 \
  -output-limit  "16m" \
  -copy-out-limit "16m" \
  -open-file-limit 64 \
  -file-timeout  5m \
  -dir           "$SCRIPT_DIR/file-store"
