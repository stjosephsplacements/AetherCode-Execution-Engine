#!/usr/bin/env bash
# STJ-Exec go-judge micro-benchmark
# Measures: (1) per-language compile+run latency, (2) throughput at 8/32/96 parallel
set -euo pipefail

URL="http://127.0.0.1:5050"
AUTH="Authorization: Bearer stj-spike-2024"
CT="Content-Type: application/json"
ITERATIONS=20  # per-language latency samples

# ─── Helpers ──────────────────────────────────────────────────────────────────

ts_ms() { python3 -c "import time; print(int(time.time()*1000))"; }

call_judge() {
  curl -sf -X POST "$URL/run" -H "$AUTH" -H "$CT" -d "$1" 2>/dev/null
}

extract() {
  python3 -c "
import sys, json
r = json.load(sys.stdin)
print(r[0].get('$1',''))" 2>/dev/null <<< "$2"
}

extract_fid() {
  python3 -c "
import sys, json
r = json.load(sys.stdin)
fids = r[0].get('fileIds', {})
print(next(iter(fids.values()), ''))" 2>/dev/null <<< "$1"
}

percentile() {
  # $1 = space-separated sorted values, $2 = percentile (0-100)
  python3 -c "
import sys
vals = list(map(float, '$1'.split()))
n = len(vals)
p = $2
k = (n - 1) * p / 100.0
f = int(k)
c = f + 1 if f + 1 < n else f
return_val = vals[f] + (k - f) * (vals[c] - vals[f]) if c != f else vals[f]
print(f'{return_val:.1f}')
"
}

stats() {
  # stdin: one number per line. Prints: count p50 p95 p99 min max avg
  python3 -c "
import sys
vals = sorted(float(l) for l in sys.stdin if l.strip())
n = len(vals)
if n == 0:
    print('0 0 0 0 0 0 0')
    sys.exit()
def pct(p):
    k = (n-1)*p/100
    f = int(k); c = min(f+1, n-1)
    return vals[f]+(k-f)*(vals[c]-vals[f])
avg = sum(vals)/n
print(f'{n} {pct(50):.1f} {pct(95):.1f} {pct(99):.1f} {min(vals):.1f} {max(vals):.1f} {avg:.1f}')
"
}

# ─── Language payloads ────────────────────────────────────────────────────────

# Each language has: compile_payload (or empty), run_payload, name
# For compiled langs, run_payload has CACHED_FILE_ID placeholder

C_COMPILE='{"cmd":[{"args":["/usr/bin/gcc","-o","/w/a.out","/w/main.c","-O2","-lm"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":10000000000,"memoryLimit":268435456,"procLimit":50,"copyIn":{"/w/main.c":{"content":"#include <stdio.h>\n#include <math.h>\nint main() {\n    double s = 0;\n    for (int i = 0; i < 1000000; i++) s += sqrt((double)i);\n    printf(\"%.2f\\n\", s);\n    return 0;\n}"}},"copyOut":["stdout","stderr"],"copyOutCached":["/w/a.out"]}]}'

C_RUN='{"cmd":[{"args":["/w/a.out"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":5000000000,"memoryLimit":268435456,"procLimit":10,"copyIn":{"/w/a.out":{"fileId":"CACHED_FILE_ID"}},"copyOut":["stdout","stderr"]}]}'

CPP_COMPILE='{"cmd":[{"args":["/usr/bin/g++","-o","/w/a.out","/w/main.cpp","-O2","-std=c++17"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":10000000000,"memoryLimit":268435456,"procLimit":50,"copyIn":{"/w/main.cpp":{"content":"#include <iostream>\n#include <cmath>\nusing namespace std;\nint main() {\n    double s = 0;\n    for (int i = 0; i < 1000000; i++) s += sqrt((double)i);\n    cout << fixed;\n    cout.precision(2);\n    cout << s << endl;\n    return 0;\n}"}},"copyOut":["stdout","stderr"],"copyOutCached":["/w/a.out"]}]}'

CPP_RUN='{"cmd":[{"args":["/w/a.out"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":5000000000,"memoryLimit":268435456,"procLimit":10,"copyIn":{"/w/a.out":{"fileId":"CACHED_FILE_ID"}},"copyOut":["stdout","stderr"]}]}'

GO_COMPILE='{"cmd":[{"args":["/usr/bin/go","build","-o","/w/main","/w/main.go"],"env":["PATH=/usr/bin:/bin","GOROOT=/usr/lib/go-1.22","HOME=/tmp","GOCACHE=/tmp/go-cache"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":30000000000,"memoryLimit":1073741824,"procLimit":256,"copyIn":{"/w/main.go":{"content":"package main\nimport (\n\t\"fmt\"\n\t\"math\"\n)\nfunc main() {\n\ts := 0.0\n\tfor i := 0; i < 1000000; i++ {\n\t\ts += math.Sqrt(float64(i))\n\t}\n\tfmt.Printf(\"%.2f\\n\", s)\n}"}},"copyOut":["stdout","stderr"],"copyOutCached":["/w/main"]}]}'

GO_RUN='{"cmd":[{"args":["/w/main"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":5000000000,"memoryLimit":268435456,"procLimit":20,"copyIn":{"/w/main":{"fileId":"CACHED_FILE_ID"}},"copyOut":["stdout","stderr"]}]}'

JAVA_COMPILE='{"cmd":[{"args":["/usr/bin/javac","-d","/w","/w/Main.java"],"env":["PATH=/usr/bin:/bin","JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":15000000000,"memoryLimit":536870912,"procLimit":50,"copyIn":{"/w/Main.java":{"content":"public class Main {\n    public static void main(String[] args) {\n        double s = 0;\n        for (int i = 0; i < 1000000; i++) s += Math.sqrt((double)i);\n        System.out.printf(\"%.2f%n\", s);\n    }\n}"}},"copyOut":["stdout","stderr"],"copyOutCached":["/w/Main.class"]}]}'

JAVA_RUN='{"cmd":[{"args":["/usr/bin/java","-cp","/w","Main"],"env":["PATH=/usr/bin:/bin","JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":10000000000,"memoryLimit":536870912,"procLimit":50,"copyIn":{"/w/Main.class":{"fileId":"CACHED_FILE_ID"}},"copyOut":["stdout","stderr"]}]}'

PYTHON_RUN='{"cmd":[{"args":["/usr/bin/python3","/w/main.py"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":10000000000,"memoryLimit":268435456,"procLimit":50,"copyIn":{"/w/main.py":{"content":"import math\ns = sum(math.sqrt(i) for i in range(1000000))\nprint(f\"{s:.2f}\")"}},"copyOut":["stdout","stderr"]}]}'

JS_RUN='{"cmd":[{"args":["/usr/bin/node","/w/main.js"],"env":["PATH=/usr/bin:/bin","HOME=/tmp"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":10000000000,"memoryLimit":268435456,"procLimit":50,"copyIn":{"/w/main.js":{"content":"let s = 0; for (let i = 0; i < 1000000; i++) s += Math.sqrt(i); console.log(s.toFixed(2));"}},"copyOut":["stdout","stderr"]}]}'

SQLITE_RUN='{"cmd":[{"args":["/usr/bin/sqlite3",":memory:"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":"CREATE TABLE t(x INTEGER);\nWITH RECURSIVE cnt(v) AS (SELECT 1 UNION ALL SELECT v+1 FROM cnt WHERE v<1000)\nINSERT INTO t SELECT v FROM cnt;\nSELECT SUM(x) FROM t;\n"},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":5000000000,"memoryLimit":134217728,"procLimit":10,"copyOut":["stdout","stderr"]}]}'

# ─── Part 1: Per-language latency ─────────────────────────────────────────────

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  STJ-Exec go-judge Micro-Benchmark                         ║"
echo "║  $(date -Iseconds)                                  ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "go-judge parallelism: 8, iterations per test: $ITERATIONS"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Part 1: Per-Language Latency (ms)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
printf "%-12s %8s %8s %8s %8s %8s %8s\n" "Language" "p50" "p95" "p99" "min" "max" "avg"
echo "────────────────────────────────────────────────────────────────"

bench_compiled() {
  local name="$1" compile_payload="$2" run_template="$3"
  local compile_times="" run_times="" total_times=""

  for i in $(seq 1 $ITERATIONS); do
    local t0 t1 t2
    t0=$(ts_ms)

    local cresp
    cresp=$(call_judge "$compile_payload")
    local cstatus
    cstatus=$(extract "status" "$cresp")
    local fid
    fid=$(extract_fid "$cresp")

    t1=$(ts_ms)

    if [[ "$cstatus" != "Accepted" || -z "$fid" ]]; then
      echo "  WARN: $name compile failed on iteration $i (status=$cstatus)" >&2
      continue
    fi

    local run_payload
    run_payload="${run_template//CACHED_FILE_ID/$fid}"
    local rresp
    rresp=$(call_judge "$run_payload")

    t2=$(ts_ms)

    compile_times+="$((t1 - t0))\n"
    run_times+="$((t2 - t1))\n"
    total_times+="$((t2 - t0))\n"
  done

  local cstats rstats tstats
  cstats=$(echo -e "$compile_times" | stats)
  rstats=$(echo -e "$run_times" | stats)
  tstats=$(echo -e "$total_times" | stats)

  read -r cn cp50 cp95 cp99 cmin cmax cavg <<< "$cstats"
  read -r rn rp50 rp95 rp99 rmin rmax ravg <<< "$rstats"
  read -r tn tp50 tp95 tp99 tmin tmax tavg <<< "$tstats"

  printf "%-12s %8s %8s %8s %8s %8s %8s  (compile)\n" "$name" "$cp50" "$cp95" "$cp99" "$cmin" "$cmax" "$cavg"
  printf "%-12s %8s %8s %8s %8s %8s %8s  (run)\n" "" "$rp50" "$rp95" "$rp99" "$rmin" "$rmax" "$ravg"
  printf "%-12s %8s %8s %8s %8s %8s %8s  (total)\n" "" "$tp50" "$tp95" "$tp99" "$tmin" "$tmax" "$tavg"
  echo ""
}

bench_interpreted() {
  local name="$1" payload="$2"
  local times=""

  for i in $(seq 1 $ITERATIONS); do
    local t0 t1
    t0=$(ts_ms)
    local resp
    resp=$(call_judge "$payload")
    t1=$(ts_ms)

    local status
    status=$(extract "status" "$resp")
    if [[ "$status" != "Accepted" ]]; then
      echo "  WARN: $name failed on iteration $i (status=$status)" >&2
      continue
    fi

    times+="$((t1 - t0))\n"
  done

  local s
  s=$(echo -e "$times" | stats)
  read -r n p50 p95 p99 smin smax avg <<< "$s"
  printf "%-12s %8s %8s %8s %8s %8s %8s\n" "$name" "$p50" "$p95" "$p99" "$smin" "$smax" "$avg"
}

bench_compiled "C"    "$C_COMPILE"    "$C_RUN"
bench_compiled "C++"  "$CPP_COMPILE"  "$CPP_RUN"
bench_compiled "Go"   "$GO_COMPILE"   "$GO_RUN"
bench_compiled "Java" "$JAVA_COMPILE" "$JAVA_RUN"
echo "────────────────────────────────────────────────────────────────"
bench_interpreted "Python"  "$PYTHON_RUN"
bench_interpreted "Node.js" "$JS_RUN"
bench_interpreted "SQLite"  "$SQLITE_RUN"

# ─── Part 2: Throughput at 8/32/96 parallel ───────────────────────────────────

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Part 2: Throughput — Python hello-world (compile+run representative)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

THROUGHPUT_PAYLOAD='{"cmd":[{"args":["/usr/bin/python3","-c","print(42)"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":""},{"name":"stdout","max":4096},{"name":"stderr","max":4096}],"cpuLimit":5000000000,"memoryLimit":134217728,"procLimit":20,"copyOut":["stdout","stderr"]}]}'

# Also test C compile+run as a compiled-language throughput test
C_THROUGHPUT_COMPILE='{"cmd":[{"args":["/usr/bin/gcc","-o","/w/a.out","/w/t.c","-O2"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":""},{"name":"stdout","max":4096},{"name":"stderr","max":4096}],"cpuLimit":10000000000,"memoryLimit":268435456,"procLimit":50,"copyIn":{"/w/t.c":{"content":"#include <stdio.h>\nint main(){printf(\"42\\n\");return 0;}"}},"copyOut":["stdout","stderr"],"copyOutCached":["/w/a.out"]}]}'

run_throughput_test() {
  local label="$1" parallelism="$2" total="$3" payload="$4"

  local t0 completed=0 failed=0
  t0=$(ts_ms)

  # Use GNU parallel via xargs -P
  local tmpdir
  tmpdir=$(mktemp -d)

  for i in $(seq 1 "$total"); do
    echo "$i"
  done | xargs -I{} -P "$parallelism" bash -c "
    resp=\$(curl -sf -X POST '$URL/run' -H '$AUTH' -H '$CT' -d '$payload' 2>/dev/null)
    status=\$(echo \"\$resp\" | python3 -c \"import sys,json; r=json.load(sys.stdin); print(r[0]['status'])\" 2>/dev/null || echo 'FAIL')
    echo \"\$status\" > '$tmpdir/{}.status'
  " 2>/dev/null

  local t1
  t1=$(ts_ms)

  completed=$(grep -rl "Accepted" "$tmpdir"/ 2>/dev/null | wc -l)
  failed=$((total - completed))
  local elapsed_ms=$((t1 - t0))
  local elapsed_s
  elapsed_s=$(python3 -c "print(f'{$elapsed_ms/1000:.2f}')")
  local rate
  rate=$(python3 -c "print(f'{$total/($elapsed_ms/1000):.1f}')")

  printf "  %-30s %6d jobs, %4d parallel → %6s ms (%5s/s)  ok=%d fail=%d\n" \
    "$label" "$total" "$parallelism" "$elapsed_ms" "$rate" "$completed" "$failed"

  rm -rf "$tmpdir"
}

printf "  %-30s %6s       %4s          %6s  (%5s  )  %s\n" \
  "Test" "Jobs" "Para" "Time" "Rate" "Status"
echo "  ──────────────────────────────────────────────────────────────────────"

# Python throughput at different parallelism levels
run_throughput_test "Python trivial @8"   8  64  "$THROUGHPUT_PAYLOAD"
run_throughput_test "Python trivial @32"  32 128 "$THROUGHPUT_PAYLOAD"
run_throughput_test "Python trivial @96"  96 288 "$THROUGHPUT_PAYLOAD"

echo ""

# C compile+run throughput (more realistic — two API calls per job)
run_c_throughput() {
  local parallelism="$1" total="$2"

  local t0 tmpdir
  t0=$(ts_ms)
  tmpdir=$(mktemp -d)

  for i in $(seq 1 "$total"); do
    echo "$i"
  done | xargs -I{} -P "$parallelism" bash -c "
    # Step 1: compile
    cresp=\$(curl -sf -X POST '$URL/run' -H '$AUTH' -H '$CT' -d '$C_THROUGHPUT_COMPILE' 2>/dev/null)
    fid=\$(echo \"\$cresp\" | python3 -c \"import sys,json; r=json.load(sys.stdin); fids=r[0].get('fileIds',{}); print(next(iter(fids.values()),''))\" 2>/dev/null)
    if [ -z \"\$fid\" ]; then echo 'COMPILE_FAIL' > '$tmpdir/{}.status'; exit; fi

    # Step 2: run
    run_payload='{\"cmd\":[{\"args\":[\"/w/a.out\"],\"env\":[\"PATH=/usr/bin:/bin\"],\"files\":[{\"content\":\"\"},{\"name\":\"stdout\",\"max\":4096},{\"name\":\"stderr\",\"max\":4096}],\"cpuLimit\":5000000000,\"memoryLimit\":268435456,\"procLimit\":10,\"copyIn\":{\"/w/a.out\":{\"fileId\":\"'\"\$fid\"'\"}},\"copyOut\":[\"stdout\",\"stderr\"]}]}'
    rresp=\$(curl -sf -X POST '$URL/run' -H '$AUTH' -H '$CT' -d \"\$run_payload\" 2>/dev/null)
    status=\$(echo \"\$rresp\" | python3 -c \"import sys,json; r=json.load(sys.stdin); print(r[0]['status'])\" 2>/dev/null || echo 'FAIL')
    echo \"\$status\" > '$tmpdir/{}.status'
  " 2>/dev/null

  local t1
  t1=$(ts_ms)

  local completed
  completed=$(grep -rl "Accepted" "$tmpdir"/ 2>/dev/null | wc -l)
  local failed=$((total - completed))
  local elapsed_ms=$((t1 - t0))
  local elapsed_s rate
  elapsed_s=$(python3 -c "print(f'{$elapsed_ms/1000:.2f}')")
  rate=$(python3 -c "print(f'{$total/($elapsed_ms/1000):.1f}')")

  printf "  %-30s %6d jobs, %4d parallel → %6s ms (%5s/s)  ok=%d fail=%d\n" \
    "C compile+run @$parallelism" "$total" "$parallelism" "$elapsed_ms" "$rate" "$completed" "$failed"

  rm -rf "$tmpdir"
}

run_c_throughput 8  64
run_c_throughput 32 128
run_c_throughput 96 288

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Part 3: go-judge queue saturation test"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "go-judge parallelism is 8. Sending 96 concurrent requests to see"
echo "how it queues internally (requests > parallelism)."
echo ""

SATURATION_PAYLOAD='{"cmd":[{"args":["/usr/bin/python3","-c","import time; time.sleep(0.1); print(42)"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":""},{"name":"stdout","max":4096},{"name":"stderr","max":4096}],"cpuLimit":5000000000,"memoryLimit":134217728,"procLimit":20,"copyOut":["stdout","stderr"]}]}'

run_throughput_test "Sleep 100ms @8 (baseline)"  8  32  "$SATURATION_PAYLOAD"
run_throughput_test "Sleep 100ms @32 (4x over)"   32 96  "$SATURATION_PAYLOAD"
run_throughput_test "Sleep 100ms @96 (12x over)"  96 96  "$SATURATION_PAYLOAD"

echo ""
echo "Done. $(date -Iseconds)"
