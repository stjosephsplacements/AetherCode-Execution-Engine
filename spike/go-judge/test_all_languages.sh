#!/usr/bin/env bash
# Test all target languages against the go-judge spike.
# Usage: ./test_all_languages.sh
set -euo pipefail

URL="http://127.0.0.1:5050"
AUTH="Authorization: Bearer stj-spike-2024"
PASS=0; FAIL=0; TOTAL=0

run_test() {
  local name="$1" payload="$2" expect="$3"
  TOTAL=$((TOTAL+1))
  local resp
  resp=$(curl -sf -X POST "$URL/run" -H "$AUTH" -H "Content-Type: application/json" -d "$payload" 2>&1) || {
    echo "  ❌ $name — curl failed: $resp"; FAIL=$((FAIL+1)); return
  }
  local status stdout stderr
  status=$(echo "$resp" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0]['status'])" 2>/dev/null || echo "PARSE_ERROR")
  stdout=$(echo "$resp" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0].get('files',{}).get('stdout',''))" 2>/dev/null || echo "")
  stderr=$(echo "$resp" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0].get('files',{}).get('stderr',''))" 2>/dev/null || echo "")

  if [[ "$status" == "Accepted" ]] && [[ "$stdout" == *"$expect"* ]]; then
    echo "  ✅ $name — output: $(echo "$stdout" | head -1)"
    PASS=$((PASS+1))
  else
    echo "  ❌ $name — status=$status stdout='$(echo "$stdout" | head -1)' stderr='$(echo "$stderr" | head -1)'"
    FAIL=$((FAIL+1))
  fi
}

# Helper: two-step compile+run test
run_compiled_test() {
  local name="$1" compile_payload="$2" run_payload="$3" expect="$4"
  TOTAL=$((TOTAL+1))

  # Step 1: compile
  local cresp
  cresp=$(curl -sf -X POST "$URL/run" -H "$AUTH" -H "Content-Type: application/json" -d "$compile_payload" 2>&1) || {
    echo "  ❌ $name [compile] — curl failed"; FAIL=$((FAIL+1)); return
  }
  local cstatus cstderr
  cstatus=$(echo "$cresp" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0]['status'])" 2>/dev/null || echo "PARSE_ERROR")
  cstderr=$(echo "$cresp" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0].get('files',{}).get('stderr',''))" 2>/dev/null || echo "")
  local fid
  fid=$(echo "$cresp" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0].get('fileIds',{}).get('/w/a.out','') or r[0].get('fileIds',{}).get('/w/Main.class','') or r[0].get('fileIds',{}).get('/w/main',''))" 2>/dev/null || echo "")

  if [[ "$cstatus" != "Accepted" ]]; then
    echo "  ❌ $name [compile] — status=$cstatus stderr='$(echo "$cstderr" | head -1)'"
    FAIL=$((FAIL+1)); return
  fi

  if [[ -z "$fid" ]]; then
    echo "  ❌ $name [compile] — no cached file ID returned"
    FAIL=$((FAIL+1)); return
  fi

  # Step 2: run, injecting the cached fileId
  local final_payload
  final_payload=$(echo "$run_payload" | sed "s|CACHED_FILE_ID|$fid|g")

  local rresp
  rresp=$(curl -sf -X POST "$URL/run" -H "$AUTH" -H "Content-Type: application/json" -d "$final_payload" 2>&1) || {
    echo "  ❌ $name [run] — curl failed"; FAIL=$((FAIL+1)); return
  }
  local rstatus rstdout rstderr
  rstatus=$(echo "$rresp" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0]['status'])" 2>/dev/null || echo "PARSE_ERROR")
  rstdout=$(echo "$rresp" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0].get('files',{}).get('stdout',''))" 2>/dev/null || echo "")
  rstderr=$(echo "$rresp" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0].get('files',{}).get('stderr',''))" 2>/dev/null || echo "")

  if [[ "$rstatus" == "Accepted" ]] && [[ "$rstdout" == *"$expect"* ]]; then
    echo "  ✅ $name — output: $(echo "$rstdout" | head -1)"
    PASS=$((PASS+1))
  else
    echo "  ❌ $name [run] — status=$rstatus stdout='$(echo "$rstdout" | head -1)' stderr='$(echo "$rstderr" | head -1)'"
    FAIL=$((FAIL+1))
  fi
}

echo "=========================================="
echo " STJ-Exec go-judge Spike — Language Tests"
echo "=========================================="
echo ""

# ── 1. C ──────────────────────────────────────────
echo "── C (gcc) ──"
run_compiled_test "C hello" \
  '{"cmd":[{"args":["/usr/bin/gcc","-o","/w/a.out","/w/main.c","-O2","-lm"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":10000000000,"memoryLimit":268435456,"procLimit":50,"copyIn":{"/w/main.c":{"content":"#include <stdio.h>\nint main() { printf(\"Hello C 2024!\\n\"); return 0; }"}},"copyOut":["stdout","stderr"],"copyOutCached":["/w/a.out"]}]}' \
  '{"cmd":[{"args":["/w/a.out"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":5000000000,"memoryLimit":268435456,"procLimit":10,"copyIn":{"/w/a.out":{"fileId":"CACHED_FILE_ID"}},"copyOut":["stdout","stderr"]}]}' \
  "Hello C 2024!"

# ── 2. C++ ────────────────────────────────────────
echo "── C++ (g++) ──"
run_compiled_test "C++ hello" \
  '{"cmd":[{"args":["/usr/bin/g++","-o","/w/a.out","/w/main.cpp","-O2","-std=c++17"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":10000000000,"memoryLimit":268435456,"procLimit":50,"copyIn":{"/w/main.cpp":{"content":"#include <iostream>\\nusing namespace std;\\nint main() { cout << \"Hello C++ 17!\" << endl; return 0; }"}},"copyOut":["stdout","stderr"],"copyOutCached":["/w/a.out"]}]}' \
  '{"cmd":[{"args":["/w/a.out"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":5000000000,"memoryLimit":268435456,"procLimit":10,"copyIn":{"/w/a.out":{"fileId":"CACHED_FILE_ID"}},"copyOut":["stdout","stderr"]}]}' \
  "Hello C++ 17!"

# ── 3. Python ─────────────────────────────────────
echo "── Python 3 ──"
run_test "Python hello" \
  '{"cmd":[{"args":["/usr/bin/python3","/w/main.py"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":10000000000,"memoryLimit":268435456,"procLimit":50,"copyIn":{"/w/main.py":{"content":"print(\"Hello Python 3!\")"}},"copyOut":["stdout","stderr"]}]}' \
  "Hello Python 3!"

# ── 4. Python with stdin ──────────────────────────
echo "── Python 3 (with stdin) ──"
run_test "Python stdin" \
  '{"cmd":[{"args":["/usr/bin/python3","/w/main.py"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":"42\n"},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":10000000000,"memoryLimit":268435456,"procLimit":50,"copyIn":{"/w/main.py":{"content":"n = int(input())\nprint(f\"Answer: {n * 2}\")"}},"copyOut":["stdout","stderr"]}]}' \
  "Answer: 84"

# ── 5. Go ─────────────────────────────────────────
echo "── Go ──"
run_compiled_test "Go hello" \
  '{"cmd":[{"args":["/usr/bin/go","build","-o","/w/main","/w/main.go"],"env":["PATH=/usr/bin:/bin","GOROOT=/usr/lib/go-1.22","HOME=/tmp","GOCACHE=/tmp/go-cache"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":30000000000,"memoryLimit":536870912,"procLimit":50,"copyIn":{"/w/main.go":{"content":"package main\\nimport \"fmt\"\\nfunc main() { fmt.Println(\"Hello Go!\") }"}},"copyOut":["stdout","stderr"],"copyOutCached":["/w/main"]}]}' \
  '{"cmd":[{"args":["/w/main"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":5000000000,"memoryLimit":268435456,"procLimit":10,"copyIn":{"/w/main":{"fileId":"CACHED_FILE_ID"}},"copyOut":["stdout","stderr"]}]}' \
  "Hello Go!"

# ── 6. Java ───────────────────────────────────────
echo "── Java 21 ──"
run_compiled_test "Java hello" \
  '{"cmd":[{"args":["/usr/bin/javac","-d","/w","/w/Main.java"],"env":["PATH=/usr/bin:/bin","JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":15000000000,"memoryLimit":536870912,"procLimit":50,"copyIn":{"/w/Main.java":{"content":"public class Main { public static void main(String[] args) { System.out.println(\"Hello Java 21!\"); } }"}},"copyOut":["stdout","stderr"],"copyOutCached":["/w/Main.class"]}]}' \
  '{"cmd":[{"args":["/usr/bin/java","-cp","/w","Main"],"env":["PATH=/usr/bin:/bin","JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":10000000000,"memoryLimit":536870912,"procLimit":50,"copyIn":{"/w/Main.class":{"fileId":"CACHED_FILE_ID"}},"copyOut":["stdout","stderr"]}]}' \
  "Hello Java 21!"

# ── 7. JavaScript (Node.js) ───────────────────────
echo "── JavaScript (Node.js) ──"
run_test "JS hello" \
  '{"cmd":[{"args":["/usr/bin/node","/w/main.js"],"env":["PATH=/usr/bin:/bin","HOME=/tmp"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":10000000000,"memoryLimit":268435456,"procLimit":50,"copyIn":{"/w/main.js":{"content":"console.log(\"Hello Node.js!\");"}},"copyOut":["stdout","stderr"]}]}' \
  "Hello Node.js!"

# ── 8. SQLite ─────────────────────────────────────
echo "── SQL (SQLite3) ──"
run_test "SQLite query" \
  '{"cmd":[{"args":["/usr/bin/sqlite3",":memory:"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":"CREATE TABLE t(x INTEGER);\nINSERT INTO t VALUES(1),(2),(3);\nSELECT SUM(x) FROM t;\n"},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":5000000000,"memoryLimit":134217728,"procLimit":10,"copyOut":["stdout","stderr"]}]}' \
  "6"

# ── 9. PostgreSQL (psql) ─────────────────────────
echo "── PL/pgSQL (psql) ──"
# psql needs a running Postgres. For the spike we test that psql binary works.
# In production, a sandboxed PG instance will be used per submission.
TOTAL=$((TOTAL+1))
psql_resp=$(curl -sf -X POST "$URL/run" -H "$AUTH" -H "Content-Type: application/json" -d '{
  "cmd":[{"args":["/usr/bin/psql","--version"],"env":["PATH=/usr/bin:/bin"],"files":[{"content":""},{"name":"stdout","max":65536},{"name":"stderr","max":65536}],"cpuLimit":5000000000,"memoryLimit":134217728,"procLimit":10,"copyOut":["stdout","stderr"]}]
}' 2>&1)
psql_out=$(echo "$psql_resp" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0].get('files',{}).get('stdout',''))" 2>/dev/null || echo "")
if [[ "$psql_out" == *"psql (PostgreSQL)"* ]]; then
  echo "  ✅ psql binary works — $psql_out"
  PASS=$((PASS+1))
else
  psql_status=$(echo "$psql_resp" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0]['status'])" 2>/dev/null || echo "UNKNOWN")
  psql_stderr=$(echo "$psql_resp" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0].get('files',{}).get('stderr',''))" 2>/dev/null || echo "")
  echo "  ❌ psql — status=$psql_status stderr='$psql_stderr'"
  FAIL=$((FAIL+1))
fi

echo ""
echo "=========================================="
echo " Results: $PASS/$TOTAL passed, $FAIL failed"
echo "=========================================="
