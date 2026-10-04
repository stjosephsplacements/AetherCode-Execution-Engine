#!/usr/bin/env bash
# STJ-Exec end-to-end test via HTTP API
# Requires: stj-exec running on 127.0.0.1:5100
set -euo pipefail

URL="http://127.0.0.1:5100"
PASS=0; FAIL=0; TOTAL=0

submit_and_check() {
  local name="$1" payload="$2" expected_verdict="$3"
  TOTAL=$((TOTAL+1))

  local resp
  resp=$(curl -sf -X POST "$URL/api/v1/execute" -H "Content-Type: application/json" -d "$payload" 2>&1) || {
    echo "  FAIL $name — submit failed: $resp"; FAIL=$((FAIL+1)); return
  }

  local job_id
  job_id=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null)
  if [[ -z "$job_id" ]]; then
    echo "  FAIL $name — no job_id in response"; FAIL=$((FAIL+1)); return
  fi

  # Read SSE stream until VERDICT
  local sse_output verdict
  sse_output=$(timeout 30 curl -sN "$URL/api/v1/stream?job_id=$job_id" 2>/dev/null || true)
  verdict=$(echo "$sse_output" | grep '"type":"VERDICT"' | head -1 | python3 -c "
import sys,json
for line in sys.stdin:
    line = line.strip()
    if line.startswith('data: '):
        d = json.loads(line[6:])
        print(d.get('data',{}).get('verdict',''))
        break
" 2>/dev/null || echo "TIMEOUT")

  if [[ "$verdict" == "$expected_verdict" ]]; then
    local detail=""
    if [[ "$expected_verdict" == "accepted" ]]; then
      local passed total_tests
      passed=$(echo "$sse_output" | grep '"type":"VERDICT"' | head -1 | python3 -c "
import sys,json
for line in sys.stdin:
    line = line.strip()
    if line.startswith('data: '):
        d = json.loads(line[6:])
        print(f\"{d.get('data',{}).get('tests_passed',0)}/{d.get('data',{}).get('test_count',0)}\")
        break
" 2>/dev/null || echo "?/?")
      detail=" ($passed tests)"
    fi
    echo "  PASS $name — verdict=$verdict$detail"
    PASS=$((PASS+1))
  else
    echo "  FAIL $name — expected=$expected_verdict got=$verdict"
    FAIL=$((FAIL+1))
  fi
}

echo "=========================================="
echo " STJ-Exec End-to-End Test Suite"
echo " $(date -Iseconds)"
echo "=========================================="
echo ""

# ── Python ──
echo "── Python ──"
submit_and_check "Python accepted" \
  '{"language":"python","source_code":"n=int(input())\nprint(n*2)","tests":[{"input":"21\n","expected_output":"42\n"},{"input":"5\n","expected_output":"10\n"}]}' \
  "accepted"

submit_and_check "Python wrong answer" \
  '{"language":"python","source_code":"n=int(input())\nprint(n+1)","tests":[{"input":"5\n","expected_output":"10\n"}]}' \
  "wrong_answer"

submit_and_check "Python runtime error" \
  '{"language":"python","source_code":"print(1/0)","tests":[{"input":"","expected_output":"0\n"}]}' \
  "runtime_error"

submit_and_check "Python TLE" \
  '{"language":"python","source_code":"while True: pass","tests":[{"input":"","expected_output":"done\n"}]}' \
  "time_limit"

# ── C ──
echo "── C ──"
submit_and_check "C accepted" \
  '{"language":"c","source_code":"#include <stdio.h>\nint main() { int n; scanf(\"%d\", &n); printf(\"%d\\n\", n*2); return 0; }","tests":[{"input":"21\n","expected_output":"42\n"},{"input":"5\n","expected_output":"10\n"}]}' \
  "accepted"

submit_and_check "C compile error" \
  '{"language":"c","source_code":"int main() { undeclared_var = 42; }","tests":[{"input":"","expected_output":"42\n"}]}' \
  "compilation_error"

# ── C++ ──
echo "── C++ ──"
submit_and_check "C++ accepted" \
  '{"language":"cpp","source_code":"#include <iostream>\nusing namespace std;\nint main() { int n; cin >> n; cout << n*2 << endl; return 0; }","tests":[{"input":"21\n","expected_output":"42\n"}]}' \
  "accepted"

# ── Java ──
echo "── Java ──"
submit_and_check "Java accepted" \
  '{"language":"java","source_code":"import java.util.Scanner;\npublic class Main {\n  public static void main(String[] args) {\n    Scanner sc = new Scanner(System.in);\n    int n = sc.nextInt();\n    System.out.println(n * 2);\n  }\n}","tests":[{"input":"21\n","expected_output":"42\n"}]}' \
  "accepted"

# ── JavaScript ──
echo "── JavaScript ──"
submit_and_check "JS accepted" \
  '{"language":"javascript","source_code":"const readline = require(\"readline\");\nconst rl = readline.createInterface({ input: process.stdin });\nrl.on(\"line\", (line) => { console.log(parseInt(line) * 2); rl.close(); });","tests":[{"input":"21\n","expected_output":"42\n"}]}' \
  "accepted"

# ── SQLite ──
echo "── SQLite ──"
submit_and_check "SQLite accepted" \
  '{"language":"sqlite","source_code":"CREATE TABLE t(x INTEGER);\nINSERT INTO t VALUES(1),(2),(3);\nSELECT SUM(x) FROM t;","tests":[{"input":"","expected_output":"6\n"}]}' \
  "accepted"

# ── Go ──
echo "── Go ──"
submit_and_check "Go accepted" \
  '{"language":"go","source_code":"package main\nimport \"fmt\"\nfunc main() {\n\tvar n int\n\tfmt.Scan(&n)\n\tfmt.Println(n * 2)\n}","tests":[{"input":"21\n","expected_output":"42\n"}]}' \
  "accepted"

echo ""
echo "=========================================="
echo " Results: $PASS/$TOTAL passed, $FAIL failed"
echo "=========================================="
