package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/sandbox"
)

const rustSolution = `use std::io::{self, Read};
fn main() {
    let mut s = String::new();
    io::stdin().read_to_string(&mut s).unwrap();
    let n: i64 = s.trim().parse().unwrap();
    let small: u8 = n.try_into().unwrap_or(255);
    println!("{}", small);
}
`

// rustJudge is a fake go-judge for Rust jobs. Unit tests never run real
// submissions, so it answers the compile request with compileResult (adding
// a cached binary ID when the compile is accepted) and every run Cmd with
// stdout "42". It records every request it receives.
func rustJudge(t *testing.T, compileResult sandbox.Result) (*Worker, func() []sandbox.Request) {
	t.Helper()
	var mu sync.Mutex
	var reqs []sandbox.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req sandbox.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		reqs = append(reqs, req)
		mu.Unlock()
		res := make([]sandbox.Result, len(req.Cmd))
		for i, cmd := range req.Cmd {
			if slices.Contains(cmd.CopyOutCached, "/w/a.out") {
				res[i] = compileResult
				if compileResult.Status == sandbox.StatusAccepted {
					res[i].FileIDs = map[string]string{"/w/a.out": "rust-bin"}
				}
				continue
			}
			res[i] = sandbox.Result{Status: sandbox.StatusAccepted, Files: map[string]string{"stdout": "42\n"}}
		}
		_ = json.NewEncoder(w).Encode(res)
	}))
	t.Cleanup(srv.Close)

	w := New(nil, sandbox.NewClient(srv.URL, "dummy"), nil, nil, 10*time.Second, 1, 5*time.Minute)
	return w, func() []sandbox.Request {
		mu.Lock()
		defer mu.Unlock()
		return append([]sandbox.Request(nil), reqs...)
	}
}

// A Rust job compiles main.rs as edition 2021 and runs the cached binary for
// every test.
func TestRustCompilesAsEdition2021AndRunsTests(t *testing.T) {
	w, reqs := rustJudge(t, sandbox.Result{Status: sandbox.StatusAccepted})
	lang := sandbox.Languages["rust"]

	compiled, err := w.compileWithOptions(context.Background(), lang, rustSolution, nil, nil)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if compiled.Status != sandbox.StatusAccepted || compiled.FileIDs["/w/a.out"] != "rust-bin" {
		t.Fatalf("compile result = %+v, want accepted with the cached binary", compiled)
	}

	tests := []testData{{Input: "42", ExpectedOutput: "42"}, {Input: "42", ExpectedOutput: "42"}}
	_, _, _, passed, verdict, err := w.runTestsBatched(context.Background(), "job-rust", lang, rustSolution, tests, "rust-bin", sandbox.RunLimits{}, nil, slog.Default())
	if err != nil {
		t.Fatalf("runTestsBatched: %v", err)
	}
	if passed != 2 || verdict != model.VerdictAccepted {
		t.Fatalf("passed=%d verdict=%v, want 2 Accepted", passed, verdict)
	}

	got := reqs()
	if len(got) != 2 {
		t.Fatalf("want a compile request and one test batch, got %d requests", len(got))
	}
	compile := got[0].Cmd[0]
	if compile.Args[0] != "/usr/bin/rustc" || !slices.Contains(compile.Args, "--edition=2021") {
		t.Errorf("compile args = %q, want rustc with --edition=2021", compile.Args)
	}
	if src := compile.CopyIn["/w/main.rs"].Content; src == nil || *src != rustSolution {
		t.Errorf("compile did not copy the source to /w/main.rs")
	}
	for i, cmd := range got[1].Cmd {
		if !slices.Equal(cmd.Args, lang.RunArgs) {
			t.Errorf("test %d args = %q, want %q", i, cmd.Args, lang.RunArgs)
		}
		if id := cmd.CopyIn["/w/a.out"].FileID; id == nil || *id != "rust-bin" {
			t.Errorf("test %d does not run the compiled binary", i)
		}
	}
}

// A caller's --edition replaces the default instead of following it: rustc
// fails with "Option 'edition' given more than once" otherwise.
func TestRustCallerEditionReplacesDefault(t *testing.T) {
	w, reqs := rustJudge(t, sandbox.Result{Status: sandbox.StatusAccepted})

	if _, err := w.compileWithOptions(context.Background(), sandbox.Languages["rust"], rustSolution, nil, []string{"--edition=2018"}); err != nil {
		t.Fatalf("compile: %v", err)
	}
	args := reqs()[0].Cmd[0].Args
	editions := slices.DeleteFunc(slices.Clone(args), func(a string) bool { return !strings.HasPrefix(a, "--edition") })
	if !slices.Equal(editions, []string{"--edition=2018"}) {
		t.Fatalf("compile args = %q, want exactly one --edition=2018", args)
	}
}

// A Rust compile failure comes back as a non-accepted result carrying
// rustc's diagnostics, which process turns into a Compilation Error verdict.
func TestRustCompileErrorKeepsDiagnostics(t *testing.T) {
	stderr := "error[E0308]: mismatched types\n"
	w, _ := rustJudge(t, sandbox.Result{Status: sandbox.StatusNonzeroExit, ExitStatus: 1, Files: map[string]string{"stderr": stderr}})

	res, err := w.compileWithOptions(context.Background(), sandbox.Languages["rust"], `fn main() { let x: i32 = "nope"; }`, nil, nil)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if res.Status == sandbox.StatusAccepted || res.Files["stderr"] != stderr {
		t.Fatalf("compile result = %+v, want a failure with rustc's stderr", res)
	}
}
