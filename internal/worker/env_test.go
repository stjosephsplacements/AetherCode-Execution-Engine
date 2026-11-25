package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/sandbox"
)

// envRecordingJudge is a fake go-judge that records every run request and
// answers each Cmd with an accepted "ok" stdout. Compile/cache requests
// (CopyOutCached set) get a fixed file ID so interactor setup succeeds.
func envRecordingJudge(t *testing.T) (*Worker, func() []sandbox.Request) {
	t.Helper()
	var mu sync.Mutex
	var runs []sandbox.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req sandbox.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		res := make([]sandbox.Result, len(req.Cmd))
		if len(req.Cmd) == 1 && len(req.Cmd[0].CopyOutCached) > 0 {
			res[0] = sandbox.Result{Status: sandbox.StatusAccepted, FileIDs: map[string]string{"/w/a.out": "interactor-id"}}
			_ = json.NewEncoder(w).Encode(res)
			return
		}
		mu.Lock()
		runs = append(runs, req)
		mu.Unlock()
		for i := range res {
			res[i] = sandbox.Result{Status: sandbox.StatusAccepted, Files: map[string]string{"stdout": "ok"}}
		}
		_ = json.NewEncoder(w).Encode(res)
	}))
	t.Cleanup(srv.Close)

	w := New(nil, sandbox.NewClient(srv.URL, "dummy"), nil, nil, nil, 10*time.Second, 1, 5*time.Minute)
	return w, func() []sandbox.Request {
		mu.Lock()
		defer mu.Unlock()
		return append([]sandbox.Request(nil), runs...)
	}
}

func withEnv(base []string, extra ...string) []string {
	return append(append([]string{}, base...), extra...)
}

// The issue asks that injected env be observable by the running code. Unit
// tests must never execute real submissions, so the observable boundary here
// is the go-judge request: whatever Cmd.Env carries is exactly the env the
// sandboxed process is exec'd with.
func TestRunTestsBatched_ForwardsPerTestEnv(t *testing.T) {
	w, runs := envRecordingJudge(t)
	lang := sandbox.Languages["python"]
	tests := []testData{
		{Input: "1", ExpectedOutput: "ok", Env: []string{"TZ=UTC", "AC_MODE=strict"}},
		{Input: "2", ExpectedOutput: "ok"},
	}

	// A non-empty fileID skips the source pre-cache request, so the only
	// recorded run is the test batch itself.
	_, _, _, passed, verdict, err := w.runTestsBatched(context.Background(), "job-env", lang, "print('ok')", tests, "src-id", sandbox.RunLimits{}, nil, slog.Default())
	if err != nil {
		t.Fatalf("runTestsBatched: %v", err)
	}
	if passed != 2 || verdict != model.VerdictAccepted {
		t.Fatalf("passed=%d verdict=%v, want 2 Accepted", passed, verdict)
	}

	got := runs()
	if len(got) != 1 || len(got[0].Cmd) != 2 {
		t.Fatalf("want one batch of 2 cmds, got %+v", got)
	}
	if want := withEnv(lang.RunEnv, "TZ=UTC", "AC_MODE=strict"); !reflect.DeepEqual(got[0].Cmd[0].Env, want) {
		t.Errorf("test 0 env = %q, want %q", got[0].Cmd[0].Env, want)
	}
	// Tests in the same batch must not share env.
	if !reflect.DeepEqual(got[0].Cmd[1].Env, lang.RunEnv) {
		t.Errorf("test 1 env = %q, want language default %q", got[0].Cmd[1].Env, lang.RunEnv)
	}
}

func TestRunTestsBatched_InteractiveEnvOnlyReachesStudent(t *testing.T) {
	w, runs := envRecordingJudge(t)
	lang := sandbox.Languages["python"]
	tests := []testData{{
		Input: "1", ExpectedOutput: "ok", Env: []string{"TZ=UTC"},
		InteractorCode: "int main(){}", InteractorLanguage: "cpp",
	}}

	if _, _, _, _, _, err := w.runTestsBatched(context.Background(), "job-env-int", lang, "print('ok')", tests, "src-id", sandbox.RunLimits{}, nil, slog.Default()); err != nil {
		t.Fatalf("runTestsBatched: %v", err)
	}

	got := runs()
	if len(got) != 1 || len(got[0].Cmd) != 2 {
		t.Fatalf("want one interactive run of 2 cmds, got %+v", got)
	}
	if want := withEnv(lang.RunEnv, "TZ=UTC"); !reflect.DeepEqual(got[0].Cmd[0].Env, want) {
		t.Errorf("student env = %q, want %q", got[0].Cmd[0].Env, want)
	}
	if want := sandbox.Languages["cpp"].RunEnv; !reflect.DeepEqual(got[0].Cmd[1].Env, want) {
		t.Errorf("interactor env = %q, want its language default %q", got[0].Cmd[1].Env, want)
	}
}
