package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/sandbox"
)

func TestCustomChecker_Evaluate(t *testing.T) {
	// Mock judge server handling custom checker requests
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req sandbox.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		res := make([]sandbox.Result, len(req.Cmd))
		for i, cmd := range req.Cmd {
			// Check if this is the compile step or the execution step
			if len(cmd.CopyOutCached) > 0 {
				res[i] = sandbox.Result{
					Status:  sandbox.StatusAccepted,
					FileIDs: map[string]string{"/w/a.out": "mock-cached-checker-id"},
				}
				continue
			}

			// If it's running the checker:
			// Let's inspect /w/user_output.txt
			userOut := ""
			if f, ok := cmd.CopyIn["/w/user_output.txt"]; ok && f.Content != nil {
				userOut = *f.Content
			}
			if userOut == "valid answer\n" || userOut == "accepted" {
				res[i] = sandbox.Result{
					Status:     sandbox.StatusAccepted,
					ExitStatus: 0,
					Files:      map[string]string{"stdout": "OK", "stderr": ""},
				}
			} else {
				res[i] = sandbox.Result{
					Status:     sandbox.StatusNonzeroExit,
					ExitStatus: 1,
					Files:      map[string]string{"stdout": "", "stderr": "WA: answer does not satisfy graph conditions"},
				}
			}
		}
		_ = json.NewEncoder(w).Encode(res)
	}))
	defer srv.Close()

	judgeClient := sandbox.NewClient(srv.URL, "dummy")
	w := New(nil, judgeClient, nil, nil, 10*time.Second, 1, 5*time.Minute)

	ctx := context.Background()

	// 1. Prepare custom checker
	cc, err := prepareSandboxProgram(ctx, "checker", "cpp", "#include <iostream> ...", w.cc, w.judge, slog.Default())
	if err != nil {
		t.Fatalf("prepareCustomChecker failed: %v", err)
	}
	if cc.fileID != "mock-cached-checker-id" {
		t.Errorf("expected cached fileID 'mock-cached-checker-id', got %q", cc.fileID)
	}

	// 2. Evaluate accepted answer
	verdict, errLog := w.evaluateCustomChecker(ctx, cc, "10\n", "expected\n", "valid answer\n")
	if verdict != model.VerdictAccepted {
		t.Errorf("expected Accepted, got %v (err: %s)", verdict, errLog)
	}

	// 3. Evaluate wrong answer
	verdict, errLog = w.evaluateCustomChecker(ctx, cc, "10\n", "expected\n", "wrong answer\n")
	if verdict != model.VerdictWrongAnswer {
		t.Errorf("expected WrongAnswer, got %v (err: %s)", verdict, errLog)
	}
	if errLog != "WA: answer does not satisfy graph conditions" {
		t.Errorf("unexpected errLog: %s", errLog)
	}
}
