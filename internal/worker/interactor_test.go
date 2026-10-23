package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/sandbox"
)

func TestInteractiveProblems_Workflow(t *testing.T) {
	// Mock judge server handling interactive execution with PipeMapping
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req sandbox.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}

		res := make([]sandbox.Result, len(req.Cmd))
		// Check if it's the interactor compile step
		if len(req.Cmd) == 1 && len(req.Cmd[0].CopyOutCached) > 0 {
			res[0] = sandbox.Result{
				Status:  sandbox.StatusAccepted,
				FileIDs: map[string]string{"/w/a.out": "mock-cached-interactor-id"},
			}
			_ = json.NewEncoder(w).Encode(res)
			return
		}

		// Interactive execution: should receive 2 Cmds and 2 PipeMappings
		if len(req.Cmd) == 2 && len(req.PipeMapping) == 2 {
			// Cmd 0: student
			res[0] = sandbox.Result{
				Status:     sandbox.StatusAccepted,
				ExitStatus: 0,
				Time:       1000000,
				Memory:     1024,
			}
			// Cmd 1: interactor
			res[1] = sandbox.Result{
				Status:     sandbox.StatusAccepted,
				ExitStatus: 0,
				Files:      map[string]string{"stderr": "interactor: all 5 queries answered correctly"},
			}
			_ = json.NewEncoder(w).Encode(res)
			return
		}

		// Fallback for any other call
		for i := range res {
			res[i] = sandbox.Result{Status: sandbox.StatusAccepted}
		}
		_ = json.NewEncoder(w).Encode(res)
	}))
	defer srv.Close()

	judgeClient := sandbox.NewClient(srv.URL, "dummy")
	w := New(nil, judgeClient, nil, nil, 10*time.Second, 1, 5*time.Minute)

	ctx := context.Background()

	// 1. Prepare interactor
	it, err := w.prepareInteractor(ctx, "cpp", "#include <iostream> ...", nil)
	if err != nil {
		t.Fatalf("prepareInteractor: %v", err)
	}
	if it.fileID != "mock-cached-interactor-id" {
		t.Errorf("expected cached fileID, got %q", it.fileID)
	}

	// 2. Build interactive cmds and verify pipe mappings
	lang := sandbox.Languages["python"]
	cmds, pipes := buildInteractiveCmds(lang, "print('interactive')", "", sandbox.RunLimits{}, nil, it, "input_secret\n", "answer\n")
	if len(cmds) != 2 {
		t.Fatalf("expected 2 cmds, got %d", len(cmds))
	}
	if len(pipes) != 2 {
		t.Fatalf("expected 2 pipe mappings, got %d", len(pipes))
	}
	if pipes[0].In.Index != 0 || pipes[0].In.Fd != 1 || pipes[0].Out.Index != 1 || pipes[0].Out.Fd != 0 {
		t.Errorf("pipe 0 mapping mismatch: %+v", pipes[0])
	}
	if pipes[1].In.Index != 1 || pipes[1].In.Fd != 1 || pipes[1].Out.Index != 0 || pipes[1].Out.Fd != 0 {
		t.Errorf("pipe 1 mapping mismatch: %+v", pipes[1])
	}

	// 3. Test runTestsBatched with interactive interactor
	tests := []testData{
		{
			Input:              "10",
			ExpectedOutput:     "42",
			InteractorCode:     "code",
			InteractorLanguage: "cpp",
		},
	}
	results, _, _, passed, overallVerdict, err := w.runTestsBatched(ctx, "job-interact", lang, "print(42)", tests, "", sandbox.RunLimits{}, nil, nil)
	if err != nil {
		t.Fatalf("runTestsBatched failed: %v", err)
	}
	if passed != 1 || overallVerdict != model.VerdictAccepted {
		t.Errorf("expected passed=1 and Accepted, got passed=%d, verdict=%v", passed, overallVerdict)
	}
	if len(results) != 1 || results[0].Verdict != model.VerdictAccepted {
		t.Errorf("unexpected test results: %+v", results)
	}
}
