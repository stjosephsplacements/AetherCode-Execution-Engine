package worker

import (
	"strings"
	"testing"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/sandbox"
)

func TestVerdictResults_AlwaysIncludesFullOutput(t *testing.T) {
	out := strings.Repeat("o", 300)
	got := verdictResults([]model.TestResult{{
		TestIndex:     0,
		Verdict:       model.VerdictAccepted,
		StdoutPreview: out,
		StderrPreview: "warning",
	}})
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	e := got[0]
	if s, _ := e["stdout"].(string); len(s) != 300 {
		t.Errorf("stdout length = %d, want 300", len(s))
	}
	if s, _ := e["stdout_preview"].(string); len(s) != 256 {
		t.Errorf("stdout_preview length = %d, want 256", len(s))
	}
	if s, _ := e["stderr"].(string); s != "warning" {
		t.Errorf("stderr = %q, want warning", s)
	}
	if s, _ := e["stderr_preview"].(string); s != "warning" {
		t.Errorf("stderr_preview = %q, want warning", s)
	}
}

func TestJudgeResult_ReportsExitStatusAndSandboxMessage(t *testing.T) {
	r := sandbox.Result{Status: sandbox.StatusSignalled, ExitStatus: 11, Error: "signal: segmentation fault", Time: 5_000_000, Memory: 4096, RunTime: 9_000_000, Files: map[string]string{"stdout": "", "stderr": "boom"}}
	tr := judgeResult(r, 3, "", "exact")
	if tr.Verdict != model.VerdictRuntimeError || tr.ExitStatus != 11 || tr.SandboxStatus != sandbox.StatusSignalled || tr.Message != "signal: segmentation fault" {
		t.Fatalf("result = %+v, want a runtime error carrying the exit status, sandbox status and message", tr)
	}
	e := verdictResults([]model.TestResult{tr})[0]
	if e["exit_status"] != 11 || e["sandbox_status"] != sandbox.StatusSignalled || e["message"] != "signal: segmentation fault" || e["run_time_ns"] != int64(9_000_000) {
		t.Fatalf("event entry = %+v, want exit_status, sandbox_status, message and run_time_ns", e)
	}
}

func TestVerdictResults_TruncatesTheSandboxMessage(t *testing.T) {
	e := verdictResults([]model.TestResult{{Message: strings.Repeat("m", 2000)}})[0]
	if s, _ := e["message"].(string); len(s) != 512 {
		t.Fatalf("message length = %d, want 512", len(s))
	}
}
