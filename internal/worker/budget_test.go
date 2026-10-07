package worker

import (
	"testing"
	"time"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/sandbox"
)

func TestJobBudgetCoversEveryTestAtItsClockLimit(t *testing.T) {
	lang := sandbox.Languages["java"]
	limits := sandbox.RunLimits{CPU: 6_000_000_000}
	got := jobBudget(lang, limits, 20)
	// 20 tests x (2*6s+1s) + compile clock (2*15s+1s) + 15s slack
	want := 20*13*time.Second + 31*time.Second + 15*time.Second
	if got != want {
		t.Fatalf("budget %v, want %v", got, want)
	}
}
