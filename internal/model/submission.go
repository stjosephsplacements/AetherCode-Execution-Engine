package model

import (
	"time"

	"github.com/google/uuid"
)

type Verdict string

const (
	VerdictAccepted         Verdict = "accepted"
	VerdictWrongAnswer      Verdict = "wrong_answer"
	VerdictTimeLimitEx      Verdict = "time_limit"
	VerdictMemoryLimitEx    Verdict = "memory_limit"
	VerdictRuntimeError     Verdict = "runtime_error"
	VerdictCompilationError Verdict = "compilation_error"
	VerdictInternalError    Verdict = "internal_error"
	VerdictOutputLimitEx    Verdict = "output_limit"
	VerdictCancelled        Verdict = "cancelled"
)

type Status string

const (
	StatusQueued    Status = "queued"
	StatusCompiling Status = "compiling"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

// DefaultTenantID is the seeded default tenant UUID from migration 002.
var DefaultTenantID = uuid.MustParse("00000000-0000-0000-0000-000000000001")

type Submission struct {
	ID             uuid.UUID
	TenantID       uuid.UUID
	UserID         *uuid.UUID
	IdempotencyKey *string
	Language       string
	SourceCode     string
	Status         Status
	Verdict        Verdict
	CompileStderr  string
	CPUTimeNs      int64
	MemoryBytes    int64
	TestCount      int
	TestsPassed    int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type TestResult struct {
	TestIndex     int
	Verdict       Verdict
	CPUTimeNs     int64
	MemoryBytes   int64
	RunTimeNs     int64
	StdoutPreview string
	StderrPreview string
}

// Output checker names accepted in execute requests.
const (
	CheckerExact = "exact"
	CheckerToken = "token"
	CheckerFloat = "float"
)

// Priority tier names for multi-tiered job streams.
const (
	PriorityInteractive = "run"    // P0: interactive code runner (default)
	PriorityContest     = "submit" // P1: contest & assessment submission
	PriorityBulk        = "bulk"   // P2: bulk batch re-judging & backfills
)

// ValidPriority reports whether s is a valid priority tier. The empty string is valid and defaults to PriorityInteractive ("run").
func ValidPriority(s string) bool {
	switch s {
	case "", PriorityInteractive, PriorityContest, PriorityBulk:
		return true
	}
	return false
}

// ValidChecker reports whether s names a supported checker. The empty string
// is valid and means exact. Custom checkers can be specified as "custom" or when checker code is provided.
func ValidChecker(s string) bool {
	switch s {
	case "", CheckerExact, CheckerToken, CheckerFloat, "custom":
		return true
	}
	return false
}
