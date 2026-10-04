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
)

type Status string

const (
	StatusQueued    Status = "queued"
	StatusCompiling Status = "compiling"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

type Mode string

const (
	ModeRun    Mode = "run"
	ModeSubmit Mode = "submit"
)

// DefaultTenantID is the seeded default tenant UUID from migration 002.
var DefaultTenantID = uuid.MustParse("00000000-0000-0000-0000-000000000001")

type Submission struct {
	ID               uuid.UUID
	TenantID         uuid.UUID
	UserID           *uuid.UUID
	ProblemVersionID *uuid.UUID
	IdempotencyKey   *string
	Mode             string
	Language         string
	SourceCode       string
	Status           Status
	Verdict          Verdict
	CompileStderr    string
	CPUTimeNs        int64
	MemoryBytes      int64
	TestCount        int
	TestsPassed      int
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type TestCase struct {
	Index          int
	Input          string
	ExpectedOutput string
	IsSample       bool
}

type TestResult struct {
	TestIndex     int
	Verdict       Verdict
	CPUTimeNs     int64
	MemoryBytes   int64
	StdoutPreview string
	StderrPreview string
}
