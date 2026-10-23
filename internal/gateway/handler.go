package gateway

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/auth"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/db"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/eventlog"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/metrics"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/queue"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/ratelimit"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/sandbox"
)

const streamTokenTTL = 30 * time.Minute // matches sseMaxLifetime with margin
const streamTokenKeyPrefix = "ac:stoken:"

// newStreamToken generates a 20-byte cryptographically random token and stores
// it in Redis keyed to the job ID. Unauthenticated callers must present it to
// open the SSE stream, preventing UUIDv7-guessing attacks.
func newStreamToken(ctx context.Context, rdb *redis.Client, jobID string) (string, error) {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("stream token: generate: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(b)
	key := streamTokenKeyPrefix + jobID
	if err := rdb.Set(ctx, key, token, streamTokenTTL).Err(); err != nil {
		return "", fmt.Errorf("stream token: store: %w", err)
	}
	return token, nil
}

// verifyStreamToken checks that token matches the stored token for jobID.
// Returns true on match; false on mismatch, missing key, or Redis error (fail closed).
func verifyStreamToken(ctx context.Context, rdb *redis.Client, jobID, token string) bool {
	if token == "" {
		return false
	}
	stored, err := rdb.Get(ctx, streamTokenKeyPrefix+jobID).Result()
	if err != nil {
		return false
	}
	return stored == token
}

// Version is set at build time via ldflags.
var Version = "dev"

type Handler struct {
	pool                   *pgxpool.Pool
	submitStream           *queue.Stream
	runStream              *queue.Stream
	bulkStream             *queue.Stream
	pubsub                 *queue.PubSub
	judge                  *sandbox.Client
	rdb                    *redis.Client
	rateLimiter            *ratelimit.Limiter
	unauthLimiter          *ratelimit.Limiter // nil when unauth rate limiting is disabled
	admissionMaxQueueDepth int
	ingestSem              chan struct{}
	startTime              time.Time
	workerCount            int
}

func NewHandler(pool *pgxpool.Pool, submitStream, runStream *queue.Stream, pubsub *queue.PubSub, judge *sandbox.Client, rdb *redis.Client, rateLimiter *ratelimit.Limiter, unauthLimiter *ratelimit.Limiter, admissionMaxQueueDepth int, workerCount int) *Handler {
	return NewTieredHandler(pool, submitStream, runStream, nil, pubsub, judge, rdb, rateLimiter, unauthLimiter, admissionMaxQueueDepth, workerCount)
}

// NewTieredHandler creates a Handler with full 3-tier priority stream support.
func NewTieredHandler(pool *pgxpool.Pool, submitStream, runStream, bulkStream *queue.Stream, pubsub *queue.PubSub, judge *sandbox.Client, rdb *redis.Client, rateLimiter *ratelimit.Limiter, unauthLimiter *ratelimit.Limiter, admissionMaxQueueDepth int, workerCount int) *Handler {
	return &Handler{
		pool:                   pool,
		submitStream:           submitStream,
		runStream:              runStream,
		bulkStream:             bulkStream,
		pubsub:                 pubsub,
		judge:                  judge,
		rdb:                    rdb,
		rateLimiter:            rateLimiter,
		unauthLimiter:          unauthLimiter,
		admissionMaxQueueDepth: admissionMaxQueueDepth,
		ingestSem:              make(chan struct{}, 512),
		startTime:              time.Now(),
		workerCount:            workerCount,
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/execute", h.HandleExecute)
	mux.HandleFunc("GET /api/v1/stream", h.HandleStream)
	mux.HandleFunc("GET /api/v1/health", h.HandleHealth)
	mux.HandleFunc("GET /api/v1/languages", h.HandleLanguages)
	mux.HandleFunc("GET /api/v1/submissions/{id}", h.HandleGetSubmission)
	mux.HandleFunc("DELETE /api/v1/submissions/{id}", h.HandleCancel)
}

type executeRequest struct {
	Language       string      `json:"language"`
	SourceCode     string      `json:"source_code"`
	IdempotencyKey *string     `json:"idempotency_key"`
	Tests          []testInput `json:"tests"`
	// Optional limits; omitted means the language defaults.
	TimeLimitMs   *uint64 `json:"time_limit_ms"`
	MemoryLimitKb *uint64 `json:"memory_limit_kb"`
	// Optional output checker: exact (default), token, float, or custom.
	Checker string `json:"checker"`
	// Optional custom checker source code and language (e.g. "python", "cpp").
	CheckerCode     string `json:"checker_code,omitempty"`
	CheckerLanguage string `json:"checker_language,omitempty"`
	// Optional interactive interactor source code and language for interactive problems.
	InteractorCode     string `json:"interactor_code,omitempty"`
	InteractorLanguage string `json:"interactor_language,omitempty"`
	// Optional multi-file submission map: {"helper.h": "...", "solution.cpp": "..."}
	Files map[string]string `json:"files,omitempty"`
	// Optional whitelisted compiler flags: ["-O3", "-std=c++20"]
	CompilerFlags []string `json:"compiler_flags,omitempty"`
	// Optional priority tier: "run" (default / interactive), "submit" (contest / assessment), or "bulk" (batch re-judge).
	Priority string `json:"priority"`

	// Mode and ProblemVersionID exist only to reject legacy submit-mode requests.
	Mode             string  `json:"mode"`
	ProblemVersionID *string `json:"problem_version_id"`
}

type testInput struct {
	Input          string `json:"input"`
	ExpectedOutput string `json:"expected_output"`
}

type executeResponse struct {
	JobID       string `json:"job_id"`
	StreamToken string `json:"stream_token,omitempty"` // unauthenticated callers must present this to stream
}

const maxSourceSize = 64 * 1024 // 64KB
const maxTests = 100
const maxTestDataSize = 8 << 20  // 8MB per test input/output
const maxBodySize = 64 << 20     // whole request
const maxIdempotencyKeyLen = 255 // bytes; matches typical DB index page size

func (h *Handler) HandleExecute(w http.ResponseWriter, r *http.Request) {
	ct := r.Header.Get("Content-Type")
	if ct == "" || !strings.HasPrefix(ct, "application/json") {
		jsonError(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	// Limit concurrent handler goroutines to prevent PG pool exhaustion
	select {
	case h.ingestSem <- struct{}{}:
		defer func() { <-h.ingestSem }()
	default:
		w.Header().Set("Retry-After", "1")
		jsonError(w, "server is busy, try again later", http.StatusTooManyRequests)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)

	var req executeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if (req.Mode != "" && req.Mode != "run") || (req.ProblemVersionID != nil && *req.ProblemVersionID != "") {
		jsonError(w, "mode=submit and problem_version_id are no longer supported; send tests in the request body", http.StatusBadRequest)
		return
	}

	if _, ok := sandbox.Languages[req.Language]; !ok {
		jsonError(w, "unsupported language", http.StatusBadRequest)
		return
	}
	if len(req.SourceCode) == 0 {
		jsonError(w, "source_code is required", http.StatusBadRequest)
		return
	}
	if len(req.SourceCode) > maxSourceSize {
		jsonError(w, "source_code exceeds 64KB", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	identity, hasAuth := auth.GetIdentity(ctx)

	if len(req.Tests) == 0 {
		jsonError(w, "at least one test is required", http.StatusBadRequest)
		return
	}
	if len(req.Tests) > maxTests {
		jsonError(w, "too many tests (max 100)", http.StatusBadRequest)
		return
	}
	for _, t := range req.Tests {
		if len(t.Input) > maxTestDataSize || len(t.ExpectedOutput) > maxTestDataSize {
			jsonError(w, "test input/output exceeds 8MB", http.StatusBadRequest)
			return
		}
	}
	if !model.ValidChecker(req.Checker) {
		jsonError(w, "checker must be one of: exact, token, float, custom", http.StatusBadRequest)
		return
	}
	if req.Checker == "custom" || req.CheckerCode != "" {
		if req.CheckerCode == "" {
			jsonError(w, "checker_code is required when checker is custom", http.StatusBadRequest)
			return
		}
		if len(req.CheckerCode) > maxSourceSize {
			jsonError(w, "checker_code exceeds 64KB", http.StatusBadRequest)
			return
		}
		if req.CheckerLanguage == "" {
			req.CheckerLanguage = "python" // default checker language
		}
		if _, ok := sandbox.Languages[req.CheckerLanguage]; !ok {
			jsonError(w, "unsupported checker_language", http.StatusBadRequest)
			return
		}
		req.Checker = "custom"
	}
	if req.InteractorCode != "" {
		if len(req.InteractorCode) > maxSourceSize {
			jsonError(w, "interactor_code exceeds 64KB", http.StatusBadRequest)
			return
		}
		if req.InteractorLanguage == "" {
			req.InteractorLanguage = "python"
		}
		if _, ok := sandbox.Languages[req.InteractorLanguage]; !ok {
			jsonError(w, "unsupported interactor_language", http.StatusBadRequest)
			return
		}
	}
	if len(req.Files) > 0 {
		if err := sandbox.ValidateFiles(req.Files, 10, 128<<10); err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	if len(req.CompilerFlags) > 0 {
		if err := sandbox.ValidateCompilerFlags(req.Language, req.CompilerFlags); err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	if !model.ValidPriority(req.Priority) {
		jsonError(w, "priority must be one of: run, submit, bulk", http.StatusBadRequest)
		return
	}

	var limits sandbox.RunLimits
	if req.TimeLimitMs != nil {
		limits.CPU = *req.TimeLimitMs * 1_000_000
		if limits.CPU < sandbox.MinRunCPU || limits.CPU > sandbox.MaxRunCPU {
			jsonError(w, "time_limit_ms must be between 100 and 20000", http.StatusBadRequest)
			return
		}
	}
	if req.MemoryLimitKb != nil {
		limits.Memory = *req.MemoryLimitKb * 1024
		if limits.Memory < sandbox.MinRunMem || limits.Memory > sandbox.MaxRunMem {
			jsonError(w, "memory_limit_kb must be between 16384 and 2097152", http.StatusBadRequest)
			return
		}
	}

	// Per-user rate limiting for authenticated callers.
	if hasAuth {
		if rlResult := h.rateLimiter.Check(ctx, identity.UserID); !rlResult.Allowed {
			metrics.RateLimitRejections.WithLabelValues("user").Inc()
			retryAfter := int(math.Ceil(rlResult.RetryAfter.Seconds()))
			w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfter))
			jsonError(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
	}

	// Per-IP rate limiting for unauthenticated callers (when enabled via AC_RATE_LIMIT_UNAUTH_PER_MINUTE).
	// Disabled by default (perMinute=0) to preserve the existing trusted-backend behaviour.
	if !hasAuth && h.unauthLimiter != nil {
		if rlResult := h.unauthLimiter.CheckIP(ctx, remoteIP(r)); !rlResult.Allowed {
			metrics.RateLimitRejections.WithLabelValues("ip").Inc()
			retryAfter := int(math.Ceil(rlResult.RetryAfter.Seconds()))
			w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfter))
			jsonError(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
	}

	targetStream := h.runStream
	switch req.Priority {
	case model.PriorityContest:
		if h.submitStream != nil {
			targetStream = h.submitStream
		}
	case model.PriorityBulk:
		if h.bulkStream != nil {
			targetStream = h.bulkStream
		}
	}

	// Admission control — reject if queue is too deep
	admitted, err := ratelimit.AdmissionCheck(ctx, h.rdb, targetStream.StreamName(), h.admissionMaxQueueDepth)
	if err != nil {
		slog.Error("admission check failed, allowing", "err", err)
	} else if !admitted {
		w.Header().Set("Retry-After", "10")
		jsonError(w, "server is busy, try again later", http.StatusTooManyRequests)
		return
	}

	id, err := uuid.NewV7()
	if err != nil {
		slog.Error("uuid generation failed", "err", err)
		jsonError(w, "internal error", http.StatusInternalServerError)
		return
	}

	sub := &model.Submission{
		ID:         id,
		TenantID:   model.DefaultTenantID,
		Language:   req.Language,
		SourceCode: req.SourceCode,
		Status:     model.StatusQueued,
	}

	if hasAuth {
		sub.TenantID = identity.TenantID
		sub.UserID = &identity.UserID
	}
	if req.IdempotencyKey != nil && *req.IdempotencyKey != "" {
		if len(*req.IdempotencyKey) > maxIdempotencyKeyLen {
			jsonError(w, "idempotency_key exceeds 255 bytes", http.StatusBadRequest)
			return
		}
		sub.IdempotencyKey = req.IdempotencyKey
	}

	// Idempotency check
	var jobID uuid.UUID
	if sub.IdempotencyKey != nil && sub.UserID != nil {
		existingID, isNew, err := db.InsertSubmissionIdempotent(ctx, h.pool, sub)
		if err != nil {
			slog.Error("idempotent insert failed", "err", err)
			jsonError(w, "internal error", http.StatusInternalServerError)
			return
		}
		if !isNew {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(executeResponse{JobID: existingID.String()}) //nolint:errcheck // best-effort: write the idempotent response
			return
		}
		jobID = existingID
	} else {
		if err := db.InsertSubmission(ctx, h.pool, sub); err != nil {
			slog.Error("insert submission failed", "err", err)
			jsonError(w, "internal error", http.StatusInternalServerError)
			return
		}
		jobID = id
	}

	// Store tests + publish QUEUED event + enqueue in a single Redis pipeline
	tests := make([]queue.TestData, len(req.Tests))
	for i, t := range req.Tests {
		tests[i] = queue.TestData{Input: t.Input, ExpectedOutput: t.ExpectedOutput}
	}

	queuedEvt := eventlog.New(jobID.String(), eventlog.EventQueued, nil)
	testSet := queue.TestSet{
		Tests:              tests,
		Limits:             limits,
		Checker:            req.Checker,
		CheckerCode:        req.CheckerCode,
		CheckerLanguage:    req.CheckerLanguage,
		InteractorCode:     req.InteractorCode,
		InteractorLanguage: req.InteractorLanguage,
		Files:              req.Files,
		CompilerFlags:      req.CompilerFlags,
	}
	if err := h.pubsub.PrepareAndEnqueue(ctx, jobID.String(), testSet, queuedEvt, targetStream.StreamName()); err != nil {
		slog.Error("enqueue failed", "submission_id", jobID, "err", err)
		jsonError(w, "internal error", http.StatusInternalServerError)
		return
	}

	resp := executeResponse{JobID: jobID.String()}

	// Issue a stream token for every new job. Unauthenticated callers need it to
	// open the SSE stream or cancel the job; authenticated callers may use their
	// JWT instead.
	tok, err := newStreamToken(ctx, h.rdb, jobID.String())
	if err != nil {
		slog.Warn("failed to generate stream token", "job_id", jobID, "err", err)
		// Non-fatal: the client will need auth to stream, but the job is queued.
	} else {
		resp.StreamToken = tok
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp) //nolint:errcheck // best-effort: write the submission response
}

type healthResponse struct {
	Status         string            `json:"status"`
	Version        string            `json:"version"`
	GoVersion      string            `json:"go_version"`
	UptimeSeconds  int               `json:"uptime_seconds"`
	Checks         map[string]string `json:"checks"`
	SandboxVersion string            `json:"sandbox_version,omitempty"`
	Workers        workerInfo        `json:"workers"`
	Queues         map[string]int64  `json:"queues"`
}

type workerInfo struct {
	Active     int `json:"active"`
	Configured int `json:"configured"`
}

func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	checks := map[string]string{}
	dbOK, sandboxOK, redisOK := true, true, true

	if err := h.pool.Ping(ctx); err != nil {
		slog.Error("health: database check failed", "err", err)
		checks["database"] = "error"
		dbOK = false
	} else {
		checks["database"] = "ok"
	}

	var sandboxVersion string
	if ver, err := h.judge.Version(ctx); err != nil {
		slog.Error("health: sandbox check failed", "err", err)
		checks["sandbox"] = "error"
		sandboxOK = false
	} else {
		checks["sandbox"] = "ok"
		sandboxVersion = ver
	}

	if err := h.rdb.Ping(ctx).Err(); err != nil {
		slog.Error("health: redis check failed", "err", err)
		checks["redis"] = "error"
		redisOK = false
	} else {
		checks["redis"] = "ok"
	}

	queues := map[string]int64{}
	streamsToCheck := []*queue.Stream{h.submitStream, h.runStream}
	if h.bulkStream != nil {
		streamsToCheck = append(streamsToCheck, h.bulkStream)
	}
	for _, s := range streamsToCheck {
		if s == nil {
			continue
		}
		name := s.StreamName()
		length, err := h.rdb.XLen(ctx, name).Result()
		if err == nil {
			queues[name] = length
		}
	}

	overall := "healthy"
	httpStatus := http.StatusOK
	if !dbOK {
		overall = "unhealthy"
		httpStatus = http.StatusServiceUnavailable
	} else if !sandboxOK || !redisOK {
		overall = "degraded"
	}

	var activeWorkers int
	var m dto.Metric
	if err := metrics.ActiveWorkers.Write(&m); err == nil {
		activeWorkers = int(m.GetGauge().GetValue())
	}

	resp := healthResponse{
		Status:         overall,
		Version:        Version,
		GoVersion:      runtime.Version(),
		UptimeSeconds:  int(time.Since(h.startTime).Seconds()),
		Checks:         checks,
		SandboxVersion: sandboxVersion,
		Workers: workerInfo{
			Active:     activeWorkers,
			Configured: h.workerCount,
		},
		Queues: queues,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	json.NewEncoder(w).Encode(resp) //nolint:errcheck // best-effort: write the health status
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg}) //nolint:errcheck // best-effort: write the error body
}

type languageInfo struct {
	ID                   string `json:"id"`
	Compiled             bool   `json:"compiled"`
	DefaultCPULimitMs    uint64 `json:"default_cpu_limit_ms"`
	DefaultMemoryLimitKb uint64 `json:"default_memory_limit_kb"`
	RunProcLimit         uint64 `json:"run_proc_limit"`
}

func (h *Handler) HandleLanguages(w http.ResponseWriter, r *http.Request) {
	ids := make([]string, 0, len(sandbox.Languages))
	for id := range sandbox.Languages {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	langs := make([]languageInfo, len(ids))
	for i, id := range ids {
		l := sandbox.Languages[id]
		langs[i] = languageInfo{
			ID:                   id,
			Compiled:             l.Compiled,
			DefaultCPULimitMs:    l.RunCPU / 1_000_000,
			DefaultMemoryLimitKb: l.RunMem / 1024,
			RunProcLimit:         l.RunProc,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(langs) //nolint:errcheck
}

// HandleCancel cancels a queued submission. Only the submission owner (authenticated)
// or an unauthenticated caller presenting the job's stream_token may cancel.
func (h *Handler) HandleCancel(w http.ResponseWriter, r *http.Request) {
	rawID := r.PathValue("id")
	parsedID, err := uuid.Parse(rawID)
	if err != nil {
		jsonError(w, "invalid submission id", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	identity, hasAuth := auth.GetIdentity(ctx)

	sub, err := db.GetSubmission(ctx, h.pool, parsedID)
	if err != nil {
		jsonError(w, "not found", http.StatusNotFound)
		return
	}

	// Ownership check — same rules as the SSE stream endpoint.
	if hasAuth {
		if sub.TenantID != identity.TenantID || sub.UserID == nil || *sub.UserID != identity.UserID {
			jsonError(w, "not found", http.StatusNotFound)
			return
		}
	} else {
		tokenCtx, tokenCancel := context.WithTimeout(context.Background(), 2*time.Second)
		valid := verifyStreamToken(tokenCtx, h.rdb, parsedID.String(), r.URL.Query().Get("stream_token"))
		tokenCancel()
		if !valid {
			jsonError(w, "invalid or missing stream_token", http.StatusUnauthorized)
			return
		}
	}

	if err := db.CancelSubmission(ctx, h.pool, parsedID); err != nil {
		// Already terminal — return 409 Conflict
		jsonError(w, "submission is not cancellable (already terminal or not found)", http.StatusConflict)
		return
	}

	if h.pubsub != nil {
		verdictData := map[string]any{
			"verdict": model.VerdictCancelled,
		}
		pubCtx, pubCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		if err := h.pubsub.PublishEvent(pubCtx, eventlog.New(parsedID.String(), eventlog.EventVerdict, verdictData)); err != nil {
			slog.Warn("failed to publish VERDICT event for cancelled submission", "submission_id", parsedID, "err", err)
		}
		pubCancel()
	}
	metrics.JobsProcessedTotal.WithLabelValues(string(model.VerdictCancelled)).Inc()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]bool{"cancelled": true}) //nolint:errcheck
}

type getSubmissionResponse struct {
	JobID         string             `json:"job_id"`
	Status        string             `json:"status"`
	Verdict       string             `json:"verdict,omitempty"`
	CompileStderr string             `json:"compile_stderr,omitempty"`
	Language      string             `json:"language"`
	CPUTimeNs     int64              `json:"cpu_time_ns"`
	MemoryBytes   int64              `json:"memory_bytes"`
	TestCount     int                `json:"test_count"`
	TestsPassed   int                `json:"tests_passed"`
	CreatedAt     time.Time          `json:"created_at"`
	Results       []testResultOutput `json:"results,omitempty"`
}

type testResultOutput struct {
	TestIndex     int    `json:"test_index"`
	Verdict       string `json:"verdict"`
	CPUTimeNs     int64  `json:"cpu_time_ns"`
	MemoryBytes   int64  `json:"memory_bytes"`
	StdoutPreview string `json:"stdout_preview"`
	StderrPreview string `json:"stderr_preview"`
}

// HandleGetSubmission retrieves a submission's status and test results.
func (h *Handler) HandleGetSubmission(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	parsedID, err := uuid.Parse(idStr)
	if err != nil {
		jsonError(w, "invalid submission id", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	identity, hasAuth := auth.GetIdentity(ctx)

	sub, err := db.GetSubmission(ctx, h.pool, parsedID)
	if err != nil {
		jsonError(w, "not found", http.StatusNotFound)
		return
	}

	// Ownership check — same rules as SSE stream and cancellation.
	if hasAuth {
		if sub.TenantID != identity.TenantID || sub.UserID == nil || *sub.UserID != identity.UserID {
			jsonError(w, "not found", http.StatusNotFound)
			return
		}
	} else {
		tokenCtx, tokenCancel := context.WithTimeout(context.Background(), 2*time.Second)
		valid := verifyStreamToken(tokenCtx, h.rdb, parsedID.String(), r.URL.Query().Get("stream_token"))
		tokenCancel()
		if !valid {
			jsonError(w, "invalid or missing stream_token", http.StatusUnauthorized)
			return
		}
	}

	resp := getSubmissionResponse{
		JobID:         sub.ID.String(),
		Status:        string(sub.Status),
		Verdict:       string(sub.Verdict),
		CompileStderr: sub.CompileStderr,
		Language:      sub.Language,
		CPUTimeNs:     sub.CPUTimeNs,
		MemoryBytes:   sub.MemoryBytes,
		TestCount:     sub.TestCount,
		TestsPassed:   sub.TestsPassed,
		CreatedAt:     sub.CreatedAt,
	}

	testResults, err := db.GetTestResults(ctx, h.pool, parsedID)
	if err == nil && len(testResults) > 0 {
		resp.Results = make([]testResultOutput, len(testResults))
		for i, tr := range testResults {
			resp.Results[i] = testResultOutput{
				TestIndex:     tr.TestIndex,
				Verdict:       string(tr.Verdict),
				CPUTimeNs:     tr.CPUTimeNs,
				MemoryBytes:   tr.MemoryBytes,
				StdoutPreview: tr.StdoutPreview,
				StderrPreview: tr.StderrPreview,
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp) //nolint:errcheck
}
