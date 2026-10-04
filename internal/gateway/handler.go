package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
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

type Handler struct {
	pool                   *pgxpool.Pool
	submitStream           *queue.Stream
	runStream              *queue.Stream
	pubsub                 *queue.PubSub
	judge                  *sandbox.Client
	rdb                    *redis.Client
	rateLimiter            *ratelimit.Limiter
	admissionMaxQueueDepth int
	trustProxy             bool
	ingestSem              chan struct{}
}

func NewHandler(pool *pgxpool.Pool, submitStream, runStream *queue.Stream, pubsub *queue.PubSub, judge *sandbox.Client, rdb *redis.Client, rateLimiter *ratelimit.Limiter, admissionMaxQueueDepth int, trustProxy bool) *Handler {
	return &Handler{
		pool:                   pool,
		submitStream:           submitStream,
		runStream:              runStream,
		pubsub:                 pubsub,
		judge:                  judge,
		rdb:                    rdb,
		rateLimiter:            rateLimiter,
		admissionMaxQueueDepth: admissionMaxQueueDepth,
		trustProxy:             trustProxy,
		ingestSem:              make(chan struct{}, 64),
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/execute", h.HandleExecute)
	mux.HandleFunc("GET /api/v1/stream", h.HandleStream)
	mux.HandleFunc("GET /api/v1/health", h.HandleHealth)
}

type executeRequest struct {
	Language         string      `json:"language"`
	SourceCode       string      `json:"source_code"`
	Mode             string      `json:"mode"`
	ProblemVersionID *string     `json:"problem_version_id"`
	IdempotencyKey   *string     `json:"idempotency_key"`
	Tests            []testInput `json:"tests"`
}

type testInput struct {
	Input          string `json:"input"`
	ExpectedOutput string `json:"expected_output"`
}

type executeResponse struct {
	JobID string `json:"job_id"`
}

const maxSourceSize = 64 * 1024 // 64KB
const maxTests = 100
const maxTestDataSize = 64 * 1024 // 64KB per test input/output

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

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1MB total body limit

	var req executeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Mode == "" {
		req.Mode = string(model.ModeRun)
	}
	if req.Mode != string(model.ModeRun) && req.Mode != string(model.ModeSubmit) {
		jsonError(w, "mode must be 'run' or 'submit'", http.StatusBadRequest)
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

	// Mode-specific validation
	if req.Mode == string(model.ModeSubmit) {
		if !hasAuth {
			jsonError(w, "authentication required for submit mode", http.StatusUnauthorized)
			return
		}
		if req.ProblemVersionID == nil || *req.ProblemVersionID == "" {
			jsonError(w, "problem_version_id is required for submit mode", http.StatusBadRequest)
			return
		}
		pvID, err := uuid.Parse(*req.ProblemVersionID)
		if err != nil {
			jsonError(w, "invalid problem_version_id", http.StatusBadRequest)
			return
		}
		pv, err := db.GetProblemVersion(ctx, h.pool, pvID)
		if err != nil {
			jsonError(w, "problem version not found", http.StatusNotFound)
			return
		}
		if pv.TenantID != identity.TenantID {
			jsonError(w, "problem version not found", http.StatusNotFound)
			return
		}
	} else {
		// Run mode: tests from client
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
				jsonError(w, "test input/output exceeds 64KB", http.StatusBadRequest)
				return
			}
		}
	}

	// Rate limiting
	var rlResult ratelimit.Result
	if hasAuth {
		rlResult = h.rateLimiter.Check(ctx, identity.UserID)
	} else {
		rlResult = h.rateLimiter.CheckIP(ctx, ClientIP(r, h.trustProxy))
	}
	if !rlResult.Allowed {
		if hasAuth {
			metrics.RateLimitRejections.WithLabelValues("user").Inc()
		} else {
			metrics.RateLimitRejections.WithLabelValues("ip").Inc()
		}
		retryAfter := int(math.Ceil(rlResult.RetryAfter.Seconds()))
		w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfter))
		jsonError(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	// Pick the target stream based on mode
	targetStream := h.runStream
	if req.Mode == string(model.ModeSubmit) {
		targetStream = h.submitStream
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
		Mode:       req.Mode,
		Language:   req.Language,
		SourceCode: req.SourceCode,
		Status:     model.StatusQueued,
	}

	if hasAuth {
		sub.TenantID = identity.TenantID
		sub.UserID = &identity.UserID
	}
	if req.ProblemVersionID != nil {
		pvID, _ := uuid.Parse(*req.ProblemVersionID)
		sub.ProblemVersionID = &pvID
	}
	if req.IdempotencyKey != nil && *req.IdempotencyKey != "" {
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
	var tests []queue.TestData
	if req.Mode == string(model.ModeRun) {
		tests = make([]queue.TestData, len(req.Tests))
		for i, t := range req.Tests {
			tests[i] = queue.TestData{Input: t.Input, ExpectedOutput: t.ExpectedOutput}
		}
	}

	queuedEvt := eventlog.New(jobID.String(), eventlog.EventQueued, nil)
	if err := h.pubsub.PrepareAndEnqueue(ctx, jobID.String(), tests, queuedEvt, targetStream.StreamName()); err != nil {
		slog.Error("enqueue failed", "submission_id", jobID, "err", err)
		jsonError(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(executeResponse{JobID: jobID.String()}) //nolint:errcheck // best-effort: write the submission response
}

func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	status := map[string]string{}
	healthy := true

	if err := h.pool.Ping(ctx); err != nil {
		slog.Error("health: database check failed", "err", err)
		status["database"] = "error"
		healthy = false
	} else {
		status["database"] = "ok"
	}

	if err := h.judge.Ping(ctx); err != nil {
		slog.Error("health: sandbox check failed", "err", err)
		status["sandbox"] = "error"
		healthy = false
	} else {
		status["sandbox"] = "ok"
	}

	if err := h.rdb.Ping(ctx).Err(); err != nil {
		slog.Error("health: redis check failed", "err", err)
		status["redis"] = "error"
		healthy = false
	} else {
		status["redis"] = "ok"
	}

	w.Header().Set("Content-Type", "application/json")
	if !healthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(status) //nolint:errcheck // best-effort: write the health status
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg}) //nolint:errcheck // best-effort: write the error body
}
