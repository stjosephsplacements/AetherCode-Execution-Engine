package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/db"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/eventlog"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/queue"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/sandbox"
)

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *goredis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

func newUnreachablePGPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig("postgres://test:test@127.0.0.1:1/test")
	if err != nil {
		t.Fatalf("parse pgxpool config: %v", err)
	}
	cfg.MinConns = 0
	cfg.MaxConns = 1
	cfg.ConnConfig.ConnectTimeout = 100 * time.Millisecond
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("create pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func fakeJudgeServer(t *testing.T, version string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/version" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"buildVersion":%q}`, version)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestHandleHealth_DegradedWhenDatabaseDown(t *testing.T) {
	judgeSrv := fakeJudgeServer(t, "1.13.0")
	judge := sandbox.NewClient(judgeSrv.URL, "test-token")
	_, rdb := newTestRedis(t)
	pool := newUnreachablePGPool(t)

	submitStream := queue.NewStream(rdb, "ac:submissions:submit", "workers")
	runStream := queue.NewStream(rdb, "ac:submissions:run", "workers")

	h := NewHandler(pool, submitStream, runStream, nil, judge, rdb, nil, nil, 5000, 8, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	h.HandleHealth(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}

	var resp healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if resp.Status != "unhealthy" {
		t.Errorf("status = %q, want %q", resp.Status, "unhealthy")
	}
	if resp.Checks["database"] != "error" {
		t.Errorf("checks.database = %q, want %q", resp.Checks["database"], "error")
	}
	if resp.Checks["sandbox"] != "ok" {
		t.Errorf("checks.sandbox = %q, want %q", resp.Checks["sandbox"], "ok")
	}
	if resp.Checks["redis"] != "ok" {
		t.Errorf("checks.redis = %q, want %q", resp.Checks["redis"], "ok")
	}
	if resp.SandboxVersion != "1.13.0" {
		t.Errorf("sandbox_version = %q, want %q", resp.SandboxVersion, "1.13.0")
	}
}

func TestHandleHealth_ResponseShape(t *testing.T) {
	judgeSrv := fakeJudgeServer(t, "1.13.0")
	judge := sandbox.NewClient(judgeSrv.URL, "test-token")
	_, rdb := newTestRedis(t)
	pool := newUnreachablePGPool(t)

	submitStream := queue.NewStream(rdb, "ac:submissions:submit", "workers")
	runStream := queue.NewStream(rdb, "ac:submissions:run", "workers")

	h := NewHandler(pool, submitStream, runStream, nil, judge, rdb, nil, nil, 5000, 4, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	h.HandleHealth(rec, req)

	var resp healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if resp.GoVersion != runtime.Version() {
		t.Errorf("go_version = %q, want %q", resp.GoVersion, runtime.Version())
	}
	if resp.UptimeSeconds < 0 {
		t.Errorf("uptime_seconds = %d, want >= 0", resp.UptimeSeconds)
	}
	if resp.Workers.Configured != 4 {
		t.Errorf("workers.configured = %d, want 4", resp.Workers.Configured)
	}
	if resp.Workers.Active != 0 {
		t.Errorf("workers.active = %d, want 0", resp.Workers.Active)
	}
	if resp.Checks == nil {
		t.Fatal("checks is nil")
	}
}

func TestHandleHealth_SandboxDown(t *testing.T) {
	judge := sandbox.NewClient("http://127.0.0.1:1", "test-token")
	_, rdb := newTestRedis(t)
	pool := newUnreachablePGPool(t)

	submitStream := queue.NewStream(rdb, "ac:submissions:submit", "workers")
	runStream := queue.NewStream(rdb, "ac:submissions:run", "workers")

	h := NewHandler(pool, submitStream, runStream, nil, judge, rdb, nil, nil, 5000, 8, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	h.HandleHealth(rec, req)

	var resp healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if resp.Checks["sandbox"] != "error" {
		t.Errorf("checks.sandbox = %q, want %q", resp.Checks["sandbox"], "error")
	}
	if resp.SandboxVersion != "" {
		t.Errorf("sandbox_version = %q, want empty", resp.SandboxVersion)
	}
}

func TestHandleHealth_QueueDepths(t *testing.T) {
	judgeSrv := fakeJudgeServer(t, "1.13.0")
	judge := sandbox.NewClient(judgeSrv.URL, "test-token")
	_, rdb := newTestRedis(t)
	pool := newUnreachablePGPool(t)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		rdb.XAdd(ctx, &goredis.XAddArgs{
			Stream: "ac:submissions:submit",
			Values: map[string]any{"id": i},
		})
	}
	for i := 0; i < 3; i++ {
		rdb.XAdd(ctx, &goredis.XAddArgs{
			Stream: "ac:submissions:run",
			Values: map[string]any{"id": i},
		})
	}

	submitStream := queue.NewStream(rdb, "ac:submissions:submit", "workers")
	runStream := queue.NewStream(rdb, "ac:submissions:run", "workers")

	h := NewHandler(pool, submitStream, runStream, nil, judge, rdb, nil, nil, 5000, 8, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	h.HandleHealth(rec, req)

	var resp healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if resp.Queues["ac:submissions:submit"] != 5 {
		t.Errorf("submit queue depth = %d, want 5", resp.Queues["ac:submissions:submit"])
	}
	if resp.Queues["ac:submissions:run"] != 3 {
		t.Errorf("run queue depth = %d, want 3", resp.Queues["ac:submissions:run"])
	}
}

func TestHandleLanguages(t *testing.T) {
	judgeSrv := fakeJudgeServer(t, "1.13.0")
	judge := sandbox.NewClient(judgeSrv.URL, "test-token")
	_, rdb := newTestRedis(t)
	pool := newUnreachablePGPool(t)

	submitStream := queue.NewStream(rdb, "ac:submissions:submit", "workers")
	runStream := queue.NewStream(rdb, "ac:submissions:run", "workers")
	h := NewHandler(pool, submitStream, runStream, nil, judge, rdb, nil, nil, 5000, 8, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/languages", nil)
	h.HandleLanguages(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var langs []languageInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &langs); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if len(langs) == 0 {
		t.Fatal("expected at least one language")
	}

	// All sandbox languages must be present
	present := make(map[string]bool, len(langs))
	for _, l := range langs {
		present[l.ID] = true
	}
	for id := range sandbox.Languages {
		if !present[id] {
			t.Errorf("language %q missing from response", id)
		}
	}

	// Verify sorted order
	for i := 1; i < len(langs); i++ {
		if langs[i].ID < langs[i-1].ID {
			t.Errorf("languages not sorted: %q before %q", langs[i-1].ID, langs[i].ID)
		}
	}

	// Spot-check python
	for _, l := range langs {
		if l.ID == "python" {
			if l.Compiled {
				t.Error("python: compiled should be false")
			}
			if l.DefaultCPULimitMs != 2000 {
				t.Errorf("python: default_cpu_limit_ms = %d, want 2000", l.DefaultCPULimitMs)
			}
			if l.DefaultMemoryLimitKb != 524288 {
				t.Errorf("python: default_memory_limit_kb = %d, want 524288", l.DefaultMemoryLimitKb)
			}
		}
		if l.ID == "cpp" {
			if !l.Compiled {
				t.Error("cpp: compiled should be true")
			}
		}
	}
}

func TestHandleCancel_NotFound(t *testing.T) {
	judgeSrv := fakeJudgeServer(t, "1.13.0")
	judge := sandbox.NewClient(judgeSrv.URL, "test-token")
	_, rdb := newTestRedis(t)
	pool := newUnreachablePGPool(t)

	submitStream := queue.NewStream(rdb, "ac:submissions:submit", "workers")
	runStream := queue.NewStream(rdb, "ac:submissions:run", "workers")
	h := NewHandler(pool, submitStream, runStream, nil, judge, rdb, nil, nil, 5000, 8, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/submissions/00000000-0000-0000-0000-000000000001", nil)
	req.SetPathValue("id", "00000000-0000-0000-0000-000000000001")
	h.HandleCancel(rec, req)

	// DB is unreachable — GetSubmission fails → 404
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestHandleCancel_InvalidID(t *testing.T) {
	judgeSrv := fakeJudgeServer(t, "1.13.0")
	judge := sandbox.NewClient(judgeSrv.URL, "test-token")
	_, rdb := newTestRedis(t)
	pool := newUnreachablePGPool(t)

	submitStream := queue.NewStream(rdb, "ac:submissions:submit", "workers")
	runStream := queue.NewStream(rdb, "ac:submissions:run", "workers")
	h := NewHandler(pool, submitStream, runStream, nil, judge, rdb, nil, nil, 5000, 8, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/submissions/not-a-uuid", nil)
	req.SetPathValue("id", "not-a-uuid")
	h.HandleCancel(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestHandleCancel_EmitsVerdictEvent(t *testing.T) {
	_, rdb := newTestRedis(t)
	pool := newFakePGPool(t)
	ps := queue.NewPubSub(rdb)

	submitStream := queue.NewStream(rdb, "ac:submissions:submit", "workers")
	runStream := queue.NewStream(rdb, "ac:submissions:run", "workers")
	h := NewHandler(pool, submitStream, runStream, ps, nil, rdb, nil, nil, 5000, 8, nil)

	ctx := context.Background()
	jobID := uuid.New()
	if err := db.InsertSubmission(ctx, pool, &model.Submission{
		ID:         jobID,
		TenantID:   model.DefaultTenantID,
		Language:   "python",
		SourceCode: "print(1)",
		Status:     model.StatusQueued,
		TestCount:  3,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	token, err := newStreamToken(ctx, rdb, jobID.String())
	if err != nil {
		t.Fatalf("token: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/submissions/"+jobID.String()+"?stream_token="+token, nil)
	req.SetPathValue("id", jobID.String())
	h.HandleCancel(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	events, err := ps.GetEventLog(ctx, jobID.String())
	if err != nil {
		t.Fatalf("get event log: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Type != eventlog.EventVerdict {
		t.Errorf("event type = %v, want VERDICT", events[0].Type)
	}
	data, ok := events[0].Data.(map[string]any)
	if !ok || data["verdict"] != string(model.VerdictCancelled) {
		t.Errorf("verdict = %v, want cancelled", data["verdict"])
	}
}

func TestHandleGetSubmission_InvalidID(t *testing.T) {
	judgeSrv := fakeJudgeServer(t, "1.13.0")
	judge := sandbox.NewClient(judgeSrv.URL, "test-token")
	_, rdb := newTestRedis(t)
	pool := newUnreachablePGPool(t)

	submitStream := queue.NewStream(rdb, "ac:submissions:submit", "workers")
	runStream := queue.NewStream(rdb, "ac:submissions:run", "workers")
	h := NewHandler(pool, submitStream, runStream, nil, judge, rdb, nil, nil, 5000, 8, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/submissions/not-a-uuid", nil)
	req.SetPathValue("id", "not-a-uuid")
	h.HandleGetSubmission(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestHandleGetSubmission_NotFound(t *testing.T) {
	judgeSrv := fakeJudgeServer(t, "1.13.0")
	judge := sandbox.NewClient(judgeSrv.URL, "test-token")
	_, rdb := newTestRedis(t)
	pool := newUnreachablePGPool(t)

	submitStream := queue.NewStream(rdb, "ac:submissions:submit", "workers")
	runStream := queue.NewStream(rdb, "ac:submissions:run", "workers")
	h := NewHandler(pool, submitStream, runStream, nil, judge, rdb, nil, nil, 5000, 8, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/submissions/00000000-0000-0000-0000-000000000001", nil)
	req.SetPathValue("id", "00000000-0000-0000-0000-000000000001")
	h.HandleGetSubmission(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestHandleExecute_RejectsUnknownChecker(t *testing.T) {
	judgeSrv := fakeJudgeServer(t, "1.13.0")
	judge := sandbox.NewClient(judgeSrv.URL, "test-token")
	_, rdb := newTestRedis(t)

	submitStream := queue.NewStream(rdb, "ac:submissions:submit", "workers")
	runStream := queue.NewStream(rdb, "ac:submissions:run", "workers")
	h := NewHandler(nil, submitStream, runStream, queue.NewPubSub(rdb), judge, rdb, nil, nil, 5000, 8, nil)

	body := `{"language":"python","source_code":"print(1)","checker":"regex","tests":[{"input":"","expected_output":"1"}]}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/execute", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	h.HandleExecute(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	if resp.Error != "checker must be one of: exact, token, float, custom" {
		t.Errorf("error = %q", resp.Error)
	}
}

const legacyModeError = "mode=submit and problem_version_id are no longer supported; send tests in the request body"

// assertLegacyRejected posts body and checks for the legacy-field 400 with nothing enqueued.
func assertLegacyRejected(t *testing.T, body string) {
	t.Helper()
	judgeSrv := fakeJudgeServer(t, "1.13.0")
	judge := sandbox.NewClient(judgeSrv.URL, "test-token")
	_, rdb := newTestRedis(t)

	submitStream := queue.NewStream(rdb, "ac:submissions:submit", "workers")
	runStream := queue.NewStream(rdb, "ac:submissions:run", "workers")
	h := NewHandler(nil, submitStream, runStream, queue.NewPubSub(rdb), judge, rdb, nil, nil, 5000, 8, nil)

	rec := postExecute(h, body)
	assertExecuteError(t, rec, http.StatusBadRequest, legacyModeError)

	for _, s := range []*queue.Stream{submitStream, runStream} {
		if n, _ := rdb.XLen(context.Background(), s.StreamName()).Result(); n != 0 {
			t.Errorf("XLen(%s) = %d, want 0", s.StreamName(), n)
		}
	}
}

func TestHandleExecute_LegacySubmitPayloadRejected(t *testing.T) {
	assertLegacyRejected(t, `{"language":"python","source_code":"print(1)","mode":"submit","problem_version_id":"0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"}`)
}

func TestHandleExecute_LegacySubmitWithTestsRejected(t *testing.T) {
	assertLegacyRejected(t, `{"language":"python","source_code":"print(1)","mode":"submit","problem_version_id":"0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b","tests":[{"input":"","expected_output":"1"}]}`)
}

func TestHandleExecute_ProblemVersionIDAloneRejected(t *testing.T) {
	assertLegacyRejected(t, `{"language":"python","source_code":"print(1)","problem_version_id":"0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b","tests":[{"input":"","expected_output":"1"}]}`)
}

func TestHandleExecute_ModeRunIsNotRejectedAsLegacy(t *testing.T) {
	judgeSrv := fakeJudgeServer(t, "1.13.0")
	judge := sandbox.NewClient(judgeSrv.URL, "test-token")
	_, rdb := newTestRedis(t)

	submitStream := queue.NewStream(rdb, "ac:submissions:submit", "workers")
	runStream := queue.NewStream(rdb, "ac:submissions:run", "workers")
	h := NewHandler(nil, submitStream, runStream, queue.NewPubSub(rdb), judge, rdb, nil, nil, 5000, 8, nil)

	// Passes the legacy gate and fails on the next validation step.
	rec := postExecute(h, `{"language":"python","source_code":"print(1)","mode":"run"}`)
	assertExecuteError(t, rec, http.StatusBadRequest, "at least one test is required")
}

func TestHandleExecute_RequiresTests(t *testing.T) {
	judgeSrv := fakeJudgeServer(t, "1.13.0")
	judge := sandbox.NewClient(judgeSrv.URL, "test-token")
	_, rdb := newTestRedis(t)

	submitStream := queue.NewStream(rdb, "ac:submissions:submit", "workers")
	runStream := queue.NewStream(rdb, "ac:submissions:run", "workers")
	h := NewHandler(nil, submitStream, runStream, queue.NewPubSub(rdb), judge, rdb, nil, nil, 5000, 8, nil)

	rec := postExecute(h, `{"language":"python","source_code":"print(1)"}`)
	assertExecuteError(t, rec, http.StatusBadRequest, "at least one test is required")
}

func TestHandleExecute_PriorityTiers(t *testing.T) {
	judgeSrv := fakeJudgeServer(t, "1.13.0")
	judge := sandbox.NewClient(judgeSrv.URL, "test-token")
	_, rdb := newTestRedis(t)
	pool := newFakePGPool(t)

	submitStream := queue.NewStream(rdb, "ac:submissions:submit", "workers")
	runStream := queue.NewStream(rdb, "ac:submissions:run", "workers")
	bulkStream := queue.NewStream(rdb, "ac:submissions:bulk", "workers")
	h := NewTieredHandler(pool, submitStream, runStream, bulkStream, queue.NewPubSub(rdb), judge, rdb, nil, nil, 5000, 8, nil)

	// Invalid priority is rejected with 400
	rec := postExecute(h, `{"language":"python","source_code":"print(1)","priority":"invalid","tests":[{"input":"","expected_output":"1"}]}`)
	assertExecuteError(t, rec, http.StatusBadRequest, "priority must be one of: run, submit, bulk")

	// P2 bulk priority routes to bulkStream
	recBulk := postExecute(h, `{"language":"python","source_code":"print(1)","priority":"bulk","tests":[{"input":"","expected_output":"1"}]}`)
	if recBulk.Code != http.StatusCreated {
		t.Fatalf("expected 201 for bulk priority, got %d: %s", recBulk.Code, recBulk.Body.String())
	}
	nBulk, _ := rdb.XLen(context.Background(), "ac:submissions:bulk").Result()
	if nBulk != 1 {
		t.Errorf("expected 1 item in bulk stream, got %d", nBulk)
	}

	// P1 submit priority routes to submitStream
	recSubmit := postExecute(h, `{"language":"python","source_code":"print(1)","priority":"submit","tests":[{"input":"","expected_output":"1"}]}`)
	if recSubmit.Code != http.StatusCreated {
		t.Fatalf("expected 201 for submit priority, got %d: %s", recSubmit.Code, recSubmit.Body.String())
	}
	nSub, _ := rdb.XLen(context.Background(), "ac:submissions:submit").Result()
	if nSub != 1 {
		t.Errorf("expected 1 item in submit stream, got %d", nSub)
	}
}

func postExecute(h *Handler, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/execute", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	h.HandleExecute(rec, req)
	return rec
}

func assertExecuteError(t *testing.T, rec *httptest.ResponseRecorder, code int, msg string) {
	t.Helper()
	if rec.Code != code {
		t.Fatalf("status = %d, want %d: %s", rec.Code, code, rec.Body.String())
	}
	var resp struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	if resp.Error != msg {
		t.Errorf("error = %q, want %q", resp.Error, msg)
	}
}
