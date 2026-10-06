package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"

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

	h := NewHandler(pool, submitStream, runStream, nil, judge, rdb, nil, 5000, false, 8)

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

	h := NewHandler(pool, submitStream, runStream, nil, judge, rdb, nil, 5000, false, 4)

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

	h := NewHandler(pool, submitStream, runStream, nil, judge, rdb, nil, 5000, false, 8)

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

	h := NewHandler(pool, submitStream, runStream, nil, judge, rdb, nil, 5000, false, 8)

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
