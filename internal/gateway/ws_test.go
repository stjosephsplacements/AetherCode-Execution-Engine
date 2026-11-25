package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/eventlog"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/queue"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/sandbox"
)

func TestHandleWS_MissingJobID(t *testing.T) {
	_, rdb := newTestRedis(t)
	h := NewHandler(nil,
		queue.NewStream(rdb, "ac:submissions:submit", "workers"),
		queue.NewStream(rdb, "ac:submissions:run", "workers"),
		queue.NewPubSub(rdb),
		sandbox.NewClient("http://127.0.0.1:1", "t"),
		rdb, nil, nil, 5000, 1, nil,
	)
	rec := httptest.NewRecorder()
	h.HandleWS(rec, httptest.NewRequest(http.MethodGet, "/api/v1/ws", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "job_id is required") {
		t.Fatalf("body=%q", rec.Body.String())
	}
}

func TestHandleWS_InvalidJobID(t *testing.T) {
	_, rdb := newTestRedis(t)
	h := NewHandler(nil,
		queue.NewStream(rdb, "ac:submissions:submit", "workers"),
		queue.NewStream(rdb, "ac:submissions:run", "workers"),
		queue.NewPubSub(rdb),
		sandbox.NewClient("http://127.0.0.1:1", "t"),
		rdb, nil, nil, 5000, 1, nil,
	)
	rec := httptest.NewRecorder()
	h.HandleWS(rec, httptest.NewRequest(http.MethodGet, "/api/v1/ws?job_id=not-a-uuid", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", rec.Code)
	}
}

func TestHandleWS_BadStreamToken(t *testing.T) {
	_, rdb := newTestRedis(t)
	h := NewHandler(nil,
		queue.NewStream(rdb, "ac:submissions:submit", "workers"),
		queue.NewStream(rdb, "ac:submissions:run", "workers"),
		queue.NewPubSub(rdb),
		sandbox.NewClient("http://127.0.0.1:1", "t"),
		rdb, nil, nil, 5000, 1, nil,
	)
	jobID := uuid.New().String()
	for _, tok := range []string{"", "wrong"} {
		rec := httptest.NewRecorder()
		url := "/api/v1/ws?job_id=" + jobID + "&stream_token=" + tok
		h.HandleWS(rec, httptest.NewRequest(http.MethodGet, url, nil))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("token %q: status=%d want 401", tok, rec.Code)
		}
	}
}

func TestHandleWS_ReplaysTerminalEvent(t *testing.T) {
	_, rdb := newTestRedis(t)
	ps := queue.NewPubSub(rdb)
	h := NewHandler(nil,
		queue.NewStream(rdb, "ac:submissions:submit", "workers"),
		queue.NewStream(rdb, "ac:submissions:run", "workers"),
		ps,
		sandbox.NewClient("http://127.0.0.1:1", "t"),
		rdb, nil, nil, 5000, 1, nil,
	)
	ctx := context.Background()
	jobID := uuid.New()
	safeID := jobID.String()
	token, err := newStreamToken(ctx, rdb, safeID)
	if err != nil {
		t.Fatalf("stream token: %v", err)
	}
	verdict := eventlog.New(safeID, eventlog.EventVerdict, map[string]any{"verdict": "accepted"})
	if err := ps.PublishEvent(ctx, verdict); err != nil {
		t.Fatalf("publish: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(h.HandleWS))
	t.Cleanup(srv.Close)

	wsURL := strings.Replace(srv.URL, "http://", "ws://", 1) +
		"?job_id=" + safeID + "&stream_token=" + token
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(dialCtx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status=%d", resp.StatusCode)
	}

	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readCancel()
	_, data, err := conn.Read(readCtx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var got eventlog.Event
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Type != eventlog.EventVerdict {
		t.Fatalf("type=%s want VERDICT", got.Type)
	}
}