package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/auth"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/db"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/eventlog"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
)

const wsPingEvery = sseHeartbeatEvery

func (h *Handler) HandleWS(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("job_id")
	if jobID == "" {
		http.Error(w, `{"error":"job_id is required"}`, http.StatusBadRequest)
		return
	}
	parsedJobID, err := uuid.Parse(jobID)
	if err != nil {
		http.Error(w, `{"error":"invalid job_id"}`, http.StatusBadRequest)
		return
	}
	safeJobID := parsedJobID.String()

	identity, hasAuth := auth.GetIdentity(r.Context())
	if !hasAuth {
		streamToken := r.URL.Query().Get("stream_token")
		tokenCtx, tokenCancel := context.WithTimeout(context.Background(), 2*time.Second)
		valid := verifyStreamToken(tokenCtx, h.rdb, safeJobID, streamToken)
		tokenCancel()
		if !valid {
			http.Error(w, `{"error":"invalid or missing stream_token"}`, http.StatusUnauthorized)
			return
		}
	} else {
		ownerCheckCtx, ownerCheckCancel := context.WithTimeout(context.Background(), 3*time.Second)
		sub, err := db.GetSubmission(ownerCheckCtx, h.pool, parsedJobID)
		ownerCheckCancel()
		if err != nil {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		if sub.TenantID != identity.TenantID || (sub.UserID == nil || *sub.UserID != identity.UserID) {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		slog.Warn("ws: accept failed", "job_id", safeJobID, "err", err)
		return
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()

	ctx := conn.CloseRead(r.Context())
	lastEventID := r.Header.Get("Last-Event-ID")

	go wsPingLoop(ctx, conn)

	eventCh := h.pubsub.Subscribe(ctx, safeJobID)

	events, err := h.pubsub.GetEventLog(ctx, safeJobID)
	if err != nil {
		slog.Error("ws: get event log failed", "job_id", safeJobID, "err", err)
	}

	hasTerminal := false
	replayedUpTo := ""
	for _, evt := range events {
		if lastEventID != "" && compareEventIDs(evt.ID, lastEventID) <= 0 {
			continue
		}
		if !wsSendEvent(ctx, conn, evt) {
			return
		}
		replayedUpTo = evt.ID
		if evt.IsTerminal() {
			hasTerminal = true
		}
	}
	if hasTerminal {
		return
	}

	deadline := time.NewTimer(sseMaxLifetime)
	defer deadline.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			if wsSendDeadlineVerdict(ctx, h, conn, parsedJobID, safeJobID, lastEventID, replayedUpTo) {
				return
			}
			return
		case evt, ok := <-eventCh:
			if !ok {
				return
			}
			if replayedUpTo != "" && compareEventIDs(evt.ID, replayedUpTo) <= 0 {
				continue
			}
			if lastEventID != "" && compareEventIDs(evt.ID, lastEventID) <= 0 {
				continue
			}
			replayedUpTo = evt.ID
			if !wsSendEvent(ctx, conn, evt) {
				return
			}
			if evt.IsTerminal() {
				return
			}
		drainLoop:
			for {
				select {
				case nextEvt, ok := <-eventCh:
					if !ok {
						break drainLoop
					}
					if replayedUpTo != "" && compareEventIDs(nextEvt.ID, replayedUpTo) <= 0 {
						continue
					}
					if lastEventID != "" && compareEventIDs(nextEvt.ID, lastEventID) <= 0 {
						continue
					}
					replayedUpTo = nextEvt.ID
					if !wsSendEvent(ctx, conn, nextEvt) {
						return
					}
					if nextEvt.IsTerminal() {
						return
					}
				default:
					break drainLoop
				}
			}
		}
	}
}

func wsPingLoop(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(wsPingEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func wsSendEvent(ctx context.Context, conn *websocket.Conn, evt eventlog.Event) bool {
	payload, err := json.Marshal(evt)
	if err != nil {
		slog.Warn("ws: marshal event", "event_id", evt.ID, "err", err)
		return true
	}
	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := conn.Write(writeCtx, websocket.MessageText, payload); err != nil {
		return false
	}
	return true
}

func wsSendDeadlineVerdict(ctx context.Context, h *Handler, conn *websocket.Conn, parsedJobID uuid.UUID, safeJobID, lastEventID, replayedUpTo string) bool {
	checkCtx, checkCancel := context.WithTimeout(context.Background(), 3*time.Second)
	latestEvents, _ := h.pubsub.GetEventLog(checkCtx, safeJobID)
	checkCancel()
	for _, evt := range latestEvents {
		if evt.IsTerminal() {
			if replayedUpTo != "" && compareEventIDs(evt.ID, replayedUpTo) <= 0 {
				continue
			}
			if lastEventID != "" && compareEventIDs(evt.ID, lastEventID) <= 0 {
				return true
			}
			wsSendEvent(ctx, conn, evt)
			return true
		}
	}

	dbCtx, dbCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if sub, err := db.GetSubmission(dbCtx, h.pool, parsedJobID); err == nil {
		if sub.Status == model.StatusCompleted || sub.Status == model.StatusFailed {
			dbEvt := eventlog.New(safeJobID, eventlog.EventVerdict, map[string]any{
				"verdict":      sub.Verdict,
				"tests_passed": sub.TestsPassed,
				"test_count":   sub.TestCount,
			})
			wsSendEvent(ctx, conn, dbEvt)
			dbCancel()
			return true
		}
	}
	dbCancel()

	timeoutEvt := eventlog.New(safeJobID, eventlog.EventVerdict, map[string]any{
		"verdict": "internal_error",
		"error":   "no verdict within timeout",
	})
	pubCtx, pubCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := h.pubsub.PublishEvent(pubCtx, timeoutEvt); err != nil {
		slog.Warn("ws: failed to persist timeout event", "job_id", safeJobID, "err", err)
	}
	pubCancel()
	wsSendEvent(ctx, conn, timeoutEvt)
	return true
}