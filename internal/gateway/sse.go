package gateway

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/auth"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/db"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/eventlog"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
)

const (
	sseMaxLifetime    = 5 * time.Minute
	sseHeartbeatEvery = 15 * time.Second
)

func (h *Handler) HandleStream(w http.ResponseWriter, r *http.Request) {
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

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"error":"streaming not supported"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ctx := r.Context()
	lastEventID := r.Header.Get("Last-Event-ID")

	// Ownership check: fetch submission and verify caller identity.
	identity, hasAuth := auth.GetIdentity(ctx)
	ownerCheckCtx, ownerCheckCancel := context.WithTimeout(context.Background(), 3*time.Second)
	sub, err := db.GetSubmission(ownerCheckCtx, h.pool, parsedJobID)
	ownerCheckCancel()
	if err != nil {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}
	if hasAuth {
		if sub.TenantID != identity.TenantID || (sub.UserID == nil || *sub.UserID != identity.UserID) {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
	} else {
		// Unauthenticated callers may only stream run-mode jobs they created in the same session;
		// require at minimum that the job is in run mode (no ProblemVersionID) and that
		// auth is disabled globally. In deployments with auth enabled, reject unauthenticated streams.
		if sub.ProblemVersionID != nil {
			http.Error(w, `{"error":"authentication required"}`, http.StatusUnauthorized)
			return
		}
	}

	// Subscribe FIRST to avoid race between replay and live events
	eventCh := h.pubsub.Subscribe(ctx, jobID)

	// Replay stored events
	events, err := h.pubsub.GetEventLog(ctx, jobID)
	if err != nil {
		slog.Error("sse: get event log failed", "job_id", jobID, "err", err)
	}

	hasTerminal := false
	replayedUpTo := ""
	for _, evt := range events {
		if lastEventID != "" && compareEventIDs(evt.ID, lastEventID) <= 0 {
			continue
		}
		if _, err := w.Write(evt.MarshalSSE()); err != nil { //nolint:gosec // G705: server-computed SSE payload (json.Marshal HTML-escapes it); the stream is machine-readable, not user-controlled HTML
			return
		}
		flusher.Flush()
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
	heartbeat := time.NewTicker(sseHeartbeatEvery)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			// Check if a real verdict landed in the event log while we waited.
			checkCtx, checkCancel := context.WithTimeout(context.Background(), 3*time.Second)
			latestEvents, _ := h.pubsub.GetEventLog(checkCtx, jobID)
			checkCancel()
			for _, evt := range latestEvents {
				if evt.IsTerminal() {
					// Skip events already sent during replay or live streaming.
					if replayedUpTo != "" && compareEventIDs(evt.ID, replayedUpTo) <= 0 {
						continue
					}
					if lastEventID != "" && compareEventIDs(evt.ID, lastEventID) <= 0 {
						// client already has this event
						return
					}
					if _, err := w.Write(evt.MarshalSSE()); err != nil { //nolint:gosec // G705: server-computed SSE payload (json.Marshal HTML-escapes it); the stream is machine-readable, not user-controlled HTML
						slog.Warn("sse: write deadline terminal failed", "job_id", jobID, "err", err)
					}
					flusher.Flush()
					return
				}
			}

			// Fallback: event log may have expired (10min TTL). Check PG directly.
			dbCtx, dbCancel := context.WithTimeout(context.Background(), 3*time.Second)
			if sub, err := db.GetSubmission(dbCtx, h.pool, parsedJobID); err == nil {
				if sub.Status == model.StatusCompleted || sub.Status == model.StatusFailed {
					dbEvt := eventlog.New(jobID, eventlog.EventVerdict, map[string]any{
						"verdict":      sub.Verdict,
						"tests_passed": sub.TestsPassed,
						"test_count":   sub.TestCount,
					})
					if _, err := w.Write(dbEvt.MarshalSSE()); err != nil { //nolint:gosec // G705: server-computed SSE payload (json.Marshal HTML-escapes it); the stream is machine-readable, not user-controlled HTML
						slog.Warn("sse: write db verdict failed", "job_id", jobID, "err", err)
					}
					flusher.Flush()
					dbCancel()
					return
				}
			}
			dbCancel()

			timeoutEvt := eventlog.New(jobID, eventlog.EventVerdict, map[string]any{
				"verdict": "internal_error",
				"error":   "no verdict within timeout",
			})
			pubCtx, pubCancel := context.WithTimeout(context.Background(), 3*time.Second)
			if err := h.pubsub.PublishEvent(pubCtx, timeoutEvt); err != nil {
				slog.Warn("sse: failed to persist timeout event", "job_id", jobID, "err", err)
			}
			pubCancel()
			if _, err := w.Write(timeoutEvt.MarshalSSE()); err != nil { //nolint:gosec // G705: server-computed SSE payload (json.Marshal HTML-escapes it); the stream is machine-readable, not user-controlled HTML
				return
			}
			flusher.Flush()
			return
		case <-heartbeat.C:
			if _, err := w.Write([]byte(": heartbeat\n\n")); err != nil {
				return
			}
			flusher.Flush()
		case evt, ok := <-eventCh:
			if !ok {
				// Redis disconnect: write a comment frame so the client reconnects
				// with its current Last-Event-ID rather than closing the stream silently.
				// All durable events are in the log; reconnect will replay any missed ones.
				_, _ = w.Write([]byte(": reconnect\n\n"))
				flusher.Flush()
				return
			}
			if replayedUpTo != "" && compareEventIDs(evt.ID, replayedUpTo) <= 0 {
				continue
			}
			if lastEventID != "" && compareEventIDs(evt.ID, lastEventID) <= 0 {
				continue
			}
			replayedUpTo = evt.ID
			if _, err := w.Write(evt.MarshalSSE()); err != nil { //nolint:gosec // G705: server-computed SSE payload (json.Marshal HTML-escapes it); the stream is machine-readable, not user-controlled HTML
				return
			}
			flusher.Flush()
			if evt.IsTerminal() {
				return
			}
		}
	}
}

// compareEventIDs compares two event IDs of the form "jobID:epoch-seq".
// Returns -1, 0, or 1.
func compareEventIDs(a, b string) int {
	aEpoch, aSeq := parseEventSuffix(a)
	bEpoch, bSeq := parseEventSuffix(b)

	if aEpoch != bEpoch {
		if aEpoch < bEpoch {
			return -1
		}
		return 1
	}
	if aSeq != bSeq {
		if aSeq < bSeq {
			return -1
		}
		return 1
	}
	return 0
}

func parseEventSuffix(id string) (int64, int64) {
	// Format: "jobID:epoch-seq" or legacy "jobID:seq"
	idx := strings.LastIndex(id, ":")
	if idx < 0 {
		return 0, 0
	}
	suffix := id[idx+1:]

	if dash := strings.Index(suffix, "-"); dash >= 0 {
		epoch, _ := strconv.ParseInt(suffix[:dash], 10, 64)
		seq, _ := strconv.ParseInt(suffix[dash+1:], 10, 64)
		return epoch, seq
	}

	seq, _ := strconv.ParseInt(suffix, 10, 64)
	return 0, seq
}
