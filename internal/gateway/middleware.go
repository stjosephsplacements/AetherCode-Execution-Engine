package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/metrics"
)

type ctxKey int

const requestIDKey ctxKey = 0

func RequestID(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey).(string); ok {
		return id
	}
	return ""
}

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, _ := uuid.NewV7()
		reqID := id.String()

		ctx := context.WithValue(r.Context(), requestIDKey, reqID)
		r = r.WithContext(ctx)

		w.Header().Set("X-Request-Id", reqID)

		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()

		next.ServeHTTP(sw, r)

		duration := time.Since(start)

		routeLabel := normalizePath(r.URL.Path)
		metrics.HTTPRequestsTotal.WithLabelValues(r.Method, routeLabel, fmt.Sprintf("%d", sw.status)).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(r.Method, routeLabel).Observe(duration.Seconds())

		level := slog.LevelInfo
		if sw.status >= 500 {
			level = slog.LevelError
		} else if sw.status >= 400 {
			level = slog.LevelWarn
		}

		slog.Log(r.Context(), level, "http",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"duration_ms", duration.Milliseconds(),
			"bytes", sw.bytes,
			"ip", r.RemoteAddr,
			"request_id", reqID,
		)
	})
}

var knownPaths = map[string]bool{
	"/api/v1/execute": true,
	"/api/v1/stream":  true,
	"/api/v1/health":  true,
}

func normalizePath(p string) string {
	if knownPaths[p] {
		return p
	}
	return "other"
}
