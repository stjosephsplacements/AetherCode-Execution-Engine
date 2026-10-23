package gateway

import (
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/metrics"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/requestid"
)

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

		ctx := requestid.WithRequestID(r.Context(), reqID)
		r = r.WithContext(ctx)

		w.Header().Set("X-Request-Id", reqID)

		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()

		next.ServeHTTP(sw, r)

		duration := time.Since(start)

		routeLabel := normalizePath(r.URL.Path)
		metrics.HTTPRequestsTotal.WithLabelValues(r.Method, routeLabel, strconv.Itoa(sw.status)).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(r.Method, routeLabel).Observe(duration.Seconds())

		level := slog.LevelInfo
		if sw.status >= 500 {
			level = slog.LevelError
		} else if sw.status >= 400 {
			level = slog.LevelWarn
		}

		slog.Log(r.Context(), level, "http",
			"method", r.Method,
			"path", sanitizeLogValue(r.URL.Path),
			"status", sw.status,
			"duration_ms", duration.Milliseconds(),
			"bytes", sw.bytes,
			"ip", r.RemoteAddr,
			"request_id", reqID,
		)
	})
}

// remoteIP extracts the client IP for rate limiting.
// XFF/X-Real-Ip are only trusted when the direct peer matches trustedProxies.
func remoteIP(r *http.Request, trustedProxies []*net.IPNet) string {
	peerIP := peerAddr(r)

	if len(trustedProxies) > 0 && isTrusted(peerIP, trustedProxies) {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			for i := len(parts) - 1; i >= 0; i-- {
				ip := strings.TrimSpace(parts[i])
				if ip == "" {
					continue
				}
				if !isTrusted(net.ParseIP(ip), trustedProxies) {
					return ip
				}
			}
		}
		if xri := r.Header.Get("X-Real-Ip"); xri != "" {
			return strings.TrimSpace(xri)
		}
	}
	return peerIP.String()
}

func peerAddr(r *http.Request) net.IP {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return net.ParseIP(r.RemoteAddr)
	}
	return net.ParseIP(host)
}

func isTrusted(ip net.IP, nets []*net.IPNet) bool {
	if ip == nil {
		return false
	}
	for _, n := range nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

func ParseTrustedProxies(cidrs []string) []*net.IPNet {
	var nets []*net.IPNet
	for _, c := range cidrs {
		if !strings.Contains(c, "/") {
			c += "/32"
		}
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			continue
		}
		nets = append(nets, n)
	}
	return nets
}

// sanitizeLogValue strips ASCII control characters (including newlines and carriage
// returns) from user-supplied strings before they are written to structured log
// output. This prevents log-injection attacks where a crafted path could embed
// fake log entries or corrupt JSON log output.
func sanitizeLogValue(s string) string {
	out := make([]byte, 0, len(s))
	for i := range len(s) {
		b := s[i]
		if b >= 0x20 && b != 0x7f { // printable ASCII only
			out = append(out, b)
		}
	}
	return string(out)
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
