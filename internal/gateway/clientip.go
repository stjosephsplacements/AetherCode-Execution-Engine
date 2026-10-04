package gateway

import (
	"net"
	"net/http"
	"strings"
)

// ClientIP extracts the client IP from the request.
// When trustProxy is true, it reads the leftmost entry from X-Forwarded-For.
// Otherwise it uses r.RemoteAddr directly.
func ClientIP(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			raw := strings.TrimSpace(strings.SplitN(xff, ",", 2)[0])
			if parsed := net.ParseIP(raw); parsed != nil {
				return parsed.String() // normalised, CRLF-free, canonical form
			}
		}
		if xri := r.Header.Get("X-Real-Ip"); xri != "" {
			return strings.TrimSpace(xri)
		}
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
