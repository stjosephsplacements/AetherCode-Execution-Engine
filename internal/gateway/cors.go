package gateway

import (
	"net/http"
	"strings"
)

type CORSMiddleware struct {
	allowedOrigins map[string]bool
}

func NewCORS(origins string) *CORSMiddleware {
	allowed := map[string]bool{}
	for _, o := range strings.Split(origins, ",") {
		o = strings.TrimSpace(o)
		if o != "" {
			allowed[o] = true
		}
	}
	return &CORSMiddleware{allowedOrigins: allowed}
}

func (c *CORSMiddleware) Wrap(next http.Handler) http.Handler {
	if len(c.allowedOrigins) == 0 {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		allowed := origin != "" && c.allowedOrigins[origin]

		if allowed {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			w.Header().Set("Access-Control-Max-Age", "86400")
			w.Header().Set("Vary", "Origin")
		}

		if r.Method == http.MethodOptions && allowed {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
