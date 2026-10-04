package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func reqWith(remote string, headers map[string]string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "http://example.test/", nil)
	r.RemoteAddr = remote
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	return r
}

func TestClientIP(t *testing.T) {
	cases := []struct {
		name       string
		remote     string
		trustProxy bool
		headers    map[string]string
		want       string
	}{
		{"direct, no proxy", "1.2.3.4:5678", false, nil, "1.2.3.4"},
		{"proxy, xff leftmost wins", "10.0.0.1:1234", true,
			map[string]string{"X-Forwarded-For": "9.9.9.9, 10.0.0.1"}, "9.9.9.9"},
		{"xff ignored when proxy not trusted", "10.0.0.1:1234", false,
			map[string]string{"X-Forwarded-For": "9.9.9.9"}, "10.0.0.1"},
		{"malformed xff falls back to remote", "10.0.0.1:1234", true,
			map[string]string{"X-Forwarded-For": "garbage, 9.9.9.9"}, "10.0.0.1"},
		{"x-real-ip fallback", "10.0.0.1:1234", true,
			map[string]string{"X-Real-Ip": "7.7.7.7"}, "7.7.7.7"},
		{"remote addr without port", "1.2.3.4", false, nil, "1.2.3.4"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ClientIP(reqWith(c.remote, c.headers), c.trustProxy)
			if got != c.want {
				t.Fatalf("ClientIP() = %q, want %q", got, c.want)
			}
		})
	}
}
