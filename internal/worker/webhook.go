package worker

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"time"
)

type WebhookPayload struct {
	SubmissionID string           `json:"submission_id"`
	Verdict      string           `json:"verdict"`
	TestsPassed  int              `json:"tests_passed"`
	TestCount    int              `json:"test_count"`
	CPUTimeNs    int64            `json:"cpu_time_ns"`
	MemoryBytes  int64            `json:"memory_bytes"`
	RunTimeNs    int64            `json:"run_time_ns"`
	Timestamp    time.Time        `json:"timestamp"`
	Results      []map[string]any `json:"results,omitempty"`
}

// ssrfBlockedPrefixes is the list of IP address prefixes that must never be the
// target of an outbound webhook request. Covers loopback, RFC-1918 private,
// link-local (including cloud-metadata 169.254.169.254), and IPv6 equivalents.
var ssrfBlockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("127.0.0.0/8"),     // IPv4 loopback
	netip.MustParsePrefix("10.0.0.0/8"),      // RFC-1918 private
	netip.MustParsePrefix("172.16.0.0/12"),   // RFC-1918 private
	netip.MustParsePrefix("192.168.0.0/16"),  // RFC-1918 private
	netip.MustParsePrefix("169.254.0.0/16"),  // link-local / cloud metadata
	netip.MustParsePrefix("100.64.0.0/10"),   // CGNAT (RFC 6598)
	netip.MustParsePrefix("192.0.0.0/24"),    // IETF protocol assignments
	netip.MustParsePrefix("198.18.0.0/15"),   // benchmarking (RFC 2544)
	netip.MustParsePrefix("198.51.100.0/24"), // TEST-NET-2 (RFC 5737)
	netip.MustParsePrefix("203.0.113.0/24"),  // TEST-NET-3 (RFC 5737)
	netip.MustParsePrefix("240.0.0.0/4"),     // reserved
	netip.MustParsePrefix("::1/128"),         // IPv6 loopback
	netip.MustParsePrefix("fc00::/7"),        // IPv6 unique local (RFC 4193)
	netip.MustParsePrefix("fe80::/10"),       // IPv6 link-local
	netip.MustParsePrefix("64:ff9b::/96"),    // NAT64 well-known prefix (RFC 6052)
	netip.MustParsePrefix("64:ff9b:1::/48"),  // NAT64 local-use prefix (RFC 8215)
}

// errSSRFBlocked is returned by the custom dialer when the resolved address is
// in a blocked range.
var errSSRFBlocked = errors.New("webhook: target address is in a blocked IP range (SSRF protection)")

// ssrfSafeDialContext is a net.Dialer.DialContext replacement that resolves the
// destination hostname and rejects the connection when any resolved IP falls
// within a private, loopback, link-local, or cloud-metadata range. Because
// every TCP connection — including those created while following HTTP redirects
// — goes through this hook, it also prevents redirect-based SSRF.
func ssrfSafeDialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("webhook: parse address %q: %w", addr, err)
	}

	addrs, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("webhook: resolve %q: %w", host, err)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("webhook: no addresses resolved for %q", host)
	}

	for _, raw := range addrs {
		ip, parseErr := netip.ParseAddr(raw)
		if parseErr != nil {
			return nil, fmt.Errorf("webhook: parse resolved IP %q: %w", raw, parseErr)
		}
		// Unmap IPv4-in-IPv6 so that the IPv4 blocked prefixes above match.
		ip = ip.Unmap()
		for _, blocked := range ssrfBlockedPrefixes {
			if blocked.Contains(ip) {
				return nil, errSSRFBlocked
			}
		}
	}

	// All resolved addresses are routable; connect to the first one.
	d := &net.Dialer{Timeout: 10 * time.Second}
	return d.DialContext(ctx, network, net.JoinHostPort(addrs[0], port))
}

// newWebhookClient returns an http.Client configured with the SSRF-safe dialer
// and a hard timeout. CheckRedirect blocks all redirects: the caller must not
// follow a 3xx response that could point to an internal address.
func newWebhookClient() *http.Client {
	transport := &http.Transport{
		DialContext: ssrfSafeDialContext,
	}
	return &http.Client{
		Timeout:   5 * time.Second,
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return errors.New("webhook: redirects are not followed (SSRF protection)")
		},
	}
}

// dispatchWebhook delivers an HTTP POST notification to callbackURL with HMAC-SHA256 signature.
// client may be nil, in which case a new SSRF-safe client is used. Pass a non-nil
// client only in tests that need to reach loopback servers.
func dispatchWebhook(ctx context.Context, callbackURL, secret string, payload WebhookPayload, log *slog.Logger, client *http.Client) {
	if callbackURL == "" {
		return
	}

	body, err := json.Marshal(payload)
	if err != nil {
		if log != nil {
			log.Warn("failed to marshal webhook payload", "err", err)
		}
		return
	}

	if client == nil {
		client = newWebhookClient()
	}

	// Retry up to 3 times with 500ms backoff
	for attempt := 1; attempt <= 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, callbackURL, bytes.NewReader(body))
		if err != nil {
			if log != nil {
				log.Warn("failed to build webhook request", "err", err)
			}
			return
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "AetherCode-Webhook-Dispatcher/1.0")

		if secret != "" {
			mac := hmac.New(sha256.New, []byte(secret))
			mac.Write(body)
			signature := hex.EncodeToString(mac.Sum(nil))
			req.Header.Set("X-AetherCode-Signature", signature)
		}

		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				if log != nil {
					log.Info("webhook delivered successfully", "url", callbackURL, "status", resp.StatusCode)
				}
				return
			}
			if log != nil {
				log.Warn("webhook returned non-2xx status", "attempt", attempt, "status", resp.StatusCode)
			}
		} else if log != nil {
			log.Warn("webhook attempt failed", "attempt", attempt, "err", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}
