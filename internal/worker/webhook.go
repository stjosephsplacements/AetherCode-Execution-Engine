package worker

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
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

// dispatchWebhook delivers an HTTP POST notification to callbackURL with HMAC-SHA256 signature.
func dispatchWebhook(ctx context.Context, callbackURL, secret string, payload WebhookPayload, log *slog.Logger) {
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

	client := &http.Client{Timeout: 5 * time.Second}

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
