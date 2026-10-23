package worker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestDispatchWebhook(t *testing.T) {
	var mu sync.Mutex
	var receivedSignature string
	var receivedPayload WebhookPayload
	delivered := make(chan struct{}, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		receivedSignature = r.Header.Get("X-AetherCode-Signature")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &receivedPayload)
		w.WriteHeader(http.StatusOK)
		select {
		case delivered <- struct{}{}:
		default:
		}
	}))
	defer srv.Close()

	payload := WebhookPayload{
		SubmissionID: "sub-12345",
		Verdict:      "accepted",
		TestsPassed:  5,
		TestCount:    5,
		CPUTimeNs:    150000000,
		MemoryBytes:  1048576,
		RunTimeNs:    180000000,
		Timestamp:    time.Now().UTC(),
	}

	secret := "test-secret-key"
	ctx := context.Background()
	dispatchWebhook(ctx, srv.URL, secret, payload, nil)

	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("webhook was not delivered within 2s")
	}

	mu.Lock()
	defer mu.Unlock()
	if receivedPayload.SubmissionID != "sub-12345" {
		t.Errorf("expected submission ID sub-12345, got %q", receivedPayload.SubmissionID)
	}
	if receivedSignature == "" {
		t.Error("expected non-empty HMAC signature header")
	}
}
