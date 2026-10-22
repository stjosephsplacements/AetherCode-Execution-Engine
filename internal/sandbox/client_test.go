package sandbox

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/requestid"
)

func TestClientPropagatesRequestID(t *testing.T) {
	var capturedRunReqID string
	var capturedVerReqID string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/run":
			capturedRunReqID = r.Header.Get("X-Request-Id")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"status":"Accepted"}]`))
		case "/version":
			capturedVerReqID = r.Header.Get("X-Request-Id")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"buildVersion":"1.13.0"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "test-token")

	// Case 1: Context carries RequestID
	ctxWithID := requestid.WithRequestID(context.Background(), "test-req-id-1234")
	if _, err := client.Run(ctxWithID, Request{}); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if capturedRunReqID != "test-req-id-1234" {
		t.Errorf("Run X-Request-Id = %q, want %q", capturedRunReqID, "test-req-id-1234")
	}

	if _, err := client.Version(ctxWithID); err != nil {
		t.Fatalf("Version failed: %v", err)
	}
	if capturedVerReqID != "test-req-id-1234" {
		t.Errorf("Version X-Request-Id = %q, want %q", capturedVerReqID, "test-req-id-1234")
	}

	// Case 2: Context without RequestID (omitted)
	capturedRunReqID = ""
	capturedVerReqID = ""
	ctxNoID := context.Background()
	if _, err := client.Run(ctxNoID, Request{}); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if capturedRunReqID != "" {
		t.Errorf("Run X-Request-Id = %q, want empty", capturedRunReqID)
	}

	if _, err := client.Version(ctxNoID); err != nil {
		t.Fatalf("Version failed: %v", err)
	}
	if capturedVerReqID != "" {
		t.Errorf("Version X-Request-Id = %q, want empty", capturedVerReqID)
	}
}
