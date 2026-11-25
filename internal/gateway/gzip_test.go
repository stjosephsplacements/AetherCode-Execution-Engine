package gateway

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGzipDecompressMiddleware(t *testing.T) {
	var body bytes.Buffer
	zw := gzip.NewWriter(&body)
	_, _ = zw.Write([]byte(`{"ok":true}`))
	_ = zw.Close()

	var seen string
	h := GzipDecompressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		seen = string(b)
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/execute", bytes.NewReader(body.Bytes()))
	req.Header.Set("Content-Encoding", "gzip")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}
	if seen != `{"ok":true}` {
		t.Fatalf("body = %q", seen)
	}
}

func TestGzipDecompressMiddleware_PlainPassthrough(t *testing.T) {
	var seen string
	h := GzipDecompressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		seen = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`plain`))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if seen != "plain" {
		t.Fatalf("got %q", seen)
	}
}

func TestGzipResponseMiddleware_JSON(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"a":1}`))
	})
	h := GzipResponseMiddleware(inner)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/submissions/x", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("encoding = %q", rr.Header().Get("Content-Encoding"))
	}
	gr, err := gzip.NewReader(rr.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer gr.Close()
	out, _ := io.ReadAll(gr)
	if string(out) != `{"a":1}` {
		t.Fatalf("got %q", out)
	}
}

func TestGzipResponseMiddleware_SkipsSSE(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: hi\n\n"))
	})
	h := GzipResponseMiddleware(inner)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Header().Get("Content-Encoding") == "gzip" {
		t.Fatal("SSE must not be gzipped")
	}
}
