package gateway

import (
	"compress/gzip"
	"net/http"
	"strings"
)

// GzipDecompressMiddleware unwraps Content-Encoding: gzip request bodies so
// handlers see plain JSON. Uncompressed requests pass through unchanged.
func GzipDecompressMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		gr, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, `{"error":"invalid gzip body"}`, http.StatusBadRequest)
			return
		}
		defer gr.Close()
		r.Body = gr
		r.Header.Del("Content-Encoding")
		r.Header.Del("Content-Length")
		r.ContentLength = -1
		next.ServeHTTP(w, r)
	})
}

type gzipResponseWriter struct {
	http.ResponseWriter
	gz     *gzip.Writer
	wrote  bool
	skip   bool
	status int
}

func (w *gzipResponseWriter) WriteHeader(code int) {
	w.status = code
	ct := w.Header().Get("Content-Type")
	if strings.HasPrefix(ct, "text/event-stream") {
		w.skip = true
	}
	if !w.skip {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Del("Content-Length")
		w.Header().Add("Vary", "Accept-Encoding")
	}
	w.ResponseWriter.WriteHeader(code)
	w.wrote = true
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	if w.skip {
		return w.ResponseWriter.Write(b)
	}
	return w.gz.Write(b)
}

func (w *gzipResponseWriter) Flush() {
	if !w.skip {
		_ = w.gz.Flush()
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *gzipResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// GzipResponseMiddleware compresses JSON (and other) responses when the client
// sends Accept-Encoding: gzip. SSE (text/event-stream) is never compressed.
func GzipResponseMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		gz := gzip.NewWriter(w)
		gw := &gzipResponseWriter{ResponseWriter: w, gz: gz, status: http.StatusOK}
		defer func() {
			if !gw.skip {
				_ = gz.Close()
			}
		}()
		next.ServeHTTP(gw, r)
	})
}
