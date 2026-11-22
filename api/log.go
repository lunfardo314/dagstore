package api

import (
	"log/slog"
	"net/http"
	"time"
)

// statusRecorder keeps the status code a handler wrote, for the request log.
type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

// Flush lets a streaming handler (export) flush through the recorder.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// logRequests logs every request at debug level, and at warning level a server error or a request
// slower than SlowRequest. Client errors are the client's and stay at debug. The export is a bulk
// stream, long by design, so it is never logged as slow.
func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)
		took := time.Since(t)
		level, msg := slog.LevelDebug, "request"
		switch {
		case rec.code >= 500:
			level, msg = slog.LevelWarn, "server error"
		case took > s.cfg.SlowRequest && r.URL.Path != "/export":
			level, msg = slog.LevelWarn, "slow request"
		}
		slog.Log(r.Context(), level, msg, "path", r.URL.RequestURI(), "status", rec.code,
			"took", took.Round(time.Millisecond), "remote", r.RemoteAddr)
	})
}
