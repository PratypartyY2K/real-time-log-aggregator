package logging

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

const RequestIDHeader = "X-Request-Id"

type requestIDContextKey struct{}

func Middleware(logger *slog.Logger, service string, next http.Handler) http.Handler {
	if next == nil {
		return http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := requestIDFromHeader(r.Header.Get(RequestIDHeader))
		ctx := context.WithValue(r.Context(), requestIDContextKey{}, requestID)
		r = r.WithContext(ctx)

		w.Header().Set(RequestIDHeader, requestID)

		start := time.Now()
		rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		if logger != nil {
			logger.Info(
				"http request completed",
				"service", service,
				"request_id", requestID,
				"method", r.Method,
				"path", r.URL.Path,
				"query", r.URL.RawQuery,
				"status", rec.status,
				"duration_ms", time.Since(start).Milliseconds(),
				"response_bytes", rec.bytes,
				"remote_addr", r.RemoteAddr,
				"user_agent", r.UserAgent(),
			)
		}
	})
}

func RequestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	requestID, _ := ctx.Value(requestIDContextKey{}).(string)
	return requestID
}

func requestIDFromHeader(value string) string {
	value = strings.TrimSpace(value)
	if value != "" {
		return value
	}
	return randomRequestID()
}

func randomRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "bootstrap-request-id"
	}
	return hex.EncodeToString(buf[:])
}

type responseRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *responseRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(data []byte) (int, error) {
	n, err := r.ResponseWriter.Write(data)
	r.bytes += n
	return n, err
}
