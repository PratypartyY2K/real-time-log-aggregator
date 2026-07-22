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

const (
	RequestIDHeader   = "X-Request-Id"
	TraceIDHeader     = "X-Trace-Id"
	TraceparentHeader = "traceparent"
)

type requestIDContextKey struct{}
type traceIDContextKey struct{}

func Middleware(logger *slog.Logger, service string, next http.Handler) http.Handler {
	if next == nil {
		return http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := requestIDFromHeader(r.Header.Get(RequestIDHeader))
		traceID := traceIDFromHeaders(r.Header)
		ctx := WithRequestID(r.Context(), requestID)
		ctx = WithTraceID(ctx, traceID)
		r = r.WithContext(ctx)

		w.Header().Set(RequestIDHeader, requestID)
		w.Header().Set(TraceIDHeader, traceID)

		start := time.Now()
		rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		if logger != nil {
			logger.Info(
				"http request completed",
				"service", service,
				"request_id", requestID,
				"trace_id", traceID,
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

func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDContextKey{}, strings.TrimSpace(requestID))
}

func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDContextKey{}, strings.ToLower(strings.TrimSpace(traceID)))
}

func RequestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	requestID, _ := ctx.Value(requestIDContextKey{}).(string)
	return requestID
}

func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	traceID, _ := ctx.Value(traceIDContextKey{}).(string)
	return traceID
}

// PropagateContext copies correlation identifiers to a downstream HTTP request.
func PropagateContext(ctx context.Context, header http.Header) {
	if ctx == nil || header == nil {
		return
	}
	if requestID := RequestIDFromContext(ctx); requestID != "" {
		header.Set(RequestIDHeader, requestID)
	}
	if traceID := TraceIDFromContext(ctx); validHexID(traceID, 32) {
		header.Set(TraceIDHeader, traceID)
		header.Set(TraceparentHeader, "00-"+traceID+"-"+randomHex(8)+"-01")
	}
}

func requestIDFromHeader(value string) string {
	value = strings.TrimSpace(value)
	if value != "" {
		return value
	}
	return randomRequestID()
}

func randomRequestID() string {
	return randomHex(16)
}

func randomHex(size int) string {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "bootstrap-request-id"
	}
	return hex.EncodeToString(buf)
}

func traceIDFromHeaders(header http.Header) string {
	parts := strings.Split(strings.TrimSpace(header.Get(TraceparentHeader)), "-")
	if len(parts) == 4 && parts[0] == "00" && validHexID(parts[1], 32) && validHexID(parts[2], 16) {
		return strings.ToLower(parts[1])
	}
	if traceID := strings.TrimSpace(header.Get(TraceIDHeader)); validHexID(traceID, 32) {
		return strings.ToLower(traceID)
	}
	return randomHex(16)
}

func validHexID(value string, length int) bool {
	if len(value) != length || strings.Trim(value, "0") == "" {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
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
