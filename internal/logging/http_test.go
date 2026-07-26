package logging

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMiddlewareReusesIncomingRequestID(t *testing.T) {
	t.Parallel()

	var requestID string
	handler := Middleware(slog.New(slog.NewTextHandler(io.Discard, nil)), "query-api", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID = RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/logs", nil)
	req.Header.Set(RequestIDHeader, "req-123")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if requestID != "req-123" {
		t.Fatalf("expected context request id req-123, got %q", requestID)
	}
	if got := rec.Header().Get(RequestIDHeader); got != "req-123" {
		t.Fatalf("expected response request id req-123, got %q", got)
	}
}

func TestMiddlewareGeneratesRequestIDWhenMissing(t *testing.T) {
	t.Parallel()

	var requestID string
	handler := Middleware(nil, "query-api", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID = RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if len(requestID) != 32 {
		t.Fatalf("expected 32-char request id, got %q", requestID)
	}
	if got := rec.Header().Get(RequestIDHeader); got != requestID {
		t.Fatalf("expected response request id %q, got %q", requestID, got)
	}
}

func TestRequestIDFromContextHandlesMissingValue(t *testing.T) {
	t.Parallel()

	if got := RequestIDFromContext(context.Background()); got != "" {
		t.Fatalf("expected empty request id, got %q", got)
	}
}

func TestMiddlewareAcceptsTraceparentAndPropagatesContext(t *testing.T) {
	t.Parallel()

	const traceID = "0af7651916cd43dd8448eb211c80319c"
	var downstream http.Header
	handler := Middleware(nil, "ingest-api", http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		downstream = make(http.Header)
		PropagateContext(r.Context(), downstream)
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", nil)
	req.Header.Set(RequestIDHeader, "req-123")
	req.Header.Set(TraceparentHeader, "00-"+traceID+"-b7ad6b7169203331-01")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get(TraceIDHeader); got != traceID {
		t.Fatalf("expected response trace id %q, got %q", traceID, got)
	}
	if got := downstream.Get(RequestIDHeader); got != "req-123" {
		t.Fatalf("expected downstream request id req-123, got %q", got)
	}
	if got := downstream.Get(TraceparentHeader); !strings.HasPrefix(got, "00-"+traceID+"-") {
		t.Fatalf("expected downstream traceparent for %q, got %q", traceID, got)
	}
}

func TestMiddlewareReplacesInvalidTraceContext(t *testing.T) {
	t.Parallel()

	handler := Middleware(nil, "query-api", http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if got := TraceIDFromContext(r.Context()); len(got) != 32 || got == strings.Repeat("0", 32) {
			t.Fatalf("expected generated trace id, got %q", got)
		}
	}))
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set(TraceparentHeader, "invalid")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
}
