package logging

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
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
