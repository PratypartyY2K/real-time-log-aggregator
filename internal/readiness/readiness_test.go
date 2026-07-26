package readiness

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthHandlerReturnsProcessHealth(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	HealthHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("expected application/json content type, got %q", rec.Header().Get("Content-Type"))
	}
	if rec.Body.String() != "{\"status\":\"ok\"}\n" {
		t.Fatalf("unexpected health response: %s", rec.Body.String())
	}
}

func TestHandlerReturnsReadyWhenAllChecksPass(t *testing.T) {
	t.Parallel()

	handler := NewHandler(
		Func("postgres", func(context.Context) error { return nil }),
		Func("nats", func(context.Context) error { return nil }),
	)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var payload response
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Status != "ready" {
		t.Fatalf("expected ready status, got %#v", payload)
	}
}

func TestHandlerReturnsUnavailableWhenAnyCheckFails(t *testing.T) {
	t.Parallel()

	handler := NewHandler(
		Func("clickhouse", func(context.Context) error { return errors.New("unreachable") }),
	)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}

	var payload response
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Status != "not_ready" || len(payload.Checks) != 1 || payload.Checks[0].Status != "error" {
		t.Fatalf("unexpected payload: %#v", payload)
	}
}
