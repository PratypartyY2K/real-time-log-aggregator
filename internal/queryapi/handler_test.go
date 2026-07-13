package queryapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHandlerReturnsLogsForValidFilter(t *testing.T) {
	store := &stubLogStore{
		logs: []LogRecord{
			{
				Timestamp:    time.Date(2026, 7, 13, 18, 0, 0, 0, time.UTC),
				TenantID:     1,
				Service:      "checkout",
				Environment:  "prod",
				Source:       "app",
				Level:        "error",
				Message:      "database timeout",
				Fields:       map[string]any{"region": "us-west-2"},
				IngestID:     "req-123",
				RawSizeBytes: 128,
			},
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/logs?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&service=checkout&level=error&limit=50", nil)
	rec := httptest.NewRecorder()

	NewHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if store.filter.Service != "checkout" || store.filter.Level != "error" || store.filter.Limit != 50 {
		t.Fatalf("unexpected filter passed to store: %+v", store.filter)
	}

	var payload queryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Count != 1 || len(payload.Logs) != 1 {
		t.Fatalf("expected one log in payload, got %+v", payload)
	}
}

func TestHandlerRejectsMissingTimeRange(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/logs?service=checkout", nil)
	rec := httptest.NewRecorder()

	NewHandler(&stubLogStore{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandlerRejectsUnsafeFilter(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/logs?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&service=checkout%2Fapi", nil)
	rec := httptest.NewRecorder()

	NewHandler(&stubLogStore{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandlerReturnsServiceUnavailableWhenStoreFails(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/logs?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z", nil)
	rec := httptest.NewRecorder()

	NewHandler(&stubLogStore{err: errors.New("clickhouse unavailable")}).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

type stubLogStore struct {
	filter QueryFilter
	logs   []LogRecord
	err    error
}

func (s *stubLogStore) QueryLogs(_ context.Context, filter QueryFilter) ([]LogRecord, error) {
	s.filter = filter
	return s.logs, s.err
}
