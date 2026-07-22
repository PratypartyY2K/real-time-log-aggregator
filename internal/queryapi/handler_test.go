package queryapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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
	req := httptest.NewRequest(http.MethodGet, "/v1/logs?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&service=checkout&level=error&page_size=50&offset=25", nil)
	rec := httptest.NewRecorder()

	NewHandler(store).ServeHTTP(rec, tenantRequest(req))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if store.filter.TenantID != 7 || store.filter.Service != "checkout" || store.filter.Level != "error" || store.filter.Limit != 50 || store.filter.Offset != 25 {
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

func TestHandlerPassesTraceIDFilterToStore(t *testing.T) {
	store := &stubLogStore{}
	req := httptest.NewRequest(http.MethodGet, "/v1/logs?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&trace_id=trace-123", nil)
	rec := httptest.NewRecorder()

	NewHandler(store).ServeHTTP(rec, tenantRequest(req))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if store.filter.TraceID != "trace-123" {
		t.Fatalf("expected trace filter trace-123, got %q", store.filter.TraceID)
	}
}

func TestHandlerRejectsMissingTenantIdentity(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/logs?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z", nil)
	rec := httptest.NewRecorder()

	NewHandler(&stubLogStore{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandlerReturnsNextOffsetWhenPageIsFull(t *testing.T) {
	store := &stubLogStore{
		logs: []LogRecord{
			{Timestamp: time.Date(2026, 7, 13, 18, 0, 0, 0, time.UTC)},
			{Timestamp: time.Date(2026, 7, 13, 18, 1, 0, 0, time.UTC)},
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/logs?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&page_size=2&offset=4", nil)
	rec := httptest.NewRecorder()

	NewHandler(store).ServeHTTP(rec, tenantRequest(req))

	var payload queryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.NextOffset == nil || *payload.NextOffset != 6 {
		t.Fatalf("expected next offset 6, got %+v", payload.NextOffset)
	}
}

func TestHandlerReportsPartialResults(t *testing.T) {
	store := &stubLogStore{status: ClusterStatus{Partial: true, UnavailableShards: []string{"shard-2"}}}
	req := tenantRequest(httptest.NewRequest(http.MethodGet, "/v1/logs?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z", nil))
	rec := httptest.NewRecorder()

	NewHandler(store).ServeHTTP(rec, req)

	var payload queryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !payload.Partial || len(payload.UnavailableShards) != 1 || payload.UnavailableShards[0] != "shard-2" {
		t.Fatalf("expected partial result metadata, got %+v", payload)
	}
}

func TestHandlerStreamsLogsAsNDJSON(t *testing.T) {
	store := &stubLogStore{
		logs: []LogRecord{
			{Timestamp: time.Date(2026, 7, 13, 18, 0, 0, 0, time.UTC), Service: "checkout"},
			{Timestamp: time.Date(2026, 7, 13, 18, 1, 0, 0, time.UTC), Service: "billing"},
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/logs?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&stream=true&page_size=2", nil)
	rec := httptest.NewRecorder()

	NewHandler(store).ServeHTTP(rec, tenantRequest(req))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/x-ndjson" {
		t.Fatalf("unexpected content type: %s", got)
	}
	if lines := strings.Count(strings.TrimSpace(rec.Body.String()), "\n") + 1; lines != 2 {
		t.Fatalf("expected two streamed lines, got body %q", rec.Body.String())
	}
}

func TestHandlerRejectsMissingTimeRange(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/logs?service=checkout", nil)
	rec := httptest.NewRecorder()

	NewHandler(&stubLogStore{}).ServeHTTP(rec, tenantRequest(req))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandlerRejectsUnsafeFilter(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/logs?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&service=checkout%2Fapi", nil)
	rec := httptest.NewRecorder()

	NewHandler(&stubLogStore{}).ServeHTTP(rec, tenantRequest(req))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandlerRejectsExpensiveRawQueryWindow(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/logs?start=2026-07-01T00:00:00Z&end=2026-07-13T00:00:00Z", nil)
	rec := httptest.NewRecorder()

	NewHandler(&stubLogStore{}).ServeHTTP(rec, tenantRequest(req))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandlerReturnsServiceUnavailableWhenStoreFails(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/logs?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z", nil)
	rec := httptest.NewRecorder()

	NewHandler(&stubLogStore{err: errors.New("clickhouse unavailable")}).ServeHTTP(rec, tenantRequest(req))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

type stubLogStore struct {
	filter QueryFilter
	logs   []LogRecord
	err    error
	status ClusterStatus
}

func (s *stubLogStore) ClusterStatus(context.Context) ClusterStatus {
	return s.status
}

func tenantRequest(req *http.Request) *http.Request {
	return req.WithContext(WithTenantID(req.Context(), 7))
}

func (s *stubLogStore) QueryLogs(_ context.Context, filter QueryFilter) ([]LogRecord, error) {
	s.filter = filter
	return s.logs, s.err
}

func (s *stubLogStore) StreamLogs(_ context.Context, filter QueryFilter, emit func(LogRecord) error) error {
	s.filter = filter
	if s.err != nil {
		return s.err
	}
	for _, record := range s.logs {
		if err := emit(record); err != nil {
			return err
		}
	}
	return nil
}
