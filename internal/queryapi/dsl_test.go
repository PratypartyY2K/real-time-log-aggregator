package queryapi

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestQueryDSLHandlerRoutesLogQuery(t *testing.T) {
	store := &stubDSLStore{
		logs: []LogRecord{
			{Timestamp: time.Date(2026, 7, 13, 18, 0, 0, 0, time.UTC), Service: "checkout"},
		},
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewBufferString(`{
		"type":"logs",
		"start":"2026-07-13T17:00:00Z",
		"end":"2026-07-13T19:00:00Z",
		"service":"checkout",
		"page_size":25,
		"offset":50
	}`))
	rec := httptest.NewRecorder()

	NewQueryDSLHandler(store).ServeHTTP(rec, tenantRequest(req))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if store.logFilter.Service != "checkout" || store.logFilter.Limit != 25 || store.logFilter.Offset != 50 {
		t.Fatalf("unexpected log filter: %+v", store.logFilter)
	}
}

func TestQueryDSLHandlerRoutesAnalyticsQuery(t *testing.T) {
	store := &stubDSLStore{
		analytics: []AnalyticsPoint{
			{Group: map[string]string{"service": "checkout"}, Value: 8},
		},
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewBufferString(`{
		"type":"analytics",
		"start":"2026-07-13T17:00:00Z",
		"end":"2026-07-13T19:00:00Z",
		"aggregation":"count",
		"group_by":["service"],
		"top_k":5
	}`))
	rec := httptest.NewRecorder()

	NewQueryDSLHandler(store).ServeHTTP(rec, tenantRequest(req))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if store.analyticsQuery.TopK != 5 || len(store.analyticsQuery.GroupBy) != 1 || store.analyticsQuery.GroupBy[0] != "service" {
		t.Fatalf("unexpected analytics query: %+v", store.analyticsQuery)
	}
}

func TestQueryDSLHandlerRejectsUnknownFields(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/query", strings.NewReader(`{
		"type":"logs",
		"start":"2026-07-13T17:00:00Z",
		"end":"2026-07-13T19:00:00Z",
		"unsafe":"value"
	}`))
	rec := httptest.NewRecorder()

	NewQueryDSLHandler(&stubDSLStore{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

type stubDSLStore struct {
	logFilter      QueryFilter
	analyticsQuery AnalyticsQuery
	logs           []LogRecord
	analytics      []AnalyticsPoint
}

func (s *stubDSLStore) QueryLogs(_ context.Context, filter QueryFilter) ([]LogRecord, error) {
	s.logFilter = filter
	return s.logs, nil
}

func (s *stubDSLStore) StreamLogs(_ context.Context, filter QueryFilter, emit func(LogRecord) error) error {
	s.logFilter = filter
	for _, record := range s.logs {
		if err := emit(record); err != nil {
			return err
		}
	}
	return nil
}

func (s *stubDSLStore) QueryAnalytics(_ context.Context, query AnalyticsQuery) ([]AnalyticsPoint, error) {
	s.analyticsQuery = query
	return s.analytics, nil
}
