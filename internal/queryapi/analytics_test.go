package queryapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAnalyticsHandlerReturnsAggregatesForValidFilter(t *testing.T) {
	store := &stubAnalyticsStore{
		results: []AnalyticsPoint{
			{
				Bucket: "2026-07-13T18:00:00Z",
				Group:  map[string]string{"service": "checkout", "level": "error"},
				Value:  12,
			},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/analytics?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&aggregation=count&group_by=service,level&bucket=minute", nil)
	rec := httptest.NewRecorder()

	NewAnalyticsHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if store.query.Aggregation != "count" || store.query.Bucket != "minute" {
		t.Fatalf("unexpected analytics query: %+v", store.query)
	}
	if len(store.query.GroupBy) != 2 || store.query.GroupBy[0] != "service" || store.query.GroupBy[1] != "level" {
		t.Fatalf("unexpected group by: %+v", store.query.GroupBy)
	}

	var payload analyticsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Count != 1 || len(payload.Results) != 1 {
		t.Fatalf("expected one analytics result, got %+v", payload)
	}
}

func TestAnalyticsHandlerRejectsUnsupportedGroupBy(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/analytics?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&aggregation=count&group_by=message", nil)
	rec := httptest.NewRecorder()

	NewAnalyticsHandler(&stubAnalyticsStore{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestAnalyticsHandlerRejectsTopKWithoutGrouping(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/analytics?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&aggregation=count&top_k=5", nil)
	rec := httptest.NewRecorder()

	NewAnalyticsHandler(&stubAnalyticsStore{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestAnalyticsHandlerRejectsPercentileWithoutValueField(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/analytics?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&aggregation=percentile&percentile=95", nil)
	rec := httptest.NewRecorder()

	NewAnalyticsHandler(&stubAnalyticsStore{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestAnalyticsHandlerReturnsServiceUnavailableWhenStoreFails(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/analytics?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&aggregation=count", nil)
	rec := httptest.NewRecorder()

	NewAnalyticsHandler(&stubAnalyticsStore{err: errors.New("clickhouse unavailable")}).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

type stubAnalyticsStore struct {
	query   AnalyticsQuery
	results []AnalyticsPoint
	err     error
}

func (s *stubAnalyticsStore) QueryAnalytics(_ context.Context, query AnalyticsQuery) ([]AnalyticsPoint, error) {
	s.query = query
	return s.results, s.err
}
