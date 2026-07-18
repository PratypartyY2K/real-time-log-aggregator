package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/queryapi"
)

func TestRoutesExposeQueryEndpoint(t *testing.T) {
	handler := routes("query-api", &stubLogStore{
		logs: []queryapi.LogRecord{
			{
				Timestamp:   time.Date(2026, 7, 13, 18, 0, 0, 0, time.UTC),
				Service:     "checkout",
				Level:       "error",
				Message:     "database timeout",
				Environment: "prod",
				Source:      "app",
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/logs?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRoutesExposeAnalyticsEndpoint(t *testing.T) {
	handler := routes("query-api", &stubLogStore{
		analytics: []queryapi.AnalyticsPoint{
			{
				Bucket: "2026-07-13T18:00:00Z",
				Group:  map[string]string{"service": "checkout"},
				Value:  9,
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/analytics?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&aggregation=count&group_by=service&bucket=hour", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestRoutesExposePrometheusMetrics(t *testing.T) {
	handler := routes("query-api", &stubLogStore{})

	logReq := httptest.NewRequest(http.MethodGet, "/v1/logs?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z", nil)
	logRec := httptest.NewRecorder()
	handler.ServeHTTP(logRec, logReq)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("unexpected content type: %s", got)
	}
	if body := rec.Body.String(); !strings.Contains(body, `logagg_http_requests_total{`) || !strings.Contains(body, `route="/v1/logs"`) {
		t.Fatalf("expected query metrics payload, got %s", body)
	}
}

func TestRoutesExposeReadiness(t *testing.T) {
	handler := routes("query-api", &stubLogStore{})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"ready"`) {
		t.Fatalf("expected readiness payload, got %s", rec.Body.String())
	}
}

type stubLogStore struct {
	logs      []queryapi.LogRecord
	analytics []queryapi.AnalyticsPoint
	err       error
}

func (s *stubLogStore) QueryLogs(context.Context, queryapi.QueryFilter) ([]queryapi.LogRecord, error) {
	return s.logs, s.err
}

func (s *stubLogStore) StreamLogs(_ context.Context, _ queryapi.QueryFilter, emit func(queryapi.LogRecord) error) error {
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

func (s *stubLogStore) QueryAnalytics(context.Context, queryapi.AnalyticsQuery) ([]queryapi.AnalyticsPoint, error) {
	return s.analytics, s.err
}

func (s *stubLogStore) Check(context.Context) error {
	return s.err
}
