package main

import (
	"context"
	"net/http"
	"net/http/httptest"
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

type stubLogStore struct {
	logs []queryapi.LogRecord
	err  error
}

func (s *stubLogStore) QueryLogs(context.Context, queryapi.QueryFilter) ([]queryapi.LogRecord, error) {
	return s.logs, s.err
}
