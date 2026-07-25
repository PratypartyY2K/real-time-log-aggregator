package queryapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func BenchmarkQueryLogsHandler(b *testing.B) {
	records := make([]LogRecord, 100)
	for i := range records {
		records[i] = LogRecord{
			Timestamp:    time.Date(2026, 7, 24, 12, 0, i, 0, time.UTC),
			TenantID:     42,
			Service:      "checkout",
			Environment:  "prod",
			Source:       "app",
			Host:         "api-1",
			Level:        "error",
			TraceID:      "trace-123",
			Fingerprint:  "0123456789abcdef",
			Message:      "database timeout",
			Fields:       map[string]any{"region": "us-west-2", "attempt": 1},
			IngestID:     "request-123",
			RawSizeBytes: 256,
		}
	}
	handler := NewHandler(benchmarkLogStore{records: records})
	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/logs?start=2026-07-24T11:00:00Z&end=2026-07-24T13:00:00Z&service=checkout&level=error&page_size=100",
		nil,
	).WithContext(WithTenantID(context.Background(), 42))

	b.ReportAllocs()
	b.SetBytes(int64(len(records)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request.Clone(request.Context()))
		if recorder.Code != http.StatusOK {
			b.Fatalf("unexpected status %d", recorder.Code)
		}
	}
}

func BenchmarkBuildLogsQuery(b *testing.B) {
	filter := QueryFilter{
		TenantID: 42,
		Start:    time.Date(2026, 7, 24, 11, 0, 0, 0, time.UTC),
		End:      time.Date(2026, 7, 24, 13, 0, 0, 0, time.UTC),
		Service:  "checkout",
		Level:    "error",
		TraceID:  "trace-123",
		Limit:    100,
		Offset:   1000,
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = buildLogsQuery(filter)
	}
}

type benchmarkLogStore struct {
	records []LogRecord
}

func (s benchmarkLogStore) QueryLogs(context.Context, QueryFilter) ([]LogRecord, error) {
	return s.records, nil
}

func (s benchmarkLogStore) StreamLogs(_ context.Context, _ QueryFilter, emit func(LogRecord) error) error {
	for _, record := range s.records {
		if err := emit(record); err != nil {
			return err
		}
	}
	return nil
}
