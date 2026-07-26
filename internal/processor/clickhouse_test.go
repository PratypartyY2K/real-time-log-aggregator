package processor

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestClickHouseWriterWritesJSONEachRowBatch(t *testing.T) {
	var (
		method      string
		contentType string
		body        string
	)

	writer := &ClickHouseWriter{
		url: "http://clickhouse.local",
		client: &http.Client{
			Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				method = r.Method
				contentType = r.Header.Get("Content-Type")
				payload, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatalf("read request body: %v", err)
				}
				body = string(payload)
				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Body:       io.NopCloser(strings.NewReader("")),
					Header:     make(http.Header),
				}, nil
			}),
		},
	}
	err := writer.WriteBatch(context.Background(), []NormalizedLogRecord{
		{
			Timestamp:    time.Date(2026, 7, 7, 23, 0, 0, 123000000, time.UTC),
			TenantID:     42,
			Service:      "checkout",
			Environment:  "prod",
			Source:       "app",
			Host:         "api-1",
			Level:        "error",
			TraceID:      "trace-123",
			Fingerprint:  "abc123",
			Message:      "database timeout",
			FieldsJSON:   `{"region":"us-west-2"}`,
			IngestID:     "req-123",
			RawSizeBytes: 123,
		},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if method != http.MethodPost {
		t.Fatalf("expected POST, got %s", method)
	}
	if contentType != "text/plain; charset=utf-8" {
		t.Fatalf("unexpected content type: %s", contentType)
	}
	if !strings.HasPrefix(body, "INSERT INTO logs SETTINGS insert_distributed_sync = 1 FORMAT JSONEachRow\n") {
		t.Fatalf("expected insert statement prefix, got %q", body)
	}
	if !strings.Contains(body, `"tenant_id":42`) {
		t.Fatalf("expected tenant_id in payload, got %q", body)
	}
	if !strings.Contains(body, `"environment":"prod"`) {
		t.Fatalf("expected environment in payload, got %q", body)
	}
	if !strings.Contains(body, `"fields_json":"{\"region\":\"us-west-2\"}"`) {
		t.Fatalf("expected fields_json in payload, got %q", body)
	}
}

func TestClickHouseWriterReturnsServerError(t *testing.T) {
	metrics := NewMetrics("processor")
	writer := &ClickHouseWriter{
		url:     "http://clickhouse.local",
		metrics: metrics,
		client: &http.Client{
			Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Status:     "400 Bad Request",
					Body:       io.NopCloser(strings.NewReader("syntax error")),
					Header:     make(http.Header),
				}, nil
			}),
		},
	}
	err := writer.WriteBatch(context.Background(), []NormalizedLogRecord{
		{
			Timestamp: time.Now().UTC(),
			TenantID:  42,
			Service:   "checkout",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "clickhouse insert failed") {
		t.Fatalf("expected clickhouse insert error, got %v", err)
	}

	var body strings.Builder
	metrics.WritePrometheus(&body)
	if !containsMetric(body.String(), "logagg_clickhouse_write_errors_total", "1", `result="error"`, `operation="write"`) {
		t.Fatalf("expected clickhouse write error metric, got %s", body.String())
	}
}

func TestClickHouseWriterScopesReplayCheckToTenantShard(t *testing.T) {
	var body string
	writer := &ClickHouseWriter{
		url: "http://clickhouse.local",
		client: &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read request: %v", err)
			}
			body = string(payload)
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
		})},
	}

	processed, err := writer.AlreadyProcessed(context.Background(), 42, "req-123")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if processed {
		t.Fatal("expected batch not to be processed")
	}
	if !strings.Contains(body, "WHERE tenant_id = 42 AND ingest_id = 'req-123'") || !strings.Contains(body, "optimize_skip_unused_shards = 1") {
		t.Fatalf("expected tenant-prunable replay query, got %q", body)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}
