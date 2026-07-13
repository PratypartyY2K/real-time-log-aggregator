package queryapi

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestClickHouseStoreQueriesLogs(t *testing.T) {
	var requestBody string
	store := &ClickHouseStore{
		url: "http://clickhouse.local",
		client: &http.Client{
			Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				payload, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatalf("read request body: %v", err)
				}
				requestBody = string(payload)
				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(`{"data":[{"timestamp":"2026-07-13T18:00:00Z","tenant_id":1,"service":"checkout","environment":"prod","source":"app","host":"api-1","level":"error","trace_id":"trace-123","fingerprint":"abc123","message":"database timeout","fields_json":"{\"region\":\"us-west-2\"}","ingest_id":"req-123","raw_size_bytes":128}]}`)),
				}, nil
			}),
		},
	}

	logs, err := store.QueryLogs(context.Background(), QueryFilter{
		Start:   time.Date(2026, 7, 13, 17, 0, 0, 0, time.UTC),
		End:     time.Date(2026, 7, 13, 19, 0, 0, 0, time.UTC),
		Service: "checkout",
		Level:   "error",
		Limit:   25,
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !strings.Contains(requestBody, "AND service = 'checkout'") {
		t.Fatalf("expected service filter in query, got %q", requestBody)
	}
	if !strings.Contains(requestBody, "AND level = 'error'") {
		t.Fatalf("expected level filter in query, got %q", requestBody)
	}
	if !strings.Contains(requestBody, "LIMIT 25") {
		t.Fatalf("expected limit in query, got %q", requestBody)
	}
	if len(logs) != 1 || logs[0].Fields["region"] != "us-west-2" {
		t.Fatalf("unexpected logs result: %+v", logs)
	}
}

func TestClickHouseStoreReturnsServerError(t *testing.T) {
	store := &ClickHouseStore{
		url: "http://clickhouse.local",
		client: &http.Client{
			Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Status:     "400 Bad Request",
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("syntax error")),
				}, nil
			}),
		},
	}

	_, err := store.QueryLogs(context.Background(), QueryFilter{
		Start: time.Date(2026, 7, 13, 17, 0, 0, 0, time.UTC),
		End:   time.Date(2026, 7, 13, 19, 0, 0, 0, time.UTC),
		Limit: 10,
	})
	if err == nil || !strings.Contains(err.Error(), "clickhouse query failed") {
		t.Fatalf("expected clickhouse query error, got %v", err)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}
