package queryapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestClickHouseStoreReportsUnavailableShards(t *testing.T) {
	store := &ClickHouseStore{
		shardURLs: []string{"http://shard-1.local", "http://shard-2.local"},
		client: &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Host == "shard-2.local" {
				return nil, errors.New("connection refused")
			}
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(strings.NewReader("1\n"))}, nil
		})},
	}

	status := store.ClusterStatus(context.Background())

	if !status.Partial || len(status.UnavailableShards) != 1 || status.UnavailableShards[0] != "shard-2" {
		t.Fatalf("unexpected cluster status: %+v", status)
	}
}

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
		TenantID: 1,
		Start:    time.Date(2026, 7, 13, 17, 0, 0, 0, time.UTC),
		End:      time.Date(2026, 7, 13, 19, 0, 0, 0, time.UTC),
		Service:  "checkout",
		Level:    "error",
		TraceID:  "trace-123",
		Limit:    25,
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !strings.Contains(requestBody, "AND service = 'checkout'") {
		t.Fatalf("expected service filter in query, got %q", requestBody)
	}
	if !strings.Contains(requestBody, "WHERE tenant_id = 1 AND timestamp >=") {
		t.Fatalf("expected tenant predicate in query, got %q", requestBody)
	}
	if !strings.Contains(requestBody, "AND level = 'error'") {
		t.Fatalf("expected level filter in query, got %q", requestBody)
	}
	if !strings.Contains(requestBody, "AND trace_id = 'trace-123'") {
		t.Fatalf("expected trace filter in query, got %q", requestBody)
	}
	if !strings.Contains(requestBody, "LIMIT 25") {
		t.Fatalf("expected limit in query, got %q", requestBody)
	}
	if len(logs) != 1 || logs[0].Fields["region"] != "us-west-2" {
		t.Fatalf("unexpected logs result: %+v", logs)
	}
}

func TestClickHouseStoreQueriesGraphRecords(t *testing.T) {
	var requestBody string
	store := &ClickHouseStore{url: "http://clickhouse.local", client: &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		payload, _ := io.ReadAll(r.Body)
		requestBody = string(payload)
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"data":[{"timestamp":"2026-07-22T10:00:00Z","service":"gateway","level":"error","trace_id":"trace-1","fields_json":"{\"session_id\":\"session-1\"}","ingest_id":"ingest-1"}]}`))}, nil
	})}}
	records, err := store.QueryGraphRecords(context.Background(), GraphQuery{TenantID: 7, Start: time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC), End: time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC), SessionID: "session-1", UserID: "user@example.com", Limit: 5000})
	if err != nil {
		t.Fatalf("query graph records: %v", err)
	}
	if len(records) != 1 || records[0].Fields["session_id"] != "session-1" {
		t.Fatalf("unexpected records: %+v", records)
	}
	if !strings.Contains(requestBody, "JSONExtractString(fields_json, 'session_id') = 'session-1'") || !strings.Contains(requestBody, "JSONExtractString(fields_json, 'user_id') = 'user@example.com'") {
		t.Fatalf("expected graph filters, got %q", requestBody)
	}
}

func TestClickHouseStoreStreamsLogs(t *testing.T) {
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
					Body: io.NopCloser(strings.NewReader(
						`{"timestamp":"2026-07-13T18:00:00Z","tenant_id":1,"service":"checkout","environment":"prod","source":"app","host":"api-1","level":"error","trace_id":"trace-123","fingerprint":"abc123","message":"database timeout","fields_json":"{}","ingest_id":"req-123","raw_size_bytes":128}` + "\n" +
							`{"timestamp":"2026-07-13T18:01:00Z","tenant_id":1,"service":"billing","environment":"prod","source":"app","host":"api-2","level":"info","trace_id":"trace-456","fingerprint":"def456","message":"ok","fields_json":"{}","ingest_id":"req-456","raw_size_bytes":64}` + "\n",
					)),
				}, nil
			}),
		},
	}

	var logs []LogRecord
	err := store.StreamLogs(context.Background(), QueryFilter{
		TenantID: 1,
		Start:    time.Date(2026, 7, 13, 17, 0, 0, 0, time.UTC),
		End:      time.Date(2026, 7, 13, 19, 0, 0, 0, time.UTC),
		Limit:    2,
		Offset:   10,
	}, func(record LogRecord) error {
		logs = append(logs, record)
		return nil
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !strings.Contains(requestBody, "LIMIT 2 OFFSET 10 SETTINGS optimize_skip_unused_shards = 1, skip_unavailable_shards = 1, output_format_json_quote_64bit_integers = 0 FORMAT JSONEachRow") {
		t.Fatalf("expected streaming limit and offset, got %q", requestBody)
	}
	if len(logs) != 2 || logs[0].Service != "checkout" || logs[1].Service != "billing" {
		t.Fatalf("unexpected streamed logs: %+v", logs)
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
		TenantID: 1,
		Start:    time.Date(2026, 7, 13, 17, 0, 0, 0, time.UTC),
		End:      time.Date(2026, 7, 13, 19, 0, 0, 0, time.UTC),
		Limit:    10,
	})
	if err == nil || !strings.Contains(err.Error(), "clickhouse returned") {
		t.Fatalf("expected clickhouse query error, got %v", err)
	}
}

func TestClickHouseStoreQueriesAnalytics(t *testing.T) {
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
					Body:       io.NopCloser(strings.NewReader(`{"data":[{"bucket":"2026-07-13T18:00:00Z","group_service":"checkout","group_error_code":"db_timeout","value":12}]}`)),
				}, nil
			}),
		},
	}

	results, err := store.QueryAnalytics(context.Background(), AnalyticsQuery{
		TenantID:    1,
		Start:       time.Date(2026, 7, 13, 17, 0, 0, 0, time.UTC),
		End:         time.Date(2026, 7, 13, 19, 0, 0, 0, time.UTC),
		Aggregation: "count",
		GroupBy:     []string{"service", "error_code"},
		Bucket:      "minute",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !strings.Contains(requestBody, "GROUP BY toStartOfMinute(timestamp), service, JSONExtractString(fields_json, 'error_code')") {
		t.Fatalf("expected grouped analytics query, got %q", requestBody)
	}
	if !strings.Contains(requestBody, "WHERE tenant_id = 1 AND timestamp >=") {
		t.Fatalf("expected tenant predicate in analytics query, got %q", requestBody)
	}
	if !strings.Contains(requestBody, "formatDateTime(toStartOfMinute(timestamp)") {
		t.Fatalf("expected bucket selection in query, got %q", requestBody)
	}
	if !strings.Contains(requestBody, "LIMIT 100") {
		t.Fatalf("expected default analytics limit, got %q", requestBody)
	}
	if len(results) != 1 || results[0].Group["service"] != "checkout" || results[0].Group["error_code"] != "db_timeout" {
		t.Fatalf("unexpected analytics result: %+v", results)
	}
}

func TestClickHouseStoreBuildsTopKPercentileQuery(t *testing.T) {
	query := buildAnalyticsQuery(AnalyticsQuery{
		TenantID:    1,
		Start:       time.Date(2026, 7, 13, 17, 0, 0, 0, time.UTC),
		End:         time.Date(2026, 7, 13, 19, 0, 0, 0, time.UTC),
		Aggregation: "percentile",
		Percentile:  95,
		ValueField:  "field.duration_ms",
		GroupBy:     []string{"service"},
		TopK:        5,
	})

	if !strings.Contains(query, "quantileTDigest(0.95)(toFloat64OrNull(JSONExtractString(fields_json, 'duration_ms')))) AS value") {
		t.Fatalf("expected percentile expression, got %q", query)
	}
	if !strings.Contains(query, "ORDER BY value DESC LIMIT 5") {
		t.Fatalf("expected top-k ordering, got %q", query)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}
