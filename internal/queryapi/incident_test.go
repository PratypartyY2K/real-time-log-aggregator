package queryapi

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestIncidentSummaryReadsOutputText(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Fatal("missing authorization")
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"output":[{"content":[{"type":"output_text","text":"pool exhausted"}]}]}`))}, nil
	})}

	handler := NewIncidentHandler(nil, nil, "test-key", "test-model")
	handler.client = client
	answer, err := handler.summarize(context.Background(), "why?", []LogRecord{{Message: "database connection pool exhausted"}})
	if err != nil || answer != "pool exhausted" {
		t.Fatalf("answer=%q err=%v", answer, err)
	}
}

func TestIncidentQueryUsesGlobalINForDistributedLogs(t *testing.T) {
	store := NewClickHouseStore("http://clickhouse.test")
	store.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		query, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(query), "trace_id GLOBAL IN") {
			t.Fatalf("query must use GLOBAL IN: %s", query)
		}
		if !strings.Contains(string(query), "output_format_json_quote_64bit_integers = 0") {
			t.Fatalf("query must emit numeric tenant IDs: %s", query)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"data":[]}`))}, nil
	})}
	if _, err := store.QueryIncidentLogs(context.Background(), 1, []string{"template-1"}); err != nil {
		t.Fatal(err)
	}
}
