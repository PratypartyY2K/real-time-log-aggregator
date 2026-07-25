package main

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestIngestCommandSendsDataset(t *testing.T) {
	transport := &cliTransport{response: `{"accepted":2,"status":"queued"}`}
	var output bytes.Buffer
	err := run([]string{"ingest", "-url", "http://logagg.test/v1/logs", "-api-key", "secret"},
		strings.NewReader(`{"logs":[{},{}]}`), &output, &http.Client{Transport: transport})
	if err != nil {
		t.Fatalf("run ingest: %v", err)
	}
	if transport.method != http.MethodPost || transport.apiKey != "secret" || !strings.Contains(transport.body, `"logs"`) {
		t.Fatalf("unexpected request: %+v", transport)
	}
	if output.String() != transport.response {
		t.Fatalf("unexpected output %q", output.String())
	}
}

func TestQueryCommandBuildsFilters(t *testing.T) {
	transport := &cliTransport{response: `{"count":0,"logs":[]}`}
	err := run([]string{
		"query", "-url", "http://logagg.test/v1/logs", "-api-key", "secret",
		"-start", "2026-07-25T10:00:00Z", "-end", "2026-07-25T11:00:00Z",
		"-service", "checkout", "-level", "error", "-trace-id", "trace-1", "-limit", "25",
	}, strings.NewReader(""), io.Discard, &http.Client{Transport: transport})
	if err != nil {
		t.Fatalf("run query: %v", err)
	}
	query := transport.request.URL.Query()
	if query.Get("service") != "checkout" || query.Get("level") != "error" || query.Get("trace_id") != "trace-1" || query.Get("limit") != "25" {
		t.Fatalf("unexpected query filters: %s", transport.request.URL.RawQuery)
	}
}

type cliTransport struct {
	response string
	request  *http.Request
	method   string
	apiKey   string
	body     string
}

func (t *cliTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
	}
	t.request = req
	t.method = req.Method
	t.apiKey = req.Header.Get("X-API-Key")
	t.body = string(body)
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(t.response)),
	}, nil
}
