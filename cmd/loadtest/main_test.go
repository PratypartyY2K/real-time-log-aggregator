package main

import (
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRunBurstSendsUniqueSuccessfulRequests(t *testing.T) {
	transport := &recordingTransport{}
	client := &http.Client{Transport: transport}
	results, err := runBurst(client, "http://ingest.test/v1/logs", "key", requestPayload{
		SchemaVersion: "logs.ingest.v1",
		Service:       "checkout",
		Env:           "prod",
		Source:        "loadtest",
	}, 2, 0, 8, 3, "")
	if err != nil {
		t.Fatalf("run burst: %v", err)
	}

	stats := summarize(results, time.Second)
	if stats.requests != 8 || stats.successes != 8 || stats.failures != 0 {
		t.Fatalf("unexpected summary: %+v", stats)
	}
	if transport.uniquePayloads() != 8 {
		t.Fatalf("expected 8 unique payloads, got %d", transport.uniquePayloads())
	}
	if transport.maxInFlight > 3 {
		t.Fatalf("expected concurrency cap 3, got %d", transport.maxInFlight)
	}
}

func TestSummarizeTreatsNon2xxAsFailures(t *testing.T) {
	stats := summarize([]result{
		{statusCode: http.StatusAccepted, duration: time.Millisecond},
		{statusCode: http.StatusTooManyRequests, duration: 2 * time.Millisecond},
		{err: io.ErrUnexpectedEOF},
	}, time.Second)

	if stats.successes != 1 || stats.failures != 2 {
		t.Fatalf("unexpected summary: %+v", stats)
	}
	if got := errorRate(stats); got < 0.66 || got > 0.67 {
		t.Fatalf("expected error rate near 0.667, got %f", got)
	}
}

func TestRunSustainedSendsRequestsForDuration(t *testing.T) {
	transport := &recordingTransport{}
	results, err := runSustained(
		&http.Client{Transport: transport},
		"http://ingest.test/v1/logs",
		"key",
		requestPayload{SchemaVersion: "logs.ingest.v1", Service: "checkout", Env: "prod", Source: "loadtest"},
		1,
		25*time.Millisecond,
		500,
		4,
		"",
	)
	if err != nil {
		t.Fatalf("run sustained: %v", err)
	}
	if len(results) < 5 {
		t.Fatalf("expected sustained profile to send multiple requests, got %d", len(results))
	}
	if stats := summarize(results, 25*time.Millisecond); stats.failures != 0 {
		t.Fatalf("unexpected sustained failures: %+v", stats)
	}
}

func TestBuildLogsCanIncludeStableAlertErrorCode(t *testing.T) {
	logs := buildLogs(2, 3, "PAYMENT_TIMEOUT")
	if len(logs) != 2 {
		t.Fatalf("expected two logs, got %d", len(logs))
	}
	for _, log := range logs {
		if log.Level != "error" || log.Fields["error_code"] != "PAYMENT_TIMEOUT" {
			t.Fatalf("expected alert-oriented error log, got %+v", log)
		}
	}
	if logs[0].Fields["trace_id"] == logs[1].Fields["trace_id"] {
		t.Fatalf("expected per-record trace ids to stay unique, got %+v", logs)
	}
}

type recordingTransport struct {
	mu          sync.Mutex
	payloads    map[string]struct{}
	inFlight    int
	maxInFlight int
}

func (t *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	payload, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	if t.payloads == nil {
		t.payloads = make(map[string]struct{})
	}
	t.payloads[string(payload)] = struct{}{}
	t.inFlight++
	if t.inFlight > t.maxInFlight {
		t.maxInFlight = t.inFlight
	}
	t.mu.Unlock()

	time.Sleep(time.Millisecond)

	t.mu.Lock()
	t.inFlight--
	t.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusAccepted,
		Body:       io.NopCloser(strings.NewReader(`{"status":"queued"}`)),
		Header:     make(http.Header),
	}, nil
}

func (t *recordingTransport) uniquePayloads() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.payloads)
}
