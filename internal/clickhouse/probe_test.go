package clickhouse

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestProbeChecksClickHouseWithSelectOne(t *testing.T) {
	t.Parallel()

	var body string
	err := Probe(context.Background(), "http://clickhouse.local", &http.Client{
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			body = string(payload)
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"data":[{"1":1}]}`)),
			}, nil
		}),
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if body != "SELECT 1 FORMAT JSON" {
		t.Fatalf("unexpected probe body: %q", body)
	}
}

func TestProbeReturnsServerError(t *testing.T) {
	t.Parallel()

	err := Probe(context.Background(), "http://clickhouse.local", &http.Client{
		Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusBadRequest,
				Status:     "400 Bad Request",
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("bad query")),
			}, nil
		}),
	})
	if err == nil || !strings.Contains(err.Error(), "clickhouse returned") {
		t.Fatalf("expected clickhouse probe error, got %v", err)
	}
}

func TestFormatDateTime64UsesClickHouseCompatibleUTC(t *testing.T) {
	value := time.Date(2026, 7, 30, 14, 32, 1, 234567890, time.FixedZone("offset", -7*60*60))
	if got := FormatDateTime64(value); got != "2026-07-30 21:32:01.234" {
		t.Fatalf("got %q", got)
	}
}

func TestParseDateTime64AcceptsClickHouseAndRFC3339(t *testing.T) {
	for _, value := range []string{"2026-07-30 21:32:01.234", "2026-07-30T21:32:01.234Z"} {
		parsed, err := ParseDateTime64(value)
		if err != nil || parsed.Format(time.RFC3339Nano) != "2026-07-30T21:32:01.234Z" {
			t.Fatalf("parse %q: time=%s err=%v", value, parsed, err)
		}
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}
