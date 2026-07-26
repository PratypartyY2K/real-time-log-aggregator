package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
)

func TestNewWritesJSONLogs(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	logger := NewWithWriter("info", &out)
	logger.Info("service started", "service", "query-api", "operation", "startup")

	var entry map[string]any
	if err := json.Unmarshal(out.Bytes(), &entry); err != nil {
		t.Fatalf("decode json log: %v\n%s", err, out.String())
	}
	for _, key := range []string{"time", "level", "msg", "service", "operation"} {
		if _, ok := entry[key]; !ok {
			t.Fatalf("expected key %q in log entry: %#v", key, entry)
		}
	}
	if entry["msg"] != "service started" || entry["service"] != "query-api" {
		t.Fatalf("unexpected log entry: %#v", entry)
	}
}

func TestJSONLoggerIncludesRequestAndErrorFields(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&out, nil))
	logger.Error(
		"processor failed to handle batch",
		"service", "processor",
		"operation", "process_batch",
		"request_id", "req-123",
		"error", errors.New("clickhouse unavailable"),
	)

	var entry map[string]any
	if err := json.Unmarshal(out.Bytes(), &entry); err != nil {
		t.Fatalf("decode json log: %v\n%s", err, out.String())
	}
	if entry["request_id"] != "req-123" || entry["operation"] != "process_batch" || entry["error"] != "clickhouse unavailable" {
		t.Fatalf("missing structured request/error fields: %#v", entry)
	}
}
