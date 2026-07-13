package processor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
)

func TestHandleBatchAcceptsPublishedBatch(t *testing.T) {
	logger := &stubLogger{}
	writer := &stubLogWriter{}
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		ReceivedAt:    "2026-07-09T20:12:07Z",
		TenantID:      42,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []contracts.LogsRawRecord{
			{
				Timestamp: "2026-07-07T16:00:00Z",
				Level:     "error",
				Message:   "database timeout",
			},
		},
	}

	if err := handleBatch(context.Background(), logger, writer, batch); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if logger.infoCalls != 1 {
		t.Fatalf("expected one info log call, got %d", logger.infoCalls)
	}
	if writer.calls != 1 {
		t.Fatalf("expected one writer call, got %d", writer.calls)
	}
	if len(writer.lastBatch) != 1 || writer.lastBatch[0].TenantID != 42 {
		t.Fatalf("expected tenant id 42 in written records, got %+v", writer.lastBatch)
	}
}

func TestHandleBatchRejectsMissingRequestID(t *testing.T) {
	logger := &stubLogger{}
	writer := &stubLogWriter{}

	err := handleBatch(context.Background(), logger, writer, contracts.LogsRawEvent{})
	if err == nil {
		t.Fatal("expected error for missing request id")
	}
	if logger.infoCalls != 0 {
		t.Fatalf("expected no info logs, got %d", logger.infoCalls)
	}
	if writer.calls != 0 {
		t.Fatalf("expected no writer calls, got %d", writer.calls)
	}
}

func TestHandleBatchReturnsWriterError(t *testing.T) {
	logger := &stubLogger{}
	writer := &stubLogWriter{err: errors.New("clickhouse unavailable")}
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		ReceivedAt:    "2026-07-09T20:12:07Z",
		TenantID:      42,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []contracts.LogsRawRecord{
			{
				Timestamp: "2026-07-07T16:00:00Z",
				Level:     "error",
				Message:   "database timeout",
			},
		},
	}

	err := handleBatch(context.Background(), logger, writer, batch)
	if err == nil || !strings.Contains(err.Error(), "persist normalized logs") {
		t.Fatalf("expected persist error, got %v", err)
	}
	if logger.infoCalls != 0 {
		t.Fatalf("expected no info logs when write fails, got %d", logger.infoCalls)
	}
}

func TestNormalizeBatchNormalizesRecords(t *testing.T) {
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		ReceivedAt:    "2026-07-09T20:12:07.612769-07:00",
		TenantID:      42,
		Service:       " Checkout ",
		Env:           " PROD ",
		Source:        " App ",
		Logs: []contracts.LogsRawRecord{
			{
				Timestamp: "2026-07-07T16:00:00-07:00",
				Level:     " WARNING ",
				Message:   " database timeout ",
				Fields: map[string]any{
					"trace_id": "trace-123",
					"host":     "api-1",
					"region":   "us-west-2",
					"attempt":  float64(3),
				},
			},
		},
	}

	records, err := normalizeBatch(batch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected one record, got %d", len(records))
	}

	record := records[0]
	expectedTimestamp := time.Date(2026, 7, 7, 23, 0, 0, 0, time.UTC)
	if !record.Timestamp.Equal(expectedTimestamp) {
		t.Fatalf("expected timestamp %s, got %s", expectedTimestamp, record.Timestamp)
	}
	if record.TenantID != 42 {
		t.Fatalf("expected tenant id 42, got %d", record.TenantID)
	}
	if record.Service != "checkout" {
		t.Fatalf("expected service checkout, got %q", record.Service)
	}
	if record.Environment != "prod" {
		t.Fatalf("expected environment prod, got %q", record.Environment)
	}
	if record.Source != "app" {
		t.Fatalf("expected source app, got %q", record.Source)
	}
	if record.Level != "warn" {
		t.Fatalf("expected level warn, got %q", record.Level)
	}
	if record.Host != "api-1" {
		t.Fatalf("expected host api-1, got %q", record.Host)
	}
	if record.TraceID != "trace-123" {
		t.Fatalf("expected trace id trace-123, got %q", record.TraceID)
	}
	if record.Message != "database timeout" {
		t.Fatalf("expected trimmed message, got %q", record.Message)
	}
	if record.FieldsJSON != `{"attempt":3,"region":"us-west-2"}` {
		t.Fatalf("unexpected fields json: %s", record.FieldsJSON)
	}
	if record.IngestID != "req-123" {
		t.Fatalf("expected ingest id req-123, got %q", record.IngestID)
	}
	if record.RawSizeBytes == 0 {
		t.Fatal("expected raw_size_bytes to be populated")
	}
	if len(record.Fingerprint) != 32 {
		t.Fatalf("expected 32 hex chars in fingerprint, got %q", record.Fingerprint)
	}
}

func TestNormalizeBatchFallsBackToReceivedAtForMissingTimestamp(t *testing.T) {
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		ReceivedAt:    "2026-07-09T20:12:07.612769Z",
		TenantID:      42,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []contracts.LogsRawRecord{
			{
				Level:   "error",
				Message: "database timeout",
			},
		},
	}

	records, err := normalizeBatch(batch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if got := records[0].Timestamp.Format(time.RFC3339Nano); got != "2026-07-09T20:12:07.612769Z" {
		t.Fatalf("expected fallback timestamp from received_at, got %s", got)
	}
}

func TestNormalizeBatchRejectsInvalidRecordTimestamp(t *testing.T) {
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		ReceivedAt:    "2026-07-09T20:12:07Z",
		TenantID:      42,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []contracts.LogsRawRecord{
			{
				Timestamp: "yesterday",
				Level:     "error",
				Message:   "database timeout",
			},
		},
	}

	_, err := normalizeBatch(batch)
	if err == nil {
		t.Fatal("expected invalid timestamp error")
	}
	if !strings.Contains(err.Error(), "parse log timestamp") {
		t.Fatalf("expected parse log timestamp error, got %v", err)
	}
}

func TestNormalizeBatchProducesStableFingerprint(t *testing.T) {
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		ReceivedAt:    "2026-07-09T20:12:07Z",
		TenantID:      42,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []contracts.LogsRawRecord{
			{
				Timestamp: "2026-07-07T16:00:00Z",
				Level:     "ERROR",
				Message:   "database timeout",
				Fields: map[string]any{
					"b": "two",
					"a": "one",
				},
			},
			{
				Timestamp: "2026-07-08T16:00:00Z",
				Level:     "error",
				Message:   "database timeout",
				Fields: map[string]any{
					"a": "one",
					"b": "two",
				},
			},
		},
	}

	records, err := normalizeBatch(batch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if records[0].Fingerprint != records[1].Fingerprint {
		t.Fatalf("expected stable fingerprints, got %q and %q", records[0].Fingerprint, records[1].Fingerprint)
	}
}

type stubLogger struct {
	infoCalls int
}

func (l *stubLogger) Info(_ string, _ ...any) {
	l.infoCalls++
}

func (l *stubLogger) Error(_ string, _ ...any) {}

type stubLogWriter struct {
	lastBatch []NormalizedLogRecord
	calls     int
	err       error
}

func (w *stubLogWriter) WriteBatch(_ context.Context, batch []NormalizedLogRecord) error {
	w.calls++
	w.lastBatch = append([]NormalizedLogRecord(nil), batch...)
	return w.err
}
