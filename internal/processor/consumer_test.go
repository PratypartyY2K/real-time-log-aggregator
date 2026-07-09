package processor

import (
	"context"
	"testing"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
)

func TestHandleBatchAcceptsPublishedBatch(t *testing.T) {
	logger := &stubLogger{}
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		ReceivedAt:    "2026-07-09T20:12:07Z",
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

	if err := handleBatch(context.Background(), logger, batch); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if logger.infoCalls != 1 {
		t.Fatalf("expected one info log call, got %d", logger.infoCalls)
	}
}

func TestHandleBatchRejectsMissingRequestID(t *testing.T) {
	logger := &stubLogger{}

	err := handleBatch(context.Background(), logger, contracts.LogsRawEvent{})
	if err == nil {
		t.Fatal("expected error for missing request id")
	}
	if logger.infoCalls != 0 {
		t.Fatalf("expected no info logs, got %d", logger.infoCalls)
	}
}

type stubLogger struct {
	infoCalls int
}

func (l *stubLogger) Info(_ string, _ ...any) {
	l.infoCalls++
}

func (l *stubLogger) Error(_ string, _ ...any) {}
