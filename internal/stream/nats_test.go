package stream

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
)

func TestConsumeMessageAcksSuccessfulHandler(t *testing.T) {
	msg := &stubConsumableMessage{
		payload: []byte(`{"schema_version":"logs.raw.v1","request_id":"req-123","received_at":"2026-07-09T20:12:07Z","tenant_id":1,"service":"checkout","env":"prod","source":"app","logs":[{"timestamp":"2026-07-07T16:00:00Z","level":"error","message":"database timeout"}]}`),
	}

	var handled contracts.LogsRawEvent
	err := consumeMessage(context.Background(), msg, func(_ context.Context, event contracts.LogsRawEvent) error {
		handled = event
		return nil
	}, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if handled.RequestID != "req-123" {
		t.Fatalf("expected handler to receive request id req-123, got %q", handled.RequestID)
	}
	if msg.ackCalls != 1 {
		t.Fatalf("expected one ack, got %d", msg.ackCalls)
	}
	if msg.nakCalls != 0 {
		t.Fatalf("expected no nak, got %d", msg.nakCalls)
	}
	if msg.termCalls != 0 {
		t.Fatalf("expected no term, got %d", msg.termCalls)
	}
}

func TestConsumeMessageNaksRetryableHandlerError(t *testing.T) {
	msg := &stubConsumableMessage{
		payload: []byte(`{"schema_version":"logs.raw.v1","request_id":"req-123","received_at":"2026-07-09T20:12:07Z","tenant_id":1,"service":"checkout","env":"prod","source":"app","logs":[{"timestamp":"2026-07-07T16:00:00Z","level":"error","message":"database timeout"}]}`),
	}

	var reported error
	err := consumeMessage(context.Background(), msg, func(context.Context, contracts.LogsRawEvent) error {
		return errors.New("clickhouse unavailable")
	}, func(_ context.Context, consumeErr error) {
		reported = consumeErr
	})
	if err != nil {
		t.Fatalf("expected loop to continue after handler error, got %v", err)
	}
	if msg.ackCalls != 0 {
		t.Fatalf("expected no ack, got %d", msg.ackCalls)
	}
	if msg.nakCalls != 1 {
		t.Fatalf("expected one nak, got %d", msg.nakCalls)
	}
	if msg.nakDelay != consumerRetryDelay {
		t.Fatalf("expected retry delay %s, got %s", consumerRetryDelay, msg.nakDelay)
	}
	if reported == nil || !strings.Contains(reported.Error(), "clickhouse unavailable") {
		t.Fatalf("expected reported handler error, got %v", reported)
	}
}

func TestConsumeMessageTermsMalformedPayload(t *testing.T) {
	msg := &stubConsumableMessage{payload: []byte(`{not-json`)}

	var reported error
	err := consumeMessage(context.Background(), msg, func(context.Context, contracts.LogsRawEvent) error {
		t.Fatal("handler should not be called for malformed payload")
		return nil
	}, func(_ context.Context, consumeErr error) {
		reported = consumeErr
	})
	if err != nil {
		t.Fatalf("expected malformed payload to be terminated without stopping loop, got %v", err)
	}
	if msg.termCalls != 1 {
		t.Fatalf("expected one term, got %d", msg.termCalls)
	}
	if msg.ackCalls != 0 {
		t.Fatalf("expected no ack, got %d", msg.ackCalls)
	}
	if msg.nakCalls != 0 {
		t.Fatalf("expected no nak, got %d", msg.nakCalls)
	}
	if reported == nil || !strings.Contains(reported.Error(), "decode message") {
		t.Fatalf("expected decode error to be reported, got %v", reported)
	}
}

type stubConsumableMessage struct {
	payload   []byte
	ackErr    error
	nakErr    error
	termErr   error
	ackCalls  int
	nakCalls  int
	termCalls int
	nakDelay  time.Duration
}

func (m *stubConsumableMessage) Payload() []byte {
	return m.payload
}

func (m *stubConsumableMessage) Ack() error {
	m.ackCalls++
	return m.ackErr
}

func (m *stubConsumableMessage) NakWithDelay(delay time.Duration) error {
	m.nakCalls++
	m.nakDelay = delay
	return m.nakErr
}

func (m *stubConsumableMessage) Term() error {
	m.termCalls++
	return m.termErr
}
