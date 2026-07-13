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
		payload:       []byte(`{"schema_version":"logs.raw.v1","request_id":"req-123","received_at":"2026-07-09T20:12:07Z","tenant_id":1,"service":"checkout","env":"prod","source":"app","logs":[{"timestamp":"2026-07-07T16:00:00Z","level":"error","message":"database timeout"}]}`),
		deliveryCount: 1,
	}

	var handled contracts.LogsRawEvent
	err := consumeMessage(context.Background(), msg, nil, 5, func(_ context.Context, event contracts.LogsRawEvent) error {
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
		payload:       []byte(`{"schema_version":"logs.raw.v1","request_id":"req-123","received_at":"2026-07-09T20:12:07Z","tenant_id":1,"service":"checkout","env":"prod","source":"app","logs":[{"timestamp":"2026-07-07T16:00:00Z","level":"error","message":"database timeout"}]}`),
		deliveryCount: 1,
	}

	var reported error
	err := consumeMessage(context.Background(), msg, nil, 5, func(context.Context, contracts.LogsRawEvent) error {
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
	dlq := &stubDLQPublisher{}
	msg := &stubConsumableMessage{payload: []byte(`{not-json`), deliveryCount: 1}

	var reported error
	err := consumeMessage(context.Background(), msg, dlq, 5, func(context.Context, contracts.LogsRawEvent) error {
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
	if len(dlq.events) != 1 || dlq.events[0].Reason != "malformed_payload" {
		t.Fatalf("expected malformed_payload dlq event, got %#v", dlq.events)
	}
}

func TestConsumeMessageTermsPoisonBatchToDLQ(t *testing.T) {
	dlq := &stubDLQPublisher{}
	msg := &stubConsumableMessage{
		payload:       []byte(`{"schema_version":"logs.raw.v1","request_id":"req-123","received_at":"2026-07-09T20:12:07Z","tenant_id":1,"service":"checkout","env":"prod","source":"app","logs":[{"timestamp":"2026-07-07T16:00:00Z","level":"error","message":"database timeout"}]}`),
		deliveryCount: 1,
	}

	err := consumeMessage(context.Background(), msg, dlq, 5, func(context.Context, contracts.LogsRawEvent) error {
		return MarkPoisonBatch(errors.New("invalid logs.raw event"))
	}, nil)
	if err != nil {
		t.Fatalf("expected poison batch to be handled without stopping loop, got %v", err)
	}
	if msg.termCalls != 1 {
		t.Fatalf("expected one term, got %d", msg.termCalls)
	}
	if msg.nakCalls != 0 {
		t.Fatalf("expected no nak, got %d", msg.nakCalls)
	}
	if len(dlq.events) != 1 || dlq.events[0].Reason != "invalid_batch" {
		t.Fatalf("expected invalid_batch dlq event, got %#v", dlq.events)
	}
}

func TestConsumeMessageTermsRetryExhaustedBatchToDLQ(t *testing.T) {
	dlq := &stubDLQPublisher{}
	msg := &stubConsumableMessage{
		payload:       []byte(`{"schema_version":"logs.raw.v1","request_id":"req-123","received_at":"2026-07-09T20:12:07Z","tenant_id":1,"service":"checkout","env":"prod","source":"app","logs":[{"timestamp":"2026-07-07T16:00:00Z","level":"error","message":"database timeout"}]}`),
		deliveryCount: 5,
	}

	err := consumeMessage(context.Background(), msg, dlq, 5, func(context.Context, contracts.LogsRawEvent) error {
		return errors.New("clickhouse unavailable")
	}, nil)
	if err != nil {
		t.Fatalf("expected retry exhausted batch to be handled without stopping loop, got %v", err)
	}
	if msg.termCalls != 1 {
		t.Fatalf("expected one term, got %d", msg.termCalls)
	}
	if msg.nakCalls != 0 {
		t.Fatalf("expected no nak, got %d", msg.nakCalls)
	}
	if len(dlq.events) != 1 || dlq.events[0].Reason != "retry_exhausted" {
		t.Fatalf("expected retry_exhausted dlq event, got %#v", dlq.events)
	}
}

type stubConsumableMessage struct {
	payload       []byte
	deliveryCount uint64
	ackErr        error
	nakErr        error
	termErr       error
	ackCalls      int
	nakCalls      int
	termCalls     int
	nakDelay      time.Duration
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

func (m *stubConsumableMessage) DeliveryCount() uint64 {
	if m.deliveryCount == 0 {
		return 1
	}
	return m.deliveryCount
}

type stubDLQPublisher struct {
	events []contracts.LogsDLQEvent
	err    error
}

func (p *stubDLQPublisher) Publish(_ context.Context, event contracts.LogsDLQEvent) error {
	p.events = append(p.events, event)
	return p.err
}
