package stream

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
	"github.com/nats-io/nats.go"
)

func TestChaosProcessorKillRedeliversUnackedBatchAfterRecovery(t *testing.T) {
	payload := []byte(`{"schema_version":"logs.raw.v1","request_id":"req-chaos","fingerprint":"req-chaos","received_at":"2026-07-24T20:00:00Z","tenant_id":1,"service":"checkout","env":"prod","source":"chaos","logs":[{"timestamp":"2026-07-24T20:00:00Z","level":"error","message":"processor interrupted"}]}`)

	interruptedDelivery := &stubConsumableMessage{payload: payload, deliveryCount: 1}
	if interruptedDelivery.ackCalls != 0 || interruptedDelivery.nakCalls != 0 {
		t.Fatal("a killed processor must leave its in-flight message unacknowledged")
	}

	recoveredDelivery := &stubConsumableMessage{payload: payload, deliveryCount: 2}
	handled := 0
	err := consumeMessage(context.Background(), recoveredDelivery, nil, 5, func(_ context.Context, event contracts.LogsRawEvent) error {
		handled++
		if event.RequestID != "req-chaos" {
			t.Fatalf("unexpected recovered request id %q", event.RequestID)
		}
		return nil
	}, nil, nil, nil)
	if err != nil {
		t.Fatalf("process redelivered batch after restart: %v", err)
	}
	if handled != 1 || recoveredDelivery.ackCalls != 1 {
		t.Fatalf("expected recovered processor to handle and ack once, handled=%d acked=%d", handled, recoveredDelivery.ackCalls)
	}
	if recoveredDelivery.DeliveryCount() != 2 {
		t.Fatalf("expected redelivery count 2, got %d", recoveredDelivery.DeliveryCount())
	}
}

func TestChaosNATSDisconnectConsumerWaitsAndRecovers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var fetchCalls atomic.Int32
	var reported atomic.Int32
	consumer := &JetStreamConsumer{
		retryDelay: time.Millisecond,
		fetch: func(int, ...nats.PullOpt) ([]*nats.Msg, error) {
			call := fetchCalls.Add(1)
			if call == 1 {
				return nil, nats.ErrDisconnected
			}
			cancel()
			return nil, nats.ErrTimeout
		},
	}

	err := consumer.Consume(ctx, func(context.Context, contracts.LogsRawEvent) error {
		return errors.New("unexpected handler call")
	}, func(_ context.Context, err error) {
		if isTransientConnectionError(err) {
			reported.Add(1)
		}
	})
	if err != nil {
		t.Fatalf("expected consumer to survive transient disconnect, got %v", err)
	}
	if fetchCalls.Load() < 2 {
		t.Fatalf("expected fetch to resume after disconnect, got %d calls", fetchCalls.Load())
	}
	if reported.Load() != 1 {
		t.Fatalf("expected one observable disconnect error, got %d", reported.Load())
	}
}
