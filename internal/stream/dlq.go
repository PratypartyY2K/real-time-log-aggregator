package stream

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
	"github.com/nats-io/nats.go"
)

type DLQPublisher struct {
	js      nats.JetStreamContext
	subject string
}

func (p *DLQPublisher) Publish(ctx context.Context, event contracts.LogsDLQEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal logs.dlq event: %w", err)
	}

	msg := &nats.Msg{
		Subject: p.subject,
		Data:    payload,
		Header:  nats.Header{},
	}
	if event.RequestID != "" {
		msg.Header.Set(nats.MsgIdHdr, dlqMessageID(event))
	}

	if _, err := p.js.PublishMsg(msg, nats.Context(ctx)); err != nil {
		return fmt.Errorf("publish logs.dlq event: %w", err)
	}

	return nil
}

func dlqMessageID(event contracts.LogsDLQEvent) string {
	return event.RequestID + ":" + event.Reason
}
