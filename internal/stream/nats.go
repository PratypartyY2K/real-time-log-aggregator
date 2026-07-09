package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/ingest"
	"github.com/nats-io/nats.go"
)

type JetStreamPublisher struct {
	js      nats.JetStreamContext
	subject string
}

func ConnectJetStream(url, streamName, subject string) (*nats.Conn, *JetStreamPublisher, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to nats: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("create jetstream context: %w", err)
	}

	if err := ensureStream(js, streamName, subject); err != nil {
		nc.Close()
		return nil, nil, err
	}

	return nc, &JetStreamPublisher{
		js:      js,
		subject: subject,
	}, nil
}

func (p *JetStreamPublisher) Publish(ctx context.Context, batch ingest.PublishedBatch) error {
	payload, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal batch: %w", err)
	}

	msg := &nats.Msg{
		Subject: p.subject,
		Data:    payload,
		Header:  nats.Header{},
	}
	msg.Header.Set(nats.MsgIdHdr, batch.RequestID)

	if _, err := p.js.PublishMsg(msg, nats.Context(ctx)); err != nil {
		return fmt.Errorf("publish batch: %w", err)
	}

	return nil
}

func ensureStream(js nats.JetStreamContext, streamName, subject string) error {
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
		Storage:  nats.FileStorage,
	}); err != nil {
		var apiErr *nats.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode == 10058 {
			return nil
		}
		return fmt.Errorf("ensure stream %q: %w", streamName, err)
	}

	return nil
}
