package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
	"github.com/nats-io/nats.go"
)

type JetStreamPublisher struct {
	js      nats.JetStreamContext
	subject string
}

type JetStreamConsumer struct {
	sub *nats.Subscription
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

	if err := validateStream(js, streamName, subject); err != nil {
		nc.Close()
		return nil, nil, err
	}

	return nc, &JetStreamPublisher{
		js:      js,
		subject: subject,
	}, nil
}

func ConnectJetStreamConsumer(url, streamName, subject, durable string) (*nats.Conn, *JetStreamConsumer, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to nats: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("create jetstream context: %w", err)
	}

	if err := validateStream(js, streamName, subject); err != nil {
		nc.Close()
		return nil, nil, err
	}

	if err := validateConsumer(js, streamName, durable, subject); err != nil {
		nc.Close()
		return nil, nil, err
	}

	sub, err := js.PullSubscribe(subject, durable, nats.Bind(streamName, durable))
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("create pull subscription: %w", err)
	}

	return nc, &JetStreamConsumer{sub: sub}, nil
}

func (p *JetStreamPublisher) Publish(ctx context.Context, event contracts.LogsRawEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal logs.raw event: %w", err)
	}

	msg := &nats.Msg{
		Subject: p.subject,
		Data:    payload,
		Header:  nats.Header{},
	}
	msg.Header.Set(nats.MsgIdHdr, event.RequestID)

	if _, err := p.js.PublishMsg(msg, nats.Context(ctx)); err != nil {
		return fmt.Errorf("publish logs.raw event: %w", err)
	}

	return nil
}

func (c *JetStreamConsumer) Consume(ctx context.Context, handler func(context.Context, contracts.LogsRawEvent) error) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msgs, err := c.sub.Fetch(1, nats.MaxWait(time.Second))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		for _, msg := range msgs {
			var event contracts.LogsRawEvent
			if err := json.Unmarshal(msg.Data, &event); err != nil {
				_ = msg.Term()
				return fmt.Errorf("decode message: %w", err)
			}

			if err := handler(ctx, event); err != nil {
				return err
			}

			if err := msg.Ack(); err != nil {
				return fmt.Errorf("ack message: %w", err)
			}
		}
	}
}

func SetupJetStream(url, streamName, subject, durable string) error {
	nc, err := nats.Connect(url)
	if err != nil {
		return fmt.Errorf("connect to nats: %w", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("create jetstream context: %w", err)
	}

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
		Storage:  nats.FileStorage,
	}); err != nil {
		if validateErr := validateStream(js, streamName, subject); validateErr != nil {
			return fmt.Errorf("ensure stream %q: %w", streamName, err)
		}
	}

	if _, err := js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:       durable,
		FilterSubject: subject,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       30 * time.Second,
	}); err != nil {
		if validateErr := validateConsumer(js, streamName, durable, subject); validateErr != nil {
			return fmt.Errorf("ensure consumer %q: %w", durable, err)
		}
	}

	if err := validateStream(js, streamName, subject); err != nil {
		return err
	}

	return validateConsumer(js, streamName, durable, subject)
}

func validateStream(js nats.JetStreamContext, streamName, subject string) error {
	info, err := js.StreamInfo(streamName)
	if err != nil {
		return fmt.Errorf("lookup stream %q: %w", streamName, err)
	}
	if !slices.Contains(info.Config.Subjects, subject) {
		return fmt.Errorf("stream %q does not include subject %q", streamName, subject)
	}
	return nil
}

func validateConsumer(js nats.JetStreamContext, streamName, durable, subject string) error {
	info, err := js.ConsumerInfo(streamName, durable)
	if err != nil {
		return fmt.Errorf("lookup consumer %q on stream %q: %w", durable, streamName, err)
	}
	if info.Config.FilterSubject != subject {
		return fmt.Errorf("consumer %q filter subject mismatch: got %q want %q", durable, info.Config.FilterSubject, subject)
	}
	return nil
}
