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
	sub        *nats.Subscription
	dlq        *DLQPublisher
	maxDeliver uint64
}

const consumerRetryDelay = 5 * time.Second

type consumeErrorHandler func(context.Context, error)

type dlqPublisher interface {
	Publish(context.Context, contracts.LogsDLQEvent) error
}

type consumableMessage interface {
	Payload() []byte
	Ack() error
	NakWithDelay(time.Duration) error
	Term() error
	DeliveryCount() uint64
}

type natsConsumableMessage struct {
	msg *nats.Msg
}

func (m natsConsumableMessage) Payload() []byte {
	return m.msg.Data
}

func (m natsConsumableMessage) Ack() error {
	return m.msg.Ack()
}

func (m natsConsumableMessage) NakWithDelay(delay time.Duration) error {
	return m.msg.NakWithDelay(delay)
}

func (m natsConsumableMessage) Term() error {
	return m.msg.Term()
}

func (m natsConsumableMessage) DeliveryCount() uint64 {
	meta, err := m.msg.Metadata()
	if err != nil || meta == nil || meta.NumDelivered == 0 {
		return 1
	}
	return meta.NumDelivered
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

func ConnectJetStreamConsumer(url, streamName, subject, dlqSubject, durable string, maxDeliver int) (*nats.Conn, *JetStreamConsumer, error) {
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

	if err := validateConsumer(js, streamName, durable, subject, maxDeliver); err != nil {
		nc.Close()
		return nil, nil, err
	}

	sub, err := js.PullSubscribe(subject, durable, nats.Bind(streamName, durable))
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("create pull subscription: %w", err)
	}

	return nc, &JetStreamConsumer{
		sub:        sub,
		dlq:        &DLQPublisher{js: js, subject: dlqSubject},
		maxDeliver: uint64(maxDeliver),
	}, nil
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

func (c *JetStreamConsumer) Consume(ctx context.Context, handler func(context.Context, contracts.LogsRawEvent) error, onError consumeErrorHandler) error {
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
			if err := consumeMessage(ctx, natsConsumableMessage{msg: msg}, c.dlq, c.maxDeliver, handler, onError); err != nil {
				return err
			}
		}
	}
}

func consumeMessage(
	ctx context.Context,
	msg consumableMessage,
	dlqPublisher dlqPublisher,
	maxDeliver uint64,
	handler func(context.Context, contracts.LogsRawEvent) error,
	onError consumeErrorHandler,
) error {
	var event contracts.LogsRawEvent
	if err := json.Unmarshal(msg.Payload(), &event); err != nil {
		if dlqErr := publishDLQ(ctx, dlqPublisher, msg.Payload(), nil, msg.DeliveryCount(), "malformed_payload", err); dlqErr != nil {
			return fmt.Errorf("publish malformed payload to dlq: %w", dlqErr)
		}
		if termErr := msg.Term(); termErr != nil {
			return fmt.Errorf("term invalid message: %w", termErr)
		}
		reportConsumeError(ctx, onError, fmt.Errorf("decode message: %w", err))
		return nil
	}

	if err := handler(ctx, event); err != nil {
		if isPoisonBatchError(err) {
			if dlqErr := publishDLQ(ctx, dlqPublisher, msg.Payload(), &event, msg.DeliveryCount(), "invalid_batch", err); dlqErr != nil {
				return fmt.Errorf("publish invalid batch to dlq: %w", dlqErr)
			}
			if termErr := msg.Term(); termErr != nil {
				return fmt.Errorf("term poison message: %w", termErr)
			}
			reportConsumeError(ctx, onError, fmt.Errorf("handle message: %w", err))
			return nil
		}
		if maxDeliver > 0 && msg.DeliveryCount() >= maxDeliver {
			if dlqErr := publishDLQ(ctx, dlqPublisher, msg.Payload(), &event, msg.DeliveryCount(), "retry_exhausted", err); dlqErr != nil {
				return fmt.Errorf("publish retry exhausted batch to dlq: %w", dlqErr)
			}
			if termErr := msg.Term(); termErr != nil {
				return fmt.Errorf("term exhausted message: %w", termErr)
			}
			reportConsumeError(ctx, onError, fmt.Errorf("handle message: %w", err))
			return nil
		}
		if nakErr := msg.NakWithDelay(consumerRetryDelay); nakErr != nil {
			return fmt.Errorf("nak message for retry: %w", nakErr)
		}
		reportConsumeError(ctx, onError, fmt.Errorf("handle message: %w", err))
		return nil
	}

	if err := msg.Ack(); err != nil {
		return fmt.Errorf("ack message: %w", err)
	}

	return nil
}

func reportConsumeError(ctx context.Context, onError consumeErrorHandler, err error) {
	if onError != nil {
		onError(ctx, err)
	}
}

func SetupJetStream(url, streamName, subject, dlqSubject, durable string, maxDeliver int) error {
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
		Subjects: []string{subject, dlqSubject},
		Storage:  nats.FileStorage,
	}); err != nil {
		if validateErr := validateStream(js, streamName, subject, dlqSubject); validateErr != nil {
			return fmt.Errorf("ensure stream %q: %w", streamName, err)
		}
	}

	if _, err := js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:       durable,
		FilterSubject: subject,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    maxDeliver,
	}); err != nil {
		if validateErr := validateConsumer(js, streamName, durable, subject, maxDeliver); validateErr != nil {
			return fmt.Errorf("ensure consumer %q: %w", durable, err)
		}
	}

	if err := validateStream(js, streamName, subject, dlqSubject); err != nil {
		return err
	}

	return validateConsumer(js, streamName, durable, subject, maxDeliver)
}

func validateStream(js nats.JetStreamContext, streamName string, subjects ...string) error {
	info, err := js.StreamInfo(streamName)
	if err != nil {
		return fmt.Errorf("lookup stream %q: %w", streamName, err)
	}
	for _, subject := range subjects {
		if !slices.Contains(info.Config.Subjects, subject) {
			return fmt.Errorf("stream %q does not include subject %q", streamName, subject)
		}
	}
	return nil
}

func validateConsumer(js nats.JetStreamContext, streamName, durable, subject string, maxDeliver int) error {
	info, err := js.ConsumerInfo(streamName, durable)
	if err != nil {
		return fmt.Errorf("lookup consumer %q on stream %q: %w", durable, streamName, err)
	}
	if info.Config.FilterSubject != subject {
		return fmt.Errorf("consumer %q filter subject mismatch: got %q want %q", durable, info.Config.FilterSubject, subject)
	}
	if maxDeliver > 0 && info.Config.MaxDeliver != maxDeliver {
		return fmt.Errorf("consumer %q max deliver mismatch: got %d want %d", durable, info.Config.MaxDeliver, maxDeliver)
	}
	return nil
}
