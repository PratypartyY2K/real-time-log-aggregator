package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/logging"
	"github.com/nats-io/nats.go"
)

type JetStreamPublisher struct {
	js      nats.JetStreamContext
	subject string
}

type JetStreamConsumer struct {
	sub           *nats.Subscription
	fetch         func(int, ...nats.PullOpt) ([]*nats.Msg, error)
	dlq           *DLQPublisher
	dlqObserver   DLQObserver
	retryObserver RetryObserver
	maxDeliver    uint64
	retryDelay    time.Duration
}

type DLQObserver interface {
	ObserveDLQ(reason, outcome string)
}

type RetryObserver interface {
	ObserveRetry(reason string)
}

type ConsumerOptions struct {
	URL           string
	StreamName    string
	Subject       string
	DLQSubject    string
	Durable       string
	MaxDeliver    int
	ReplayMode    string
	ReplaySeq     uint64
	ReplayTime    string
	DLQObserver   DLQObserver
	RetryObserver RetryObserver
}

const consumerRetryDelay = 5 * time.Second
const connectionRetryDelay = time.Second

type consumeErrorHandler func(context.Context, error)

type dlqPublisher interface {
	Publish(context.Context, contracts.LogsDLQEvent) error
}

type consumableMessage interface {
	Payload() []byte
	Header() nats.Header
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

func (m natsConsumableMessage) Header() nats.Header {
	return m.msg.Header
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

func CheckConnection(_ context.Context, nc *nats.Conn) error {
	if nc == nil {
		return fmt.Errorf("nats connection is nil")
	}
	if !nc.IsConnected() {
		return fmt.Errorf("nats is not connected")
	}
	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		return fmt.Errorf("flush nats connection: %w", err)
	}
	if err := nc.LastError(); err != nil {
		return fmt.Errorf("nats connection error: %w", err)
	}
	return nil
}

func CheckURL(_ context.Context, url string) error {
	nc, err := nats.Connect(url, nats.Timeout(2*time.Second))
	if err != nil {
		return fmt.Errorf("connect to nats: %w", err)
	}
	defer nc.Close()

	return CheckConnection(context.Background(), nc)
}

func ConnectJetStreamConsumer(opts ConsumerOptions) (*nats.Conn, *JetStreamConsumer, error) {
	nc, err := nats.Connect(opts.URL)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to nats: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("create jetstream context: %w", err)
	}

	if err := validateStream(js, opts.StreamName, opts.Subject); err != nil {
		nc.Close()
		return nil, nil, err
	}

	sub, err := buildConsumerSubscription(js, opts)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}

	return nc, &JetStreamConsumer{
		sub:           sub,
		fetch:         sub.Fetch,
		dlq:           &DLQPublisher{js: js, subject: opts.DLQSubject},
		dlqObserver:   opts.DLQObserver,
		retryObserver: opts.RetryObserver,
		maxDeliver:    uint64(opts.MaxDeliver),
		retryDelay:    connectionRetryDelay,
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
	msgID := strings.TrimSpace(event.Fingerprint)
	if msgID == "" {
		msgID = event.RequestID
	}
	msg.Header.Set(nats.MsgIdHdr, msgID)
	if event.CorrelationID != "" {
		msg.Header.Set(logging.RequestIDHeader, event.CorrelationID)
	}
	if event.TraceID != "" {
		msg.Header.Set(logging.TraceIDHeader, event.TraceID)
	}

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

		msgs, err := c.fetchMessages(1, nats.MaxWait(time.Second))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			if isTransientConnectionError(err) {
				reportConsumeError(ctx, onError, fmt.Errorf("fetch message while nats unavailable: %w", err))
				if !waitForRetry(ctx, c.retryDelay) {
					return nil
				}
				continue
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		for _, msg := range msgs {
			if err := consumeMessage(ctx, natsConsumableMessage{msg: msg}, c.dlq, c.maxDeliver, handler, onError, c.dlqObserver, c.retryObserver); err != nil {
				return err
			}
		}
	}
}

func (c *JetStreamConsumer) fetchMessages(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
	if c.fetch != nil {
		return c.fetch(batch, opts...)
	}
	if c.sub == nil {
		return nil, nats.ErrBadSubscription
	}
	return c.sub.Fetch(batch, opts...)
}

func isTransientConnectionError(err error) bool {
	return errors.Is(err, nats.ErrDisconnected) ||
		errors.Is(err, nats.ErrConnectionReconnecting) ||
		errors.Is(err, nats.ErrNoServers) ||
		errors.Is(err, nats.ErrConnectionClosed)
}

func waitForRetry(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func consumeMessage(
	ctx context.Context,
	msg consumableMessage,
	dlqPublisher dlqPublisher,
	maxDeliver uint64,
	handler func(context.Context, contracts.LogsRawEvent) error,
	onError consumeErrorHandler,
	dlqObserver DLQObserver,
	retryObserver RetryObserver,
) error {
	var event contracts.LogsRawEvent
	if err := json.Unmarshal(msg.Payload(), &event); err != nil {
		dlqErr := publishDLQ(ctx, dlqPublisher, msg.Payload(), nil, msg.DeliveryCount(), "malformed_payload", err)
		observeDLQPublication(dlqObserver, dlqPublisher, "malformed_payload", dlqErr)
		if dlqErr != nil {
			return fmt.Errorf("publish malformed payload to dlq: %w", dlqErr)
		}
		if termErr := msg.Term(); termErr != nil {
			return fmt.Errorf("term invalid message: %w", termErr)
		}
		reportConsumeError(ctx, onError, fmt.Errorf("decode message: %w", err))
		return nil
	}
	applyCorrelationHeaders(&event, msg.Header())

	if err := handler(ctx, event); err != nil {
		if isPoisonBatchError(err) {
			dlqErr := publishDLQ(ctx, dlqPublisher, msg.Payload(), &event, msg.DeliveryCount(), "invalid_batch", err)
			observeDLQPublication(dlqObserver, dlqPublisher, "invalid_batch", dlqErr)
			if dlqErr != nil {
				return fmt.Errorf("publish invalid batch to dlq: %w", dlqErr)
			}
			if termErr := msg.Term(); termErr != nil {
				return fmt.Errorf("term poison message: %w", termErr)
			}
			reportConsumeError(ctx, onError, fmt.Errorf("handle message: %w", err))
			return nil
		}
		if maxDeliver > 0 && msg.DeliveryCount() >= maxDeliver {
			dlqErr := publishDLQ(ctx, dlqPublisher, msg.Payload(), &event, msg.DeliveryCount(), "retry_exhausted", err)
			observeDLQPublication(dlqObserver, dlqPublisher, "retry_exhausted", dlqErr)
			if dlqErr != nil {
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
		observeRetry(retryObserver, "retryable_error")
		reportConsumeError(ctx, onError, fmt.Errorf("handle message: %w", err))
		return nil
	}

	if err := msg.Ack(); err != nil {
		return fmt.Errorf("ack message: %w", err)
	}

	return nil
}

func applyCorrelationHeaders(event *contracts.LogsRawEvent, header nats.Header) {
	if event == nil {
		return
	}
	if event.CorrelationID == "" {
		event.CorrelationID = strings.TrimSpace(header.Get(logging.RequestIDHeader))
	}
	if event.TraceID == "" {
		event.TraceID = strings.TrimSpace(header.Get(logging.TraceIDHeader))
	}
	if event.CorrelationID == "" {
		event.CorrelationID = event.RequestID
	}
}

func observeRetry(observer RetryObserver, reason string) {
	if observer != nil {
		observer.ObserveRetry(reason)
	}
}

func observeDLQPublication(observer DLQObserver, publisher dlqPublisher, reason string, err error) {
	if observer == nil || publisher == nil {
		return
	}
	outcome := "published"
	if err != nil {
		outcome = "failed"
	}
	observer.ObserveDLQ(reason, outcome)
}

func reportConsumeError(ctx context.Context, onError consumeErrorHandler, err error) {
	if onError != nil {
		onError(ctx, err)
	}
}

func SetupJetStream(url, streamName, subject, dlqSubject, durable string, maxDeliver int, duplicateWindow time.Duration) error {
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
		Name:       streamName,
		Subjects:   []string{subject, dlqSubject},
		Storage:    nats.FileStorage,
		Duplicates: duplicateWindow,
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

func buildConsumerSubscription(js nats.JetStreamContext, opts ConsumerOptions) (*nats.Subscription, error) {
	replayMode := opts.ReplayMode
	if replayMode == "" || replayMode == "live" {
		if err := validateConsumer(js, opts.StreamName, opts.Durable, opts.Subject, opts.MaxDeliver); err != nil {
			return nil, err
		}
		sub, err := js.PullSubscribe(opts.Subject, opts.Durable, nats.Bind(opts.StreamName, opts.Durable))
		if err != nil {
			return nil, fmt.Errorf("create pull subscription: %w", err)
		}
		return sub, nil
	}

	subOpts := []nats.SubOpt{
		nats.BindStream(opts.StreamName),
		nats.AckExplicit(),
		nats.MaxDeliver(opts.MaxDeliver),
	}

	switch replayMode {
	case "all":
		subOpts = append(subOpts, nats.DeliverAll())
	case "sequence":
		if opts.ReplaySeq == 0 {
			return nil, fmt.Errorf("replay mode sequence requires NATS_REPLAY_SEQUENCE > 0")
		}
		subOpts = append(subOpts, nats.StartSequence(opts.ReplaySeq))
	case "time":
		replayTime, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(opts.ReplayTime))
		if err != nil {
			return nil, fmt.Errorf("parse NATS_REPLAY_TIME: %w", err)
		}
		subOpts = append(subOpts, nats.StartTime(replayTime.UTC()))
	default:
		return nil, fmt.Errorf("unsupported replay mode %q", replayMode)
	}

	sub, err := js.PullSubscribe(opts.Subject, "", subOpts...)
	if err != nil {
		return nil, fmt.Errorf("create replay pull subscription: %w", err)
	}
	return sub, nil
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
