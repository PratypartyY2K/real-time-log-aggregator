package stream

import (
	"context"
	"errors"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
)

type poisonBatchError struct {
	err error
}

func (e poisonBatchError) Error() string {
	return e.err.Error()
}

func (e poisonBatchError) Unwrap() error {
	return e.err
}

func MarkPoisonBatch(err error) error {
	if err == nil {
		return nil
	}
	return poisonBatchError{err: err}
}

func isPoisonBatchError(err error) bool {
	var target poisonBatchError
	return errors.As(err, &target)
}

func publishDLQ(
	ctx context.Context,
	publisher dlqPublisher,
	rawPayload []byte,
	event *contracts.LogsRawEvent,
	attempts uint64,
	reason string,
	err error,
) error {
	if publisher == nil {
		return nil
	}

	dlqEvent := contracts.LogsDLQEvent{
		SchemaVersion: contracts.LogsDLQSchemaVersion,
		FailedAt:      time.Now().UTC().Format(time.RFC3339Nano),
		Reason:        reason,
		Error:         err.Error(),
		Attempts:      attempts,
		RawPayload:    append([]byte(nil), rawPayload...),
	}
	if event != nil {
		dlqEvent.RequestID = event.RequestID
		dlqEvent.TenantID = event.TenantID
		dlqEvent.Service = event.Service
		dlqEvent.Env = event.Env
		dlqEvent.Source = event.Source
		eventCopy := *event
		dlqEvent.Event = &eventCopy
	}

	return publisher.Publish(ctx, dlqEvent)
}
