package processor

import (
	"context"
	"fmt"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/stream"
)

func Run(ctx context.Context, logger app.Logger, cfg config.Config, metrics *Metrics) error {
	nc, consumer, err := stream.ConnectJetStreamConsumer(
		cfg.NATSURL,
		cfg.NATSStream,
		cfg.NATSSubject,
		cfg.NATSDLQSubject,
		cfg.NATSDurable,
		cfg.NATSMaxDeliver,
	)
	if err != nil {
		return err
	}
	defer nc.Drain()

	writer := NewClickHouseWriter(cfg.ClickHouseDSN)

	logger.Info("processor consumer started", "stream", cfg.NATSStream, "subject", cfg.NATSSubject, "durable", cfg.NATSDurable)

	return consumer.Consume(ctx, func(ctx context.Context, batch contracts.LogsRawEvent) error {
		start := time.Now()
		err := handleBatch(ctx, logger, writer, batch)
		result := resultSuccess
		if err != nil {
			result = resultRetryable
		}
		if err != nil && stream.IsPoisonBatchError(err) {
			result = resultInvalidBatch
		}
		metrics.ObserveBatch(result, len(batch.Logs), time.Since(start))
		return err
	}, func(_ context.Context, err error) {
		logger.Error("processor failed to handle batch", "error", err)
	})
}

func handleBatch(ctx context.Context, logger app.Logger, writer LogWriter, batch contracts.LogsRawEvent) error {
	if err := batch.Validate(); err != nil {
		return stream.MarkPoisonBatch(fmt.Errorf("invalid logs.raw event: %w", err))
	}

	normalized, err := normalizeBatch(batch)
	if err != nil {
		return stream.MarkPoisonBatch(fmt.Errorf("normalize logs.raw event: %w", err))
	}
	if writer == nil {
		return fmt.Errorf("processor writer is not configured")
	}
	if err := writer.WriteBatch(ctx, normalized); err != nil {
		return fmt.Errorf("persist normalized logs: %w", err)
	}

	logger.Info(
		"processor persisted batch",
		"request_id", batch.RequestID,
		"tenant_id", batch.TenantID,
		"received_at", batch.ReceivedAt,
		"schema_version", batch.SchemaVersion,
		"service", batch.Service,
		"env", batch.Env,
		"source", batch.Source,
		"log_count", len(batch.Logs),
		"normalized_log_count", len(normalized),
	)

	return nil
}
