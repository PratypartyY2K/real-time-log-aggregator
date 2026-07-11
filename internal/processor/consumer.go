package processor

import (
	"context"
	"fmt"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/stream"
)

func Run(ctx context.Context, logger app.Logger, cfg config.Config) error {
	nc, consumer, err := stream.ConnectJetStreamConsumer(cfg.NATSURL, cfg.NATSStream, cfg.NATSSubject, cfg.NATSDurable)
	if err != nil {
		return err
	}
	defer nc.Drain()

	logger.Info("processor consumer started", "stream", cfg.NATSStream, "subject", cfg.NATSSubject, "durable", cfg.NATSDurable)

	return consumer.Consume(ctx, func(ctx context.Context, batch contracts.LogsRawEvent) error {
		return handleBatch(ctx, logger, batch)
	}, func(_ context.Context, err error) {
		logger.Error("processor failed to handle batch", "error", err)
	})
}

func handleBatch(_ context.Context, logger app.Logger, batch contracts.LogsRawEvent) error {
	if err := batch.Validate(); err != nil {
		return fmt.Errorf("invalid logs.raw event: %w", err)
	}

	normalized, err := normalizeBatch(batch)
	if err != nil {
		return fmt.Errorf("normalize logs.raw event: %w", err)
	}

	logger.Info(
		"processor received batch",
		"request_id", batch.RequestID,
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
