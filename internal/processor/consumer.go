package processor

import (
	"context"
	"fmt"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/ingest"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/stream"
)

func Run(ctx context.Context, logger app.Logger, cfg config.Config) error {
	nc, consumer, err := stream.ConnectJetStreamConsumer(cfg.NATSURL, cfg.NATSStream, cfg.NATSSubject, cfg.NATSDurable)
	if err != nil {
		return err
	}
	defer nc.Drain()

	logger.Info("processor consumer started", "stream", cfg.NATSStream, "subject", cfg.NATSSubject, "durable", cfg.NATSDurable)

	return consumer.Consume(ctx, func(ctx context.Context, batch ingest.PublishedBatch) error {
		return handleBatch(ctx, logger, batch)
	})
}

func handleBatch(_ context.Context, logger app.Logger, batch ingest.PublishedBatch) error {
	if batch.RequestID == "" {
		return fmt.Errorf("batch missing request_id")
	}

	logger.Info(
		"processor received batch",
		"request_id", batch.RequestID,
		"received_at", batch.ReceivedAt,
		"service", batch.Batch.Service,
		"env", batch.Batch.Env,
		"source", batch.Batch.Source,
		"log_count", len(batch.Batch.Logs),
	)

	return nil
}
