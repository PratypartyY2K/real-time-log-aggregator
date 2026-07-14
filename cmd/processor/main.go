package main

import (
	"context"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/metrics"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/processor"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/worker"
)

func main() {
	cfg := config.Load("processor", "")
	processorMetrics := processor.NewMetrics(cfg.ServiceName)
	service := worker.New(cfg, func(ctx context.Context, logger app.Logger) error {
		return processor.Run(ctx, logger, cfg, processorMetrics)
	}, worker.WithMetricsHandler(metrics.NewHandler(cfg.ServiceName, processorMetrics)))
	app.Run(service)
}
