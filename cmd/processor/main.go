package main

import (
	"context"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/processor"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/worker"
)

func main() {
	cfg := config.Load("processor", "")
	service := worker.New(cfg, func(ctx context.Context, logger app.Logger) error {
		return processor.Run(ctx, logger, cfg)
	})
	app.Run(service)
}
