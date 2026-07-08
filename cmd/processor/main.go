package main

import (
	"context"
	"time"

	"github.com/pratyushkumar/real-time-log-aggregator/internal/app"
	"github.com/pratyushkumar/real-time-log-aggregator/internal/config"
	"github.com/pratyushkumar/real-time-log-aggregator/internal/worker"
)

func main() {
	cfg := config.Load("processor", "")
	service := worker.New(cfg, func(ctx context.Context, logger app.Logger) error {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		logger.Info("processor worker started", "loop", "bootstrap")
		for {
			select {
			case <-ctx.Done():
				logger.Info("processor worker stopping")
				return nil
			case <-ticker.C:
				logger.Info("processor heartbeat", "component", "worker")
			}
		}
	})
	app.Run(service)
}
