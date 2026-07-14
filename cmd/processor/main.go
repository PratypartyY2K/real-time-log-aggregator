package main

import (
	"context"
	"net/http"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/metrics"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/processor"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/readiness"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/stream"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/worker"
)

func main() {
	cfg := config.Load("processor", "")
	processorMetrics := processor.NewMetrics(cfg.ServiceName)
	writer := processor.NewClickHouseWriter(cfg.ClickHouseDSN)
	probeMux := http.NewServeMux()
	probeMux.Handle("/metrics", metrics.NewHandler(cfg.ServiceName, processorMetrics))
	probeMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	probeMux.Handle("/readyz", readiness.NewHandler(
		readiness.Func("nats", func(ctx context.Context) error {
			return stream.CheckURL(ctx, cfg.NATSURL)
		}),
		readiness.Func("clickhouse", writer.Check),
	))
	service := worker.New(cfg, func(ctx context.Context, logger app.Logger) error {
		return processor.Run(ctx, logger, cfg, processorMetrics)
	}, worker.WithMetricsHandler(probeMux))
	app.Run(service)
}
