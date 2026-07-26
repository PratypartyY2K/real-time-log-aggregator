package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/alerts"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/metrics"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/processor"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/readiness"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/stream"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/worker"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	cfg := config.Load("processor", "")
	db, err := sql.Open("pgx", cfg.PostgresDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := db.PingContext(context.Background()); err != nil {
		log.Fatal(err)
	}
	processorMetrics := processor.NewMetrics(cfg.ServiceName)
	alertMetrics := alerts.NewMetricsCollector(cfg.ServiceName)
	writer := processor.NewClickHouseWriter(cfg.ClickHouseDSN)
	ruleStore := alerts.NewPostgresStore(db)
	queueMonitor := stream.NewQueueMonitor(cfg.NATSURL, cfg.NATSStream, cfg.NATSDurable)
	probeMux := http.NewServeMux()
	healthHandler := readiness.HealthHandler()
	readyHandler := readiness.NewHandler(
		readiness.Func("nats", func(ctx context.Context) error {
			return stream.CheckURL(ctx, cfg.NATSURL)
		}),
		readiness.PostgresChecker("postgres", db),
		readiness.Func("clickhouse", writer.Check),
	)
	probeMux.Handle("/metrics", metrics.NewHandler(cfg.ServiceName, processorMetrics, alertMetrics, stream.NewQueueLagCollector(cfg.ServiceName, queueMonitor)))
	probeMux.Handle("/health", healthHandler)
	probeMux.Handle("/healthz", healthHandler)
	probeMux.Handle("/ready", readyHandler)
	probeMux.Handle("/readyz", readyHandler)
	service := worker.New(cfg, func(ctx context.Context, logger app.Logger) error {
		return processor.Run(ctx, logger, cfg, processorMetrics, alertMetrics, ruleStore)
	}, worker.WithMetricsHandler(probeMux))
	app.Run(service)
}
