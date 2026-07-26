package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/auth"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/ingest"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/logging"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/metrics"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/readiness"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/stream"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	cfg := config.Load("ingest-api", ":8080")
	db, err := sql.Open("pgx", cfg.PostgresDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	nc, publisher, err := stream.ConnectJetStream(cfg.NATSURL, cfg.NATSStream, cfg.NATSSubject)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	queueMonitor := stream.NewQueueMonitor(cfg.NATSURL, cfg.NATSStream, cfg.NATSDurable)
	observer := ingest.NewMetricsObserver(logging.New(cfg.LogLevel))
	service := app.NewHTTPService(cfg, routes(cfg, db, auth.NewPostgresAuthenticator(db), publisher, observer, queueMonitor))
	app.Run(service)
}

func routes(cfg config.Config, db *sql.DB, authenticator ingest.Authenticator, publisher ingest.Publisher, observer *ingest.MetricsObserver, queueMonitor stream.QueueStatsProvider) http.Handler {
	httpMetrics := metrics.NewHTTPCollector("ingest-api")
	metricsHandler := metrics.NewHandler("ingest-api", httpMetrics, observer, stream.NewQueueLagCollector("ingest-api", queueMonitor))
	readyHandler := readiness.NewHandler(
		readiness.PostgresChecker("postgres", db),
		readiness.Func("nats", func(ctx context.Context) error {
			return stream.CheckURL(ctx, cfg.NATSURL)
		}),
	)

	handler := ingest.NewHandler(ingest.Config{
		MaxBodyBytes:  1 << 20,
		MaxLogEntries: 1000,
		Authenticator: authenticator,
		Observer:      observer,
		RateLimiter:   ingest.NewMemoryRateLimiter(),
		Backpressure: ingest.QueueLagBackpressure{
			Strategy:      cfg.NATSBackpressureStrategy,
			HighWatermark: cfg.NATSQueueLagHighWatermark,
			Delay:         cfg.NATSBackpressureDelay,
			Monitor:       queueMonitor,
		},
		Publisher: publisher,
	})

	mux := http.NewServeMux()
	healthHandler := readiness.HealthHandler()
	mux.Handle("/metrics", metricsHandler)
	mux.Handle("/health", httpMetrics.Middleware("/health", healthHandler))
	mux.Handle("/ready", httpMetrics.Middleware("/ready", readyHandler))
	mux.Handle("/v1/logs", httpMetrics.Middleware("/v1/logs", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		handler.ServeHTTP(w, r)
	})))
	return mux
}
