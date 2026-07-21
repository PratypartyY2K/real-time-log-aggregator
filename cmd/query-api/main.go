package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/auth"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/metrics"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/queryapi"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/readiness"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	cfg := config.Load("query-api", ":8081")
	db, err := sql.Open("pgx", cfg.PostgresDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
	service := app.NewHTTPService(cfg, routes(cfg.ServiceName, db, auth.NewPostgresAuthenticator(db), queryapi.NewClickHouseStore(cfg.ClickHouseDSN, cfg.ClickHouseShardDSNs...)))
	app.Run(service)
}

type readyStore interface {
	queryapi.LogStore
	queryapi.AnalyticsStore
	Check(context.Context) error
}

func routes(serviceName string, db *sql.DB, resolver queryapi.TenantResolver, store readyStore) http.Handler {
	httpMetrics := metrics.NewHTTPCollector(serviceName)
	metricsHandler := metrics.NewHandler(serviceName, httpMetrics)
	checks := []readiness.Checker{readiness.Func("clickhouse", store.Check)}
	if db != nil {
		checks = append([]readiness.Checker{readiness.PostgresChecker("postgres", db)}, checks...)
	}
	readyHandler := readiness.NewHandler(checks...)

	mux := http.NewServeMux()
	mux.Handle("/metrics", metricsHandler)
	mux.Handle("/healthz", httpMetrics.Middleware("/healthz", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})))
	mux.Handle("/readyz", httpMetrics.Middleware("/readyz", readyHandler))
	mux.Handle("/v1/status", httpMetrics.Middleware("/v1/status", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"service": serviceName,
			"time":    time.Now().UTC(),
			"status":  "bootstrap",
		})
	})))
	mux.Handle("/v1/analytics", httpMetrics.Middleware("/v1/analytics", queryapi.TenantAuthMiddleware(resolver, queryapi.NewAnalyticsHandler(store))))
	mux.Handle("/v1/logs", httpMetrics.Middleware("/v1/logs", queryapi.TenantAuthMiddleware(resolver, queryapi.NewHandler(store))))
	mux.Handle("/v1/query", httpMetrics.Middleware("/v1/query", queryapi.TenantAuthMiddleware(resolver, queryapi.NewQueryDSLHandler(store))))
	return mux
}
