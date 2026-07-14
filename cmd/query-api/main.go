package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/metrics"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/queryapi"
)

func main() {
	cfg := config.Load("query-api", ":8081")
	service := app.NewHTTPService(cfg, routes(cfg.ServiceName, queryapi.NewClickHouseStore(cfg.ClickHouseDSN)))
	app.Run(service)
}

func routes(serviceName string, store queryapi.LogStore) http.Handler {
	httpMetrics := metrics.NewHTTPCollector(serviceName)
	metricsHandler := metrics.NewHandler(serviceName, httpMetrics)

	mux := http.NewServeMux()
	mux.Handle("/metrics", metricsHandler)
	mux.Handle("/healthz", httpMetrics.Middleware("/healthz", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})))
	mux.Handle("/readyz", httpMetrics.Middleware("/readyz", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})))
	mux.Handle("/v1/status", httpMetrics.Middleware("/v1/status", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"service": serviceName,
			"time":    time.Now().UTC(),
			"status":  "bootstrap",
		})
	})))
	mux.Handle("/v1/logs", httpMetrics.Middleware("/v1/logs", queryapi.NewHandler(store)))
	return mux
}
