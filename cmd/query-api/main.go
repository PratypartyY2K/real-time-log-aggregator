package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/pratyushkumar/real-time-log-aggregator/internal/app"
	"github.com/pratyushkumar/real-time-log-aggregator/internal/config"
)

func main() {
	cfg := config.Load("query-api", ":8081")
	service := app.NewHTTPService(cfg, routes(cfg.ServiceName))
	app.Run(service)
}

func routes(serviceName string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"service": serviceName,
			"time":    time.Now().UTC(),
			"status":  "bootstrap",
		})
	})
	return mux
}
