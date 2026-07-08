package main

import (
	"net/http"

	"github.com/pratyushkumar/real-time-log-aggregator/internal/app"
	"github.com/pratyushkumar/real-time-log-aggregator/internal/config"
	"github.com/pratyushkumar/real-time-log-aggregator/internal/ingest"
)

func main() {
	cfg := config.Load("ingest-api", ":8080")
	service := app.NewHTTPService(cfg, routes())
	app.Run(service)
}

func routes() http.Handler {
	handler := ingest.NewHandler(ingest.Config{
		MaxBodyBytes: 1 << 20,
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	mux.HandleFunc("/v1/logs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		handler.ServeHTTP(w, r)
	})
	return mux
}

var _ app.Service = (*app.HTTPService)(nil)
