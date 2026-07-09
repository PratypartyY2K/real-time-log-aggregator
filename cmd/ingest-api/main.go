package main

import (
	"log"
	"net/http"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/ingest"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/stream"
)

func main() {
	cfg := config.Load("ingest-api", ":8080")
	nc, publisher, err := stream.ConnectJetStream(cfg.NATSURL, cfg.NATSStream, cfg.NATSSubject)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	service := app.NewHTTPService(cfg, routes(publisher))
	app.Run(service)
}

func routes(publisher ingest.Publisher) http.Handler {
	handler := ingest.NewHandler(ingest.Config{
		MaxBodyBytes: 1 << 20,
		Publisher:    publisher,
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
