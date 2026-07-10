package main

import (
	"database/sql"
	"log"
	"net/http"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/auth"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/ingest"
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

	service := app.NewHTTPService(cfg, routes(auth.NewPostgresAuthenticator(db), publisher))
	app.Run(service)
}

func routes(authenticator ingest.Authenticator, publisher ingest.Publisher) http.Handler {
	handler := ingest.NewHandler(ingest.Config{
		MaxBodyBytes:  1 << 20,
		Authenticator: authenticator,
		Publisher:     publisher,
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
