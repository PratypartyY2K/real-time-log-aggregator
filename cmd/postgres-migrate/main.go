package main

import (
	"context"
	"database/sql"
	"log"

	postgresmigrations "github.com/PratypartyY2K/real-time-log-aggregator/db/postgres"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/migrate"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	cfg := config.Load("postgres-migrate", "")

	db, err := sql.Open("pgx", cfg.PostgresDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	migrations, err := migrate.LoadMigrations(postgresmigrations.Files)
	if err != nil {
		log.Fatal(err)
	}

	if err := migrate.RunPostgres(context.Background(), db, migrations); err != nil {
		log.Fatal(err)
	}

	log.Printf("applied postgres migrations from %d files", len(migrations))
}
