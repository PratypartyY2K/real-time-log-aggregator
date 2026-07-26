package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"strings"

	clickhousemigrations "github.com/PratypartyY2K/real-time-log-aggregator/db/clickhouse"
	postgresmigrations "github.com/PratypartyY2K/real-time-log-aggregator/db/postgres"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/migrate"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	target := flag.String("target", "all", "migration target: all, postgres, or clickhouse")
	flag.Parse()

	cfg := config.Load("migrate", "")
	ctx := context.Background()

	switch strings.ToLower(strings.TrimSpace(*target)) {
	case "all":
		runPostgres(ctx, cfg)
		runClickHouse(ctx, cfg)
	case "postgres":
		runPostgres(ctx, cfg)
	case "clickhouse":
		runClickHouse(ctx, cfg)
	default:
		log.Fatalf("unknown migration target %q", *target)
	}
}

func runPostgres(ctx context.Context, cfg config.Config) {
	db, err := sql.Open("pgx", cfg.PostgresDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		log.Fatal(err)
	}

	migrations, err := migrate.LoadMigrations(postgresmigrations.Files)
	if err != nil {
		log.Fatal(err)
	}

	if err := migrate.RunPostgres(ctx, db, migrations); err != nil {
		log.Fatal(err)
	}

	log.Printf("applied postgres migrations from %d files", len(migrations))
}

func runClickHouse(ctx context.Context, cfg config.Config) {
	migrations, err := migrate.LoadMigrations(clickhousemigrations.Files)
	if err != nil {
		log.Fatal(err)
	}

	if err := migrate.RunClickHouse(ctx, cfg.ClickHouseDSN, migrations); err != nil {
		log.Fatal(err)
	}

	log.Printf("applied clickhouse migrations from %d files", len(migrations))
}
