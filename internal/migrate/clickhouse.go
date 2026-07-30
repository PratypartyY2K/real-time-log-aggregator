package migrate

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	commonclickhouse "github.com/PratypartyY2K/real-time-log-aggregator/internal/clickhouse"
)

type ClickHouseRunner struct {
	URL    string
	Client *http.Client
}

func RunClickHouse(ctx context.Context, dsn string, migrations []Migration) error {
	return ClickHouseRunner{URL: dsn}.Run(ctx, migrations)
}

func (r ClickHouseRunner) Run(ctx context.Context, migrations []Migration) error {
	if strings.TrimSpace(r.URL) == "" {
		return fmt.Errorf("clickhouse url is not configured")
	}

	if err := r.exec(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations
(
    version String,
    applied_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY version`); err != nil {
		return fmt.Errorf("ensure clickhouse schema_migrations table: %w", err)
	}

	applied, err := r.clickHouseAppliedVersions(ctx)
	if err != nil {
		return err
	}

	for _, migration := range migrations {
		if _, ok := applied[migration.Version]; ok {
			continue
		}
		if err := r.exec(ctx, migration.SQL); err != nil {
			return fmt.Errorf("execute clickhouse migration %s: %w", migration.Name, err)
		}
		if err := r.exec(ctx, fmt.Sprintf("INSERT INTO schema_migrations (version) VALUES (%s)", clickHouseString(migration.Version))); err != nil {
			return fmt.Errorf("record clickhouse migration %s: %w", migration.Name, err)
		}
	}

	return nil
}

func (r ClickHouseRunner) clickHouseAppliedVersions(ctx context.Context) (map[string]struct{}, error) {
	body, err := r.query(ctx, `SELECT version FROM schema_migrations FORMAT TabSeparated`)
	if err != nil {
		return nil, fmt.Errorf("list clickhouse applied migrations: %w", err)
	}

	applied := map[string]struct{}{}
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			applied[line] = struct{}{}
		}
	}
	return applied, nil
}

func (r ClickHouseRunner) exec(ctx context.Context, query string) error {
	// ponytail: current migrations contain no semicolons inside literals; use a SQL parser if that changes.
	lines := strings.Split(query, "\n")
	query = ""
	for _, line := range lines {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			query += line + "\n"
		}
	}
	for _, statement := range strings.Split(query, ";") {
		if statement = strings.TrimSpace(statement); statement != "" {
			if _, err := r.do(ctx, statement); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r ClickHouseRunner) query(ctx context.Context, query string) (string, error) {
	return r.do(ctx, query)
}

func (r ClickHouseRunner) do(ctx context.Context, query string) (string, error) {
	client := r.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	body, err := commonclickhouse.Do(ctx, client, r.URL, query)
	if err != nil {
		return "", fmt.Errorf("run clickhouse query: %w", err)
	}
	defer body.Close()

	payload, err := io.ReadAll(body)
	if err != nil {
		return "", fmt.Errorf("read clickhouse response: %w", err)
	}

	return string(payload), nil
}

func clickHouseString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
