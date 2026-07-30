package migrate

import (
	"context"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"testing"
	"testing/fstest"
)

func TestLoadMigrationsSortsLexically(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"010_more.sql": &fstest.MapFile{Data: []byte("SELECT 2;")},
		"001_init.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
		"README.txt":   &fstest.MapFile{Data: []byte("ignore")},
	}

	migrations, err := LoadMigrations(fsys)
	if err != nil {
		t.Fatalf("LoadMigrations returned error: %v", err)
	}
	if len(migrations) != 2 {
		t.Fatalf("expected 2 migrations, got %d", len(migrations))
	}
	if migrations[0].Version != "001_init" || migrations[1].Version != "010_more" {
		t.Fatalf("unexpected order: %#v", migrations)
	}
}

func TestLoadMigrationsReadFailure(t *testing.T) {
	t.Parallel()

	_, err := LoadMigrations(readFailFS{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestRunPostgresRejectsNilDB(t *testing.T) {
	t.Parallel()

	err := RunPostgres(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestRunClickHouseAppliesOnlyPendingMigrations(t *testing.T) {
	t.Parallel()

	var queries []string
	runner := ClickHouseRunner{
		URL: "http://clickhouse.local",
		Client: &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read request body: %v", err)
			}
			query := string(payload)
			queries = append(queries, query)
			body := ""
			if strings.Contains(query, "SELECT version FROM schema_migrations") {
				body = "001_init\n"
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
	}

	err := runner.Run(context.Background(), []Migration{
		{Version: "001_init", Name: "001_init.sql", SQL: "CREATE TABLE skipped"},
		{Version: "002_logs", Name: "002_logs.sql", SQL: "CREATE TABLE logs; ALTER TABLE logs ADD COLUMN message String;\n-- trailing comment"},
	})
	if err != nil {
		t.Fatalf("RunClickHouse returned error: %v", err)
	}

	joined := strings.Join(queries, "\n")
	if strings.Contains(joined, "CREATE TABLE skipped") {
		t.Fatal("already applied migration was executed")
	}
	if !strings.Contains(joined, "CREATE TABLE logs") {
		t.Fatal("pending migration was not executed")
	}
	if !strings.Contains(joined, "ALTER TABLE logs") {
		t.Fatal("multi-statement migration was not fully executed")
	}
	for _, query := range queries {
		if strings.Contains(query, ";") {
			t.Fatalf("server received multiple statements: %q", query)
		}
	}
	if !strings.Contains(joined, "INSERT INTO schema_migrations") || !strings.Contains(joined, "'002_logs'") {
		t.Fatal("pending migration version was not recorded")
	}
}

func TestRunClickHouseRejectsMissingURL(t *testing.T) {
	t.Parallel()

	err := RunClickHouse(context.Background(), "", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

type readFailFS struct{}

func (readFailFS) Open(string) (fs.File, error) {
	return nil, fs.ErrNotExist
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
