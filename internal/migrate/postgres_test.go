package migrate

import (
	"context"
	"io/fs"
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

type readFailFS struct{}

func (readFailFS) Open(string) (fs.File, error) {
	return nil, fs.ErrNotExist
}
