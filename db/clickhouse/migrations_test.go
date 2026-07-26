package clickhousemigrations

import (
	"strings"
	"testing"
)

func TestDistributedLogsMigrationIsEmbedded(t *testing.T) {
	payload, err := Files.ReadFile("003_distributed_logs.sql")
	if err != nil {
		t.Fatalf("read embedded migration: %v", err)
	}
	sql := string(payload)
	for _, required := range []string{
		"logs_cluster",
		"logs_local",
		"Distributed",
		"logs_single_node",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration does not contain %q", required)
		}
	}
}
