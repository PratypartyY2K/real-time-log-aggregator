package config

import (
	"reflect"
	"testing"
)

func TestLoadParsesClickHouseShardDSNs(t *testing.T) {
	t.Setenv("CLICKHOUSE_SHARD_DSNS", " http://shard-1:8123, http://shard-2:8123 ")

	cfg := Load("query-api", ":8081")

	want := []string{"http://shard-1:8123", "http://shard-2:8123"}
	if !reflect.DeepEqual(cfg.ClickHouseShardDSNs, want) {
		t.Fatalf("expected shard DSNs %v, got %v", want, cfg.ClickHouseShardDSNs)
	}
}
