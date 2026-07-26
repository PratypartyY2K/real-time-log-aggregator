package config

import (
	"reflect"
	"testing"
	"time"
)

func TestLoadParsesClickHouseShardDSNs(t *testing.T) {
	t.Setenv("CLICKHOUSE_SHARD_DSNS", " http://shard-1:8123, http://shard-2:8123 ")

	cfg := Load("query-api", ":8081")

	want := []string{"http://shard-1:8123", "http://shard-2:8123"}
	if !reflect.DeepEqual(cfg.ClickHouseShardDSNs, want) {
		t.Fatalf("expected shard DSNs %v, got %v", want, cfg.ClickHouseShardDSNs)
	}
}

func TestLoadParsesNotificationDeliverySettings(t *testing.T) {
	t.Setenv("NOTIFICATION_POLL_INTERVAL", "2s")
	t.Setenv("NOTIFICATION_RETRY_BASE", "15s")
	t.Setenv("NOTIFICATION_RETRY_MAX", "10m")
	t.Setenv("NOTIFICATION_LEASE_DURATION", "45s")
	t.Setenv("NOTIFICATION_MAX_ATTEMPTS", "8")
	t.Setenv("NOTIFICATION_BATCH_SIZE", "75")
	t.Setenv("NOTIFICATION_WEBHOOK_URL", "https://alerts.example.test/webhook")
	t.Setenv("ALERT_EVALUATION_INTERVAL", "10s")
	t.Setenv("ALERT_EVALUATION_MAX_RECORDS", "25000")

	cfg := Load("processor", "")
	if cfg.NotificationPollInterval != 2*time.Second ||
		cfg.NotificationRetryBase != 15*time.Second ||
		cfg.NotificationRetryMax != 10*time.Minute ||
		cfg.NotificationLeaseDuration != 45*time.Second ||
		cfg.NotificationMaxAttempts != 8 ||
		cfg.NotificationBatchSize != 75 ||
		cfg.NotificationWebhookURL != "https://alerts.example.test/webhook" ||
		cfg.AlertEvaluationInterval != 10*time.Second ||
		cfg.AlertEvaluationMaxRecords != 25000 {
		t.Fatalf("unexpected notification config: %+v", cfg)
	}
}
