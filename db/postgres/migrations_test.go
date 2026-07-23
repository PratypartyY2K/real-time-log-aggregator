package postgresmigrations

import (
	"strings"
	"testing"
)

func TestNotificationDeliveryAuditMigrationIsEmbedded(t *testing.T) {
	payload, err := Files.ReadFile("002_notification_delivery_audit.sql")
	if err != nil {
		t.Fatalf("read embedded migration: %v", err)
	}
	sql := string(payload)
	for _, required := range []string{
		"notification_delivery_attempts",
		"payload_json",
		"available_at",
		"idx_notification_deliveries_due",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration does not contain %q", required)
		}
	}
}
