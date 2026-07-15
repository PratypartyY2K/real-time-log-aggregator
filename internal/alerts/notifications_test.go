package alerts

import (
	"database/sql"
	"testing"
)

func TestNotificationDestinationPrefersOwner(t *testing.T) {
	t.Parallel()

	channel, target := notificationDestination(Rule{
		TenantID:    42,
		ServiceName: sql.NullString{String: "checkout", Valid: true},
		Environment: sql.NullString{String: "prod", Valid: true},
		Owner:       sql.NullString{String: "ops@example.com", Valid: true},
	})
	if channel != DefaultNotificationChannel || target != "ops@example.com" {
		t.Fatalf("unexpected destination: %s %s", channel, target)
	}
}

func TestNotificationDestinationFallsBackToServiceEnv(t *testing.T) {
	t.Parallel()

	_, target := notificationDestination(Rule{
		TenantID:    42,
		ServiceName: sql.NullString{String: "checkout", Valid: true},
		Environment: sql.NullString{String: "prod", Valid: true},
	})
	if target != "checkout:prod" {
		t.Fatalf("unexpected target: %s", target)
	}
}

func TestNotificationDestinationFallsBackToTenant(t *testing.T) {
	t.Parallel()

	_, target := notificationDestination(Rule{TenantID: 42})
	if target != "tenant:42" {
		t.Fatalf("unexpected target: %s", target)
	}
}
