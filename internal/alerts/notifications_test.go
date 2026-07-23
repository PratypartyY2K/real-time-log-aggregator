package alerts

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
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

func TestRetryDelayUsesExponentialBackoffWithinCap(t *testing.T) {
	t.Parallel()
	base := 30 * time.Second
	maximum := 2 * time.Minute
	first := retryDelay(10, 1, base, maximum)
	third := retryDelay(10, 3, base, maximum)
	fifth := retryDelay(10, 5, base, maximum)
	if first < base || third <= first {
		t.Fatalf("expected increasing retry delays, got first=%s third=%s", first, third)
	}
	if fifth > maximum {
		t.Fatalf("expected capped retry delay, got %s", fifth)
	}
}

func TestDeliveryPolicyNormalizesUnsafeValues(t *testing.T) {
	t.Parallel()
	policy := (DeliveryPolicy{BatchSize: 1000}).normalized()
	if policy.WorkerID == "" || policy.MaxAttempts != DefaultDeliveryAttempts || policy.BatchSize != 500 {
		t.Fatalf("unexpected normalized policy: %+v", policy)
	}
}

func TestRunDeliveryWorkerPollsImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &stubDeliveryBatchStore{called: make(chan struct{}, 1)}
	go RunDeliveryWorker(ctx, nil, store, stubDispatcher{}, DeliveryPolicy{WorkerID: "test"}, time.Hour)
	select {
	case <-store.called:
	case <-time.After(time.Second):
		t.Fatal("expected immediate delivery poll")
	}
	cancel()
}

func TestNewDeliveryWorkerIDIncludesServiceAndProcessIdentity(t *testing.T) {
	t.Parallel()
	workerID := NewDeliveryWorkerID("processor")
	if !strings.HasPrefix(workerID, "processor:") {
		t.Fatalf("unexpected worker id %q", workerID)
	}
}

type stubDeliveryBatchStore struct{ called chan struct{} }

func (s *stubDeliveryBatchStore) DispatchNotificationBatch(context.Context, NotificationDispatcher, DeliveryPolicy, time.Time) error {
	select {
	case s.called <- struct{}{}:
	default:
	}
	return nil
}

type stubDispatcher struct{}

func (s stubDispatcher) Dispatch(context.Context, NotificationDelivery, NotificationPayload) error {
	return nil
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
