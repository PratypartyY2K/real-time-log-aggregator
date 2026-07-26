package alerts

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWebhookDispatcherPostsNotificationPayload(t *testing.T) {
	t.Parallel()

	var headers http.Header
	var payload WebhookDeliveryPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers = r.Header.Clone()
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode webhook payload: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	err := NewWebhookDispatcher(server.URL, server.Client()).Dispatch(context.Background(), NotificationDelivery{
		ID:              44,
		AlertInstanceID: 55,
	}, NotificationPayload{
		RuleID:        7,
		RuleName:      "payment errors",
		EventType:     AlertEventTriggered,
		Status:        AlertStatusFiring,
		DedupeKey:     "scope=all",
		ObservedAtUTC: time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("dispatch webhook: %v", err)
	}
	if payload.DeliveryID != 44 || payload.AlertInstanceID != 55 || payload.Notification.RuleID != 7 {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	if headers.Get("Content-Type") != "application/json" || headers.Get("X-Logagg-Delivery-Id") != "44" {
		t.Fatalf("unexpected headers: %+v", headers)
	}
}

func TestWebhookDispatcherReturnsErrorForNonSuccessStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "try again", http.StatusTooManyRequests)
	}))
	defer server.Close()

	err := NewWebhookDispatcher(server.URL, server.Client()).Dispatch(context.Background(), NotificationDelivery{ID: 44}, NotificationPayload{})
	if err == nil {
		t.Fatal("expected webhook error, got nil")
	}
}
