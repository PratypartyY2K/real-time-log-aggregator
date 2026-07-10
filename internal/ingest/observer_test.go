package ingest

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/logging"
)

func TestMetricsObserverTracksAuthOutcomes(t *testing.T) {
	observer := NewMetricsObserver(logging.New("error"))

	observer.ObserveAuth(context.Background(), AuthObservation{Outcome: AuthOutcomeAuthorized})
	observer.ObserveAuth(context.Background(), AuthObservation{Outcome: AuthOutcomeAuthorized})
	observer.ObserveAuth(context.Background(), AuthObservation{Outcome: AuthOutcomeMissingAPIKey})
	observer.ObserveAuth(context.Background(), AuthObservation{Outcome: AuthOutcomeInvalidAPIKey})
	observer.ObserveAuth(context.Background(), AuthObservation{Outcome: AuthOutcomeForbiddenScope})
	observer.ObserveAuth(context.Background(), AuthObservation{Outcome: AuthOutcomeBackendError})
	observer.ObserveAuth(context.Background(), AuthObservation{Outcome: AuthOutcomeAuthenticatorMissing})

	got := observer.Snapshot()
	if got.Authorized != 2 {
		t.Fatalf("expected authorized=2, got %d", got.Authorized)
	}
	if got.MissingAPIKey != 1 {
		t.Fatalf("expected missing_api_key=1, got %d", got.MissingAPIKey)
	}
	if got.InvalidAPIKey != 1 {
		t.Fatalf("expected invalid_api_key=1, got %d", got.InvalidAPIKey)
	}
	if got.ForbiddenScope != 1 {
		t.Fatalf("expected forbidden_scope=1, got %d", got.ForbiddenScope)
	}
	if got.BackendError != 1 {
		t.Fatalf("expected backend_error=1, got %d", got.BackendError)
	}
	if got.AuthenticatorMissing != 1 {
		t.Fatalf("expected authenticator_unavailable=1, got %d", got.AuthenticatorMissing)
	}
}

func TestMetricsObserverServesJSONSnapshot(t *testing.T) {
	observer := NewMetricsObserver(logging.New("error"))
	observer.ObserveAuth(context.Background(), AuthObservation{Outcome: AuthOutcomeAuthorized})

	req := httptest.NewRequest("GET", "/metricsz", nil)
	rec := httptest.NewRecorder()

	observer.ServeHTTP(rec, req)

	var payload MetricsSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode metrics payload: %v", err)
	}
	if payload.Authorized != 1 {
		t.Fatalf("expected authorized=1, got %d", payload.Authorized)
	}
}
