package queryapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAlertHistoryHandlerScopesQueryToTenant(t *testing.T) {
	store := &stubAlertHistoryStore{alerts: []AlertHistoryItem{{AlertInstanceID: 1, RuleID: 2, RuleName: "error rate", Status: "active"}}}
	req := tenantRequest(httptest.NewRequest(http.MethodGet, "/v1/alerts/history?start=2026-07-01T00:00:00Z&end=2026-07-02T00:00:00Z&rule_id=2&status=active&limit=25", nil))
	rec := httptest.NewRecorder()
	NewAlertHistoryHandler(store).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if store.query.TenantID != 7 || store.query.RuleID != 2 || store.query.Status != "active" || store.query.Limit != 25 {
		t.Fatalf("unexpected query: %+v", store.query)
	}
	var response map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil || response["alerts"] == nil {
		t.Fatalf("unexpected response: %s err=%v", rec.Body.String(), err)
	}
}

func TestAlertAuditHandlerReturnsCombinedTimeline(t *testing.T) {
	store := &stubAlertHistoryStore{audit: []AlertAuditEntry{{ID: "attempt:1", EventType: "notification_attempt"}}}
	req := tenantRequest(httptest.NewRequest(http.MethodGet, "/v1/alerts/audit?start=2026-07-01T00:00:00Z&end=2026-07-02T00:00:00Z&event_type=notification_attempt", nil))
	rec := httptest.NewRecorder()
	NewAlertAuditHandler(store).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "notification_attempt") {
		t.Fatalf("unexpected response %d: %s", rec.Code, rec.Body.String())
	}
	if store.query.EventType != "notification_attempt" {
		t.Fatalf("unexpected query: %+v", store.query)
	}
}

func TestNotificationDeliveryHistoryHandlerReturnsTrackingState(t *testing.T) {
	store := &stubAlertHistoryStore{deliveries: []NotificationDeliveryItem{{ID: 4, Status: "retrying", AttemptCount: 2, MaxAttempts: 5}}}
	req := tenantRequest(httptest.NewRequest(http.MethodGet, "/v1/alerts/deliveries?start=2026-07-01T00:00:00Z&end=2026-07-02T00:00:00Z&status=retrying", nil))
	rec := httptest.NewRecorder()
	NewNotificationDeliveryHistoryHandler(store).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "retrying") {
		t.Fatalf("unexpected response %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAlertHistoryHandlerRejectsWindowOverNinetyDays(t *testing.T) {
	req := tenantRequest(httptest.NewRequest(http.MethodGet, "/v1/alerts/history?start=2026-01-01T00:00:00Z&end=2026-07-01T00:00:00Z", nil))
	rec := httptest.NewRecorder()
	NewAlertHistoryHandler(&stubAlertHistoryStore{}).ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestAlertHistorySQLAlwaysIncludesTenantPredicate(t *testing.T) {
	query := AlertHistoryQuery{TenantID: 7, Start: time.Now().Add(-time.Hour), End: time.Now(), Limit: 100}
	historySQL, historyArgs := buildAlertHistorySQL(query)
	auditSQL, auditArgs := buildAlertAuditSQL(query)
	deliverySQL, deliveryArgs := buildNotificationDeliveryHistorySQL(query)
	for name, statement := range map[string]string{"history": historySQL, "audit": auditSQL, "delivery": deliverySQL} {
		if !strings.Contains(statement, "tenant_id = $1") {
			t.Fatalf("%s query lacks tenant predicate: %s", name, statement)
		}
	}
	if historyArgs[0] != uint64(7) || auditArgs[0] != uint64(7) || deliveryArgs[0] != uint64(7) {
		t.Fatal("expected tenant id as first query argument")
	}
}

type stubAlertHistoryStore struct {
	query      AlertHistoryQuery
	alerts     []AlertHistoryItem
	audit      []AlertAuditEntry
	deliveries []NotificationDeliveryItem
	err        error
}

func (s *stubAlertHistoryStore) QueryAlertHistory(_ context.Context, query AlertHistoryQuery) ([]AlertHistoryItem, error) {
	s.query = query
	return s.alerts, s.err
}
func (s *stubAlertHistoryStore) QueryAlertAudit(_ context.Context, query AlertHistoryQuery) ([]AlertAuditEntry, error) {
	s.query = query
	return s.audit, s.err
}
func (s *stubAlertHistoryStore) QueryNotificationDeliveries(_ context.Context, query AlertHistoryQuery) ([]NotificationDeliveryItem, error) {
	s.query = query
	return s.deliveries, s.err
}
