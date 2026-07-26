package queryapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAlertRuleHandlerCreatesRule(t *testing.T) {
	store := &stubAlertRuleStore{}
	req := tenantRequest(httptest.NewRequest(http.MethodPost, "/v1/alerts", strings.NewReader(`{
		"service":"payment-api",
		"environment":"prod",
		"log_level":"ERROR",
		"fingerprint":"payment-db-timeout",
		"name":"payment errors",
		"rule_type":"count_threshold",
		"severity":"high",
		"threshold":"20",
		"window_seconds":300
	}`)))
	rec := httptest.NewRecorder()

	NewAlertRuleHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	if store.created.TenantID != 7 || store.created.Service != "payment-api" || store.created.Environment != "prod" || store.created.LogLevel != "ERROR" || store.created.Threshold != "20" || store.created.WindowSeconds != 300 {
		t.Fatalf("unexpected create mutation: %+v", store.created)
	}
}

func TestAlertRuleHandlerListsRules(t *testing.T) {
	store := &stubAlertRuleStore{rules: []AlertRule{{ID: 9, TenantID: 7, Service: "payment-api", Environment: "prod", Name: "payment errors"}}}
	req := tenantRequest(httptest.NewRequest(http.MethodGet, "/v1/alerts?service=payment-api&environment=prod&status=active&limit=20", nil))
	rec := httptest.NewRecorder()

	NewAlertRuleHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if store.listFilter.TenantID != 7 || store.listFilter.Service != "payment-api" || store.listFilter.Environment != "prod" || store.listFilter.Status != "active" || store.listFilter.Limit != 20 {
		t.Fatalf("unexpected list filter: %+v", store.listFilter)
	}
	var payload alertRuleListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Count != 1 || payload.Rules[0].ID != 9 {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestAlertRuleHandlerGetsRule(t *testing.T) {
	store := &stubAlertRuleStore{rule: AlertRule{ID: 12, TenantID: 7, Name: "payment errors"}, found: true}
	req := tenantRequest(httptest.NewRequest(http.MethodGet, "/v1/alerts/12", nil))
	rec := httptest.NewRecorder()

	NewAlertRuleHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if store.gotTenantID != 7 || store.gotID != 12 {
		t.Fatalf("unexpected get arguments: tenant=%d id=%d", store.gotTenantID, store.gotID)
	}
}

func TestAlertRuleHandlerPatchesRule(t *testing.T) {
	store := &stubAlertRuleStore{rule: AlertRule{ID: 12, TenantID: 7, Threshold: "25"}, found: true}
	req := tenantRequest(httptest.NewRequest(http.MethodPatch, "/v1/alerts/12", strings.NewReader(`{"threshold":"25","status":"disabled"}`)))
	rec := httptest.NewRecorder()

	NewAlertRuleHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if store.patchedID != 12 || store.patched.TenantID != 7 || store.patched.Threshold == nil || *store.patched.Threshold != "25" || store.patched.Status == nil || *store.patched.Status != "disabled" {
		t.Fatalf("unexpected patch: id=%d patch=%+v", store.patchedID, store.patched)
	}
}

func TestAlertRuleHandlerDeletesRule(t *testing.T) {
	store := &stubAlertRuleStore{found: true}
	req := tenantRequest(httptest.NewRequest(http.MethodDelete, "/v1/alerts/12", nil))
	rec := httptest.NewRecorder()

	NewAlertRuleHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}
	if store.deletedTenantID != 7 || store.deletedID != 12 {
		t.Fatalf("unexpected delete arguments: tenant=%d id=%d", store.deletedTenantID, store.deletedID)
	}
}

func TestAlertRuleHandlerRejectsInvalidCreate(t *testing.T) {
	req := tenantRequest(httptest.NewRequest(http.MethodPost, "/v1/alerts", strings.NewReader(`{"service":"payment-api"}`)))
	rec := httptest.NewRecorder()

	NewAlertRuleHandler(&stubAlertRuleStore{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

type stubAlertRuleStore struct {
	created         AlertRuleMutation
	listFilter      AlertRuleListFilter
	rules           []AlertRule
	rule            AlertRule
	found           bool
	gotTenantID     uint64
	gotID           int64
	patchedID       int64
	patched         AlertRulePatch
	deletedTenantID uint64
	deletedID       int64
}

func (s *stubAlertRuleStore) CreateAlertRule(_ context.Context, mutation AlertRuleMutation) (AlertRule, error) {
	s.created = mutation
	return AlertRule{ID: 1, TenantID: mutation.TenantID, Service: mutation.Service, Environment: mutation.Environment, LogLevel: mutation.LogLevel, Fingerprint: mutation.Fingerprint, Name: mutation.Name, RuleType: mutation.RuleType, Severity: mutation.Severity, Threshold: mutation.Threshold, WindowSeconds: mutation.WindowSeconds, Status: "active"}, nil
}

func (s *stubAlertRuleStore) ListAlertRules(_ context.Context, filter AlertRuleListFilter) ([]AlertRule, error) {
	s.listFilter = filter
	return s.rules, nil
}

func (s *stubAlertRuleStore) GetAlertRule(_ context.Context, tenantID uint64, id int64) (AlertRule, bool, error) {
	s.gotTenantID = tenantID
	s.gotID = id
	return s.rule, s.found, nil
}

func (s *stubAlertRuleStore) UpdateAlertRule(_ context.Context, id int64, patch AlertRulePatch) (AlertRule, bool, error) {
	s.patchedID = id
	s.patched = patch
	return s.rule, s.found, nil
}

func (s *stubAlertRuleStore) DeleteAlertRule(_ context.Context, tenantID uint64, id int64) (bool, error) {
	s.deletedTenantID = tenantID
	s.deletedID = id
	return s.found, nil
}
