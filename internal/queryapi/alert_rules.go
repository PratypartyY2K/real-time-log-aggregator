package queryapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

const maxAlertRuleLimit = 500

type AlertRuleStore interface {
	CreateAlertRule(context.Context, AlertRuleMutation) (AlertRule, error)
	ListAlertRules(context.Context, AlertRuleListFilter) ([]AlertRule, error)
	GetAlertRule(context.Context, uint64, int64) (AlertRule, bool, error)
	UpdateAlertRule(context.Context, int64, AlertRulePatch) (AlertRule, bool, error)
	DeleteAlertRule(context.Context, uint64, int64) (bool, error)
}

type AlertRuleHandler struct {
	store AlertRuleStore
}

type AlertRule struct {
	ID              int64           `json:"id"`
	TenantID        uint64          `json:"tenant_id"`
	Service         string          `json:"service,omitempty"`
	Environment     string          `json:"environment,omitempty"`
	LogLevel        string          `json:"log_level,omitempty"`
	Fingerprint     string          `json:"fingerprint,omitempty"`
	Name            string          `json:"name"`
	RuleType        string          `json:"rule_type"`
	Severity        string          `json:"severity"`
	FilterJSON      json.RawMessage `json:"filter_json"`
	GroupBy         json.RawMessage `json:"group_by"`
	WindowSeconds   int             `json:"window_seconds"`
	Threshold       string          `json:"threshold"`
	CooldownSeconds int             `json:"cooldown_seconds"`
	Status          string          `json:"status"`
}

type AlertRuleMutation struct {
	TenantID        uint64
	Service         string          `json:"service"`
	Environment     string          `json:"environment"`
	LogLevel        string          `json:"log_level"`
	Fingerprint     string          `json:"fingerprint"`
	Name            string          `json:"name"`
	RuleType        string          `json:"rule_type"`
	Severity        string          `json:"severity"`
	FilterJSON      json.RawMessage `json:"filter_json"`
	GroupBy         json.RawMessage `json:"group_by"`
	WindowSeconds   int             `json:"window_seconds"`
	Threshold       string          `json:"threshold"`
	CooldownSeconds int             `json:"cooldown_seconds"`
	Status          string          `json:"status"`
}

type AlertRulePatch struct {
	TenantID        uint64
	Service         *string          `json:"service"`
	Environment     *string          `json:"environment"`
	LogLevel        *string          `json:"log_level"`
	Fingerprint     *string          `json:"fingerprint"`
	Name            *string          `json:"name"`
	RuleType        *string          `json:"rule_type"`
	Severity        *string          `json:"severity"`
	FilterJSON      *json.RawMessage `json:"filter_json"`
	GroupBy         *json.RawMessage `json:"group_by"`
	WindowSeconds   *int             `json:"window_seconds"`
	Threshold       *string          `json:"threshold"`
	CooldownSeconds *int             `json:"cooldown_seconds"`
	Status          *string          `json:"status"`
}

type AlertRuleListFilter struct {
	TenantID    uint64
	Service     string
	Environment string
	Status      string
	Limit       int
}

type alertRuleListResponse struct {
	Rules []AlertRule `json:"rules"`
	Count int         `json:"count"`
}

func NewAlertRuleHandler(store AlertRuleStore) *AlertRuleHandler {
	return &AlertRuleHandler{store: store}
}

func (h *AlertRuleHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "alert rule store unavailable")
		return
	}
	tenantID, ok := TenantIDFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "tenant identity required")
		return
	}

	id, hasID, err := alertRuleIDFromPath(r.URL.Path)
	if err != nil {
		writeError(w, http.StatusNotFound, "alert rule not found")
		return
	}

	switch {
	case !hasID && r.Method == http.MethodPost:
		h.create(w, r, tenantID)
	case !hasID && r.Method == http.MethodGet:
		h.list(w, r, tenantID)
	case hasID && r.Method == http.MethodGet:
		h.get(w, r, tenantID, id)
	case hasID && r.Method == http.MethodPatch:
		h.patch(w, r, tenantID, id)
	case hasID && r.Method == http.MethodDelete:
		h.delete(w, r, tenantID, id)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *AlertRuleHandler) create(w http.ResponseWriter, r *http.Request, tenantID uint64) {
	var mutation AlertRuleMutation
	if err := decodeJSONBody(r, &mutation); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	mutation.TenantID = tenantID
	if err := validateAlertRuleMutation(mutation); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	rule, err := h.store.CreateAlertRule(r.Context(), mutation)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to create alert rule")
		return
	}
	writeJSON(w, http.StatusCreated, rule)
}

func (h *AlertRuleHandler) list(w http.ResponseWriter, r *http.Request, tenantID uint64) {
	filter, err := parseAlertRuleListFilter(r, tenantID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	rules, err := h.store.ListAlertRules(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to list alert rules")
		return
	}
	writeJSON(w, http.StatusOK, alertRuleListResponse{Rules: rules, Count: len(rules)})
}

func (h *AlertRuleHandler) get(w http.ResponseWriter, r *http.Request, tenantID uint64, id int64) {
	rule, found, err := h.store.GetAlertRule(r.Context(), tenantID, id)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to get alert rule")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "alert rule not found")
		return
	}
	writeJSON(w, http.StatusOK, rule)
}

func (h *AlertRuleHandler) patch(w http.ResponseWriter, r *http.Request, tenantID uint64, id int64) {
	var patch AlertRulePatch
	if err := decodeJSONBody(r, &patch); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	patch.TenantID = tenantID
	if err := validateAlertRulePatch(patch); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	rule, found, err := h.store.UpdateAlertRule(r.Context(), id, patch)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to update alert rule")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "alert rule not found")
		return
	}
	writeJSON(w, http.StatusOK, rule)
}

func (h *AlertRuleHandler) delete(w http.ResponseWriter, r *http.Request, tenantID uint64, id int64) {
	found, err := h.store.DeleteAlertRule(r.Context(), tenantID, id)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to delete alert rule")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "alert rule not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func alertRuleIDFromPath(path string) (int64, bool, error) {
	path = strings.TrimSuffix(path, "/")
	if path == "/v1/alerts" || path == "/alerts" {
		return 0, false, nil
	}
	for _, prefix := range []string{"/v1/alerts/", "/alerts/"} {
		if strings.HasPrefix(path, prefix) {
			rawID := strings.TrimPrefix(path, prefix)
			if rawID == "" || strings.Contains(rawID, "/") {
				return 0, false, errors.New("invalid alert rule path")
			}
			id, err := strconv.ParseInt(rawID, 10, 64)
			if err != nil || id <= 0 {
				return 0, false, errors.New("invalid alert rule id")
			}
			return id, true, nil
		}
	}
	return 0, false, errors.New("invalid alert rule path")
}

func decodeJSONBody(r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errors.New("request body must be valid json")
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("request body must contain a single json object")
	}
	return nil
}

func validateAlertRuleMutation(rule AlertRuleMutation) error {
	rule.FilterJSON = defaultJSONObject(rule.FilterJSON)
	rule.GroupBy = defaultJSONArray(rule.GroupBy)
	switch {
	case strings.TrimSpace(rule.Service) == "":
		return errors.New("service is required")
	case strings.TrimSpace(rule.Environment) == "":
		return errors.New("environment is required")
	case strings.TrimSpace(rule.Name) == "":
		return errors.New("name is required")
	case strings.TrimSpace(rule.RuleType) == "":
		return errors.New("rule_type is required")
	case strings.TrimSpace(rule.Severity) == "":
		return errors.New("severity is required")
	case rule.WindowSeconds <= 0:
		return errors.New("window_seconds must be positive")
	case rule.CooldownSeconds < 0:
		return errors.New("cooldown_seconds must be non-negative")
	case strings.TrimSpace(rule.Threshold) == "":
		return errors.New("threshold is required")
	}
	return validateAlertRuleFields(rule.Service, rule.Environment, rule.LogLevel, rule.Fingerprint, rule.Status, false, rule.FilterJSON, rule.GroupBy)
}

func validateAlertRulePatch(patch AlertRulePatch) error {
	if patch.Service != nil && strings.TrimSpace(*patch.Service) == "" {
		return errors.New("service cannot be empty")
	}
	if patch.Environment != nil && strings.TrimSpace(*patch.Environment) == "" {
		return errors.New("environment cannot be empty")
	}
	if patch.Name != nil && strings.TrimSpace(*patch.Name) == "" {
		return errors.New("name cannot be empty")
	}
	if patch.RuleType != nil && strings.TrimSpace(*patch.RuleType) == "" {
		return errors.New("rule_type cannot be empty")
	}
	if patch.Severity != nil && strings.TrimSpace(*patch.Severity) == "" {
		return errors.New("severity cannot be empty")
	}
	if patch.WindowSeconds != nil && *patch.WindowSeconds <= 0 {
		return errors.New("window_seconds must be positive")
	}
	if patch.CooldownSeconds != nil && *patch.CooldownSeconds < 0 {
		return errors.New("cooldown_seconds must be non-negative")
	}
	if patch.Threshold != nil && strings.TrimSpace(*patch.Threshold) == "" {
		return errors.New("threshold cannot be empty")
	}
	filterJSON := json.RawMessage(nil)
	if patch.FilterJSON != nil {
		filterJSON = *patch.FilterJSON
	}
	groupBy := json.RawMessage(nil)
	if patch.GroupBy != nil {
		groupBy = *patch.GroupBy
	}
	return validateAlertRuleFields(stringValue(patch.Service), stringValue(patch.Environment), stringValue(patch.LogLevel), stringValue(patch.Fingerprint), stringValue(patch.Status), false, filterJSON, groupBy)
}

func validateAlertRuleFields(service, environment, logLevel, fingerprint, status string, allowDeletedStatus bool, filterJSON, groupBy json.RawMessage) error {
	for name, value := range map[string]string{"service": service, "environment": environment, "log_level": logLevel, "fingerprint": fingerprint} {
		if strings.TrimSpace(value) != "" && !isSafeTagFilter(strings.TrimSpace(value)) {
			return fmt.Errorf("%s contains unsupported characters", name)
		}
	}
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "", "active", "disabled":
	case "deleted":
		if !allowDeletedStatus {
			return errors.New("status must be active or disabled")
		}
	default:
		if allowDeletedStatus {
			return errors.New("status must be active, disabled, or deleted")
		}
		return errors.New("status must be active or disabled")
	}
	if len(filterJSON) > 0 && !json.Valid(filterJSON) {
		return errors.New("filter_json must be valid json")
	}
	if len(groupBy) > 0 && !json.Valid(groupBy) {
		return errors.New("group_by must be valid json")
	}
	return nil
}

func parseAlertRuleListFilter(r *http.Request, tenantID uint64) (AlertRuleListFilter, error) {
	values := r.URL.Query()
	limit, err := parseBoundedPositiveInt(values.Get("limit"), defaultLimit, maxAlertRuleLimit, "limit")
	if err != nil {
		return AlertRuleListFilter{}, err
	}
	filter := AlertRuleListFilter{
		TenantID:    tenantID,
		Service:     strings.TrimSpace(values.Get("service")),
		Environment: strings.TrimSpace(values.Get("environment")),
		Status:      strings.ToLower(strings.TrimSpace(values.Get("status"))),
		Limit:       limit,
	}
	if err := validateAlertRuleFields(filter.Service, filter.Environment, "", "", filter.Status, false, nil, nil); err != nil {
		return AlertRuleListFilter{}, err
	}
	return filter, nil
}

func defaultJSONObject(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`{}`)
	}
	return raw
}

func defaultJSONArray(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`[]`)
	}
	return raw
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

type PostgresAlertRuleStore struct {
	db *sql.DB
}

func NewPostgresAlertRuleStore(db *sql.DB) *PostgresAlertRuleStore {
	return &PostgresAlertRuleStore{db: db}
}

func (s *PostgresAlertRuleStore) CreateAlertRule(ctx context.Context, mutation AlertRuleMutation) (AlertRule, error) {
	if s == nil || s.db == nil {
		return AlertRule{}, errors.New("postgres alert rule store is not configured")
	}
	serviceID, err := s.serviceID(ctx, mutation.TenantID, mutation.Service, mutation.Environment)
	if err != nil {
		return AlertRule{}, err
	}
	status := strings.ToLower(strings.TrimSpace(mutation.Status))
	if status == "" {
		status = "active"
	}
	cooldownSeconds := mutation.CooldownSeconds
	if cooldownSeconds == 0 {
		cooldownSeconds = 600
	}
	const stmt = `
INSERT INTO alert_rules
    (tenant_id, service_id, name, rule_type, severity, log_level, fingerprint, filter_json, group_by, window_seconds, threshold, cooldown_seconds, status)
VALUES
    ($1, $2, $3, $4, $5, NULLIF($6, ''), NULLIF($7, ''), $8::jsonb, $9::jsonb, $10, $11::numeric, $12, $13)
RETURNING id
`
	var id int64
	if err := s.db.QueryRowContext(ctx, stmt, mutation.TenantID, serviceID, strings.TrimSpace(mutation.Name), strings.TrimSpace(mutation.RuleType), strings.TrimSpace(mutation.Severity), strings.ToLower(strings.TrimSpace(mutation.LogLevel)), strings.TrimSpace(mutation.Fingerprint), string(defaultJSONObject(mutation.FilterJSON)), string(defaultJSONArray(mutation.GroupBy)), mutation.WindowSeconds, strings.TrimSpace(mutation.Threshold), cooldownSeconds, status).Scan(&id); err != nil {
		return AlertRule{}, err
	}
	rule, _, err := s.GetAlertRule(ctx, mutation.TenantID, id)
	return rule, err
}

func (s *PostgresAlertRuleStore) ListAlertRules(ctx context.Context, filter AlertRuleListFilter) ([]AlertRule, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("postgres alert rule store is not configured")
	}
	const stmt = `
SELECT
    ar.id, ar.tenant_id, COALESCE(svc.name, ''), COALESCE(svc.environment, ''),
    COALESCE(ar.log_level, ''), COALESCE(ar.fingerprint, ''), ar.name, ar.rule_type, ar.severity,
    ar.filter_json::text, ar.group_by::text, ar.window_seconds, ar.threshold::text, ar.cooldown_seconds, ar.status
FROM alert_rules ar
LEFT JOIN services svc ON svc.id = ar.service_id
WHERE ar.tenant_id = $1
  AND ar.status <> 'deleted'
  AND ($2 = '' OR svc.name = $2)
  AND ($3 = '' OR svc.environment = $3)
  AND ($4 = '' OR ar.status = $4)
ORDER BY ar.id ASC
LIMIT $5
`
	rows, err := s.db.QueryContext(ctx, stmt, filter.TenantID, filter.Service, filter.Environment, filter.Status, filter.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rules := make([]AlertRule, 0)
	for rows.Next() {
		rule, err := scanAlertRule(rows)
		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}
	return rules, rows.Err()
}

func (s *PostgresAlertRuleStore) GetAlertRule(ctx context.Context, tenantID uint64, id int64) (AlertRule, bool, error) {
	if s == nil || s.db == nil {
		return AlertRule{}, false, errors.New("postgres alert rule store is not configured")
	}
	const stmt = `
SELECT
    ar.id, ar.tenant_id, COALESCE(svc.name, ''), COALESCE(svc.environment, ''),
    COALESCE(ar.log_level, ''), COALESCE(ar.fingerprint, ''), ar.name, ar.rule_type, ar.severity,
    ar.filter_json::text, ar.group_by::text, ar.window_seconds, ar.threshold::text, ar.cooldown_seconds, ar.status
FROM alert_rules ar
LEFT JOIN services svc ON svc.id = ar.service_id
WHERE ar.tenant_id = $1 AND ar.id = $2 AND ar.status <> 'deleted'
`
	rule, err := scanAlertRule(s.db.QueryRowContext(ctx, stmt, tenantID, id))
	if errors.Is(err, sql.ErrNoRows) {
		return AlertRule{}, false, nil
	}
	return rule, err == nil, err
}

func (s *PostgresAlertRuleStore) UpdateAlertRule(ctx context.Context, id int64, patch AlertRulePatch) (AlertRule, bool, error) {
	current, found, err := s.GetAlertRule(ctx, patch.TenantID, id)
	if err != nil || !found {
		return AlertRule{}, found, err
	}
	mutation := alertRuleMutationFromCurrent(current)
	applyAlertRulePatch(&mutation, patch)
	serviceID, err := s.serviceID(ctx, mutation.TenantID, mutation.Service, mutation.Environment)
	if err != nil {
		return AlertRule{}, false, err
	}
	const stmt = `
UPDATE alert_rules
SET service_id = $3,
    name = $4,
    rule_type = $5,
    severity = $6,
    log_level = NULLIF($7, ''),
    fingerprint = NULLIF($8, ''),
    filter_json = $9::jsonb,
    group_by = $10::jsonb,
    window_seconds = $11,
    threshold = $12::numeric,
    cooldown_seconds = $13,
    status = $14,
    updated_at = NOW()
WHERE tenant_id = $1 AND id = $2 AND status <> 'deleted'
RETURNING id
`
	var updatedID int64
	err = s.db.QueryRowContext(ctx, stmt, mutation.TenantID, id, serviceID, mutation.Name, mutation.RuleType, mutation.Severity, strings.ToLower(strings.TrimSpace(mutation.LogLevel)), strings.TrimSpace(mutation.Fingerprint), string(defaultJSONObject(mutation.FilterJSON)), string(defaultJSONArray(mutation.GroupBy)), mutation.WindowSeconds, mutation.Threshold, mutation.CooldownSeconds, mutation.Status).Scan(&updatedID)
	if errors.Is(err, sql.ErrNoRows) {
		return AlertRule{}, false, nil
	}
	if err != nil {
		return AlertRule{}, false, err
	}
	rule, _, err := s.GetAlertRule(ctx, mutation.TenantID, updatedID)
	return rule, err == nil, err
}

func (s *PostgresAlertRuleStore) DeleteAlertRule(ctx context.Context, tenantID uint64, id int64) (bool, error) {
	if s == nil || s.db == nil {
		return false, errors.New("postgres alert rule store is not configured")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE alert_rules SET status = 'deleted', updated_at = NOW() WHERE tenant_id = $1 AND id = $2 AND status <> 'deleted'`, tenantID, id)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected > 0, err
}

func (s *PostgresAlertRuleStore) serviceID(ctx context.Context, tenantID uint64, service, environment string) (int64, error) {
	const stmt = `
INSERT INTO services (tenant_id, name, environment)
VALUES ($1, $2, $3)
ON CONFLICT (tenant_id, name, environment) DO UPDATE SET name = EXCLUDED.name
RETURNING id
`
	var id int64
	err := s.db.QueryRowContext(ctx, stmt, tenantID, strings.TrimSpace(service), strings.TrimSpace(environment)).Scan(&id)
	return id, err
}

func scanAlertRule(row interface{ Scan(dest ...any) error }) (AlertRule, error) {
	var rule AlertRule
	if err := row.Scan(&rule.ID, &rule.TenantID, &rule.Service, &rule.Environment, &rule.LogLevel, &rule.Fingerprint, &rule.Name, &rule.RuleType, &rule.Severity, &rule.FilterJSON, &rule.GroupBy, &rule.WindowSeconds, &rule.Threshold, &rule.CooldownSeconds, &rule.Status); err != nil {
		return AlertRule{}, err
	}
	return rule, nil
}

func alertRuleMutationFromCurrent(rule AlertRule) AlertRuleMutation {
	return AlertRuleMutation{
		TenantID:        rule.TenantID,
		Service:         rule.Service,
		Environment:     rule.Environment,
		LogLevel:        rule.LogLevel,
		Fingerprint:     rule.Fingerprint,
		Name:            rule.Name,
		RuleType:        rule.RuleType,
		Severity:        rule.Severity,
		FilterJSON:      rule.FilterJSON,
		GroupBy:         rule.GroupBy,
		WindowSeconds:   rule.WindowSeconds,
		Threshold:       rule.Threshold,
		CooldownSeconds: rule.CooldownSeconds,
		Status:          rule.Status,
	}
}

func applyAlertRulePatch(rule *AlertRuleMutation, patch AlertRulePatch) {
	if patch.Service != nil {
		rule.Service = strings.TrimSpace(*patch.Service)
	}
	if patch.Environment != nil {
		rule.Environment = strings.TrimSpace(*patch.Environment)
	}
	if patch.LogLevel != nil {
		rule.LogLevel = strings.TrimSpace(*patch.LogLevel)
	}
	if patch.Fingerprint != nil {
		rule.Fingerprint = strings.TrimSpace(*patch.Fingerprint)
	}
	if patch.Name != nil {
		rule.Name = strings.TrimSpace(*patch.Name)
	}
	if patch.RuleType != nil {
		rule.RuleType = strings.TrimSpace(*patch.RuleType)
	}
	if patch.Severity != nil {
		rule.Severity = strings.TrimSpace(*patch.Severity)
	}
	if patch.FilterJSON != nil {
		rule.FilterJSON = *patch.FilterJSON
	}
	if patch.GroupBy != nil {
		rule.GroupBy = *patch.GroupBy
	}
	if patch.WindowSeconds != nil {
		rule.WindowSeconds = *patch.WindowSeconds
	}
	if patch.Threshold != nil {
		rule.Threshold = strings.TrimSpace(*patch.Threshold)
	}
	if patch.CooldownSeconds != nil {
		rule.CooldownSeconds = *patch.CooldownSeconds
	}
	if patch.Status != nil {
		rule.Status = strings.ToLower(strings.TrimSpace(*patch.Status))
	}
}
