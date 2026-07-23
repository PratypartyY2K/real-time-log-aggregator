package queryapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const maxAlertHistoryWindow = 90 * 24 * time.Hour

type AlertHistoryQuery struct {
	TenantID  uint64
	Start     time.Time
	End       time.Time
	RuleID    int64
	Status    string
	EventType string
	Limit     int
	Offset    int
}

type AlertHistoryItem struct {
	AlertInstanceID int64      `json:"alert_instance_id"`
	RuleID          int64      `json:"rule_id"`
	RuleName        string     `json:"rule_name"`
	Severity        string     `json:"severity"`
	DedupeKey       string     `json:"dedupe_key"`
	Status          string     `json:"status"`
	FirstFiredAt    time.Time  `json:"first_fired_at"`
	LastFiredAt     time.Time  `json:"last_fired_at"`
	ResolvedAt      *time.Time `json:"resolved_at,omitempty"`
}

type AlertAuditEntry struct {
	ID              string         `json:"id"`
	AlertInstanceID int64          `json:"alert_instance_id"`
	RuleID          int64          `json:"rule_id"`
	RuleName        string         `json:"rule_name"`
	EventType       string         `json:"event_type"`
	Payload         map[string]any `json:"payload"`
	CreatedAt       time.Time      `json:"created_at"`
}

type NotificationDeliveryItem struct {
	ID              int64      `json:"id"`
	AlertInstanceID int64      `json:"alert_instance_id"`
	RuleID          int64      `json:"rule_id"`
	RuleName        string     `json:"rule_name"`
	Channel         string     `json:"channel"`
	Target          string     `json:"target"`
	Status          string     `json:"status"`
	AttemptCount    int        `json:"attempt_count"`
	MaxAttempts     int        `json:"max_attempts"`
	AvailableAt     time.Time  `json:"available_at"`
	NextRetryAt     *time.Time `json:"next_retry_at,omitempty"`
	LastError       string     `json:"last_error,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	SentAt          *time.Time `json:"sent_at,omitempty"`
}

type AlertHistoryStore interface {
	QueryAlertHistory(context.Context, AlertHistoryQuery) ([]AlertHistoryItem, error)
	QueryAlertAudit(context.Context, AlertHistoryQuery) ([]AlertAuditEntry, error)
	QueryNotificationDeliveries(context.Context, AlertHistoryQuery) ([]NotificationDeliveryItem, error)
}

type PostgresAlertHistoryStore struct{ db *sql.DB }

func NewPostgresAlertHistoryStore(db *sql.DB) *PostgresAlertHistoryStore {
	return &PostgresAlertHistoryStore{db: db}
}

type AlertHistoryHandler struct {
	store      AlertHistoryStore
	audit      bool
	deliveries bool
}

func NewAlertHistoryHandler(store AlertHistoryStore) *AlertHistoryHandler {
	return &AlertHistoryHandler{store: store}
}

func NewAlertAuditHandler(store AlertHistoryStore) *AlertHistoryHandler {
	return &AlertHistoryHandler{store: store, audit: true}
}

func NewNotificationDeliveryHistoryHandler(store AlertHistoryStore) *AlertHistoryHandler {
	return &AlertHistoryHandler{store: store, deliveries: true}
}

func (h *AlertHistoryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h == nil || h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "alert history store unavailable")
		return
	}
	tenantID, ok := TenantIDFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "tenant identity required")
		return
	}
	query, err := parseAlertHistoryQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	query.TenantID = tenantID

	if h.deliveries {
		deliveries, err := h.store.QueryNotificationDeliveries(r.Context(), query)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "failed to query notification deliveries")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"deliveries": deliveries, "count": len(deliveries), "next_offset": historyNextOffset(len(deliveries), query)})
		return
	}
	if h.audit {
		entries, err := h.store.QueryAlertAudit(r.Context(), query)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "failed to query alert audit log")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"entries": entries, "count": len(entries), "next_offset": historyNextOffset(len(entries), query)})
		return
	}
	alerts, err := h.store.QueryAlertHistory(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to query alert history")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"alerts": alerts, "count": len(alerts), "next_offset": historyNextOffset(len(alerts), query)})
}

func (s *PostgresAlertHistoryStore) QueryNotificationDeliveries(ctx context.Context, query AlertHistoryQuery) ([]NotificationDeliveryItem, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("alert history store is not configured")
	}
	stmt, args := buildNotificationDeliveryHistorySQL(query)
	rows, err := s.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("query notification deliveries: %w", err)
	}
	defer rows.Close()
	items := make([]NotificationDeliveryItem, 0)
	for rows.Next() {
		var item NotificationDeliveryItem
		var nextRetry, sentAt sql.NullTime
		var lastError sql.NullString
		if err := rows.Scan(&item.ID, &item.AlertInstanceID, &item.RuleID, &item.RuleName,
			&item.Channel, &item.Target, &item.Status, &item.AttemptCount, &item.MaxAttempts,
			&item.AvailableAt, &nextRetry, &lastError, &item.CreatedAt, &item.UpdatedAt, &sentAt); err != nil {
			return nil, fmt.Errorf("scan notification delivery: %w", err)
		}
		item.AvailableAt = item.AvailableAt.UTC()
		item.CreatedAt = item.CreatedAt.UTC()
		item.UpdatedAt = item.UpdatedAt.UTC()
		if nextRetry.Valid {
			value := nextRetry.Time.UTC()
			item.NextRetryAt = &value
		}
		if sentAt.Valid {
			value := sentAt.Time.UTC()
			item.SentAt = &value
		}
		if lastError.Valid {
			item.LastError = lastError.String
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate notification deliveries: %w", err)
	}
	return items, nil
}

func parseAlertHistoryQuery(r *http.Request) (AlertHistoryQuery, error) {
	values := r.URL.Query()
	start, err := parseRequiredTimestamp(values.Get("start"), "start")
	if err != nil {
		return AlertHistoryQuery{}, err
	}
	end, err := parseRequiredTimestamp(values.Get("end"), "end")
	if err != nil {
		return AlertHistoryQuery{}, err
	}
	if !start.Before(end) {
		return AlertHistoryQuery{}, errors.New("start must be before end")
	}
	if end.Sub(start) > maxAlertHistoryWindow {
		return AlertHistoryQuery{}, errors.New("time range cannot exceed 90 days")
	}

	var ruleID int64
	if raw := strings.TrimSpace(values.Get("rule_id")); raw != "" {
		ruleID, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || ruleID <= 0 {
			return AlertHistoryQuery{}, errors.New("rule_id must be a positive integer")
		}
	}
	status, err := parseSafeOptionalFilter(strings.ToLower(values.Get("status")), "status")
	if err != nil {
		return AlertHistoryQuery{}, err
	}
	eventType, err := parseSafeOptionalFilter(strings.ToLower(values.Get("event_type")), "event_type")
	if err != nil {
		return AlertHistoryQuery{}, err
	}
	limit, err := parseBoundedPositiveInt(values.Get("limit"), defaultLimit, 500, "limit")
	if err != nil {
		return AlertHistoryQuery{}, err
	}
	offset := 0
	if raw := strings.TrimSpace(values.Get("offset")); raw != "" {
		offset, err = strconv.Atoi(raw)
		if err != nil || offset < 0 || offset > maxOffset {
			return AlertHistoryQuery{}, errors.New("offset must be between 0 and 10000")
		}
	}
	return AlertHistoryQuery{Start: start, End: end, RuleID: ruleID, Status: status, EventType: eventType, Limit: limit, Offset: offset}, nil
}

func historyNextOffset(count int, query AlertHistoryQuery) *int {
	if count != query.Limit || query.Offset+query.Limit > maxOffset {
		return nil
	}
	next := query.Offset + query.Limit
	return &next
}

func (s *PostgresAlertHistoryStore) QueryAlertHistory(ctx context.Context, query AlertHistoryQuery) ([]AlertHistoryItem, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("alert history store is not configured")
	}
	stmt, args := buildAlertHistorySQL(query)
	rows, err := s.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("query alert history: %w", err)
	}
	defer rows.Close()
	items := make([]AlertHistoryItem, 0)
	for rows.Next() {
		var item AlertHistoryItem
		var resolved sql.NullTime
		if err := rows.Scan(&item.AlertInstanceID, &item.RuleID, &item.RuleName, &item.Severity, &item.DedupeKey, &item.Status, &item.FirstFiredAt, &item.LastFiredAt, &resolved); err != nil {
			return nil, fmt.Errorf("scan alert history: %w", err)
		}
		if resolved.Valid {
			value := resolved.Time.UTC()
			item.ResolvedAt = &value
		}
		item.FirstFiredAt = item.FirstFiredAt.UTC()
		item.LastFiredAt = item.LastFiredAt.UTC()
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate alert history: %w", err)
	}
	return items, nil
}

func (s *PostgresAlertHistoryStore) QueryAlertAudit(ctx context.Context, query AlertHistoryQuery) ([]AlertAuditEntry, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("alert history store is not configured")
	}
	stmt, args := buildAlertAuditSQL(query)
	rows, err := s.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("query alert audit log: %w", err)
	}
	defer rows.Close()
	entries := make([]AlertAuditEntry, 0)
	for rows.Next() {
		var entry AlertAuditEntry
		var raw string
		if err := rows.Scan(&entry.ID, &entry.AlertInstanceID, &entry.RuleID, &entry.RuleName, &entry.EventType, &raw, &entry.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan alert audit entry: %w", err)
		}
		entry.Payload = map[string]any{}
		if err := json.Unmarshal([]byte(raw), &entry.Payload); err != nil {
			return nil, fmt.Errorf("decode alert audit payload: %w", err)
		}
		entry.CreatedAt = entry.CreatedAt.UTC()
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate alert audit log: %w", err)
	}
	return entries, nil
}

func buildAlertHistorySQL(query AlertHistoryQuery) (string, []any) {
	var builder strings.Builder
	builder.WriteString(`SELECT instance.id, rule.id, rule.name, rule.severity, instance.dedupe_key,
instance.status, instance.first_fired_at, instance.last_fired_at, instance.resolved_at
FROM alert_instances instance JOIN alert_rules rule ON rule.id = instance.rule_id
WHERE rule.tenant_id = $1 AND instance.last_fired_at >= $2 AND instance.first_fired_at < $3 `)
	args := []any{query.TenantID, query.Start, query.End}
	appendHistoryFilters(&builder, &args, query, false)
	builder.WriteString(fmt.Sprintf("ORDER BY instance.last_fired_at DESC, instance.id DESC LIMIT $%d OFFSET $%d", len(args)+1, len(args)+2))
	args = append(args, query.Limit, query.Offset)
	return builder.String(), args
}

func buildAlertAuditSQL(query AlertHistoryQuery) (string, []any) {
	var builder strings.Builder
	builder.WriteString(`SELECT audit_id, alert_instance_id, rule_id, rule_name, event_type, payload_json, created_at
FROM (
 SELECT 'event:' || event.id::text AS audit_id, instance.id AS alert_instance_id,
        rule.id AS rule_id, rule.name AS rule_name, event.event_type,
        event.payload_json::text AS payload_json, event.created_at
 FROM alert_events event
 JOIN alert_instances instance ON instance.id = event.alert_instance_id
 JOIN alert_rules rule ON rule.id = instance.rule_id
 WHERE rule.tenant_id = $1 AND event.created_at >= $2 AND event.created_at < $3
 UNION ALL
 SELECT 'attempt:' || attempt.id::text, instance.id, rule.id, rule.name,
        'notification_attempt',
        jsonb_build_object('delivery_id', delivery.id, 'attempt_number', attempt.attempt_number,
          'status', attempt.status, 'error', attempt.error, 'channel', delivery.channel,
          'target', delivery.target, 'started_at', attempt.started_at,
          'completed_at', attempt.completed_at)::text,
        attempt.completed_at
 FROM notification_delivery_attempts attempt
 JOIN notification_deliveries delivery ON delivery.id = attempt.delivery_id
 JOIN alert_instances instance ON instance.id = delivery.alert_instance_id
 JOIN alert_rules rule ON rule.id = instance.rule_id
 WHERE rule.tenant_id = $1 AND attempt.completed_at >= $2 AND attempt.completed_at < $3
) audit WHERE 1=1 `)
	args := []any{query.TenantID, query.Start, query.End}
	appendHistoryFilters(&builder, &args, query, true)
	builder.WriteString(fmt.Sprintf("ORDER BY created_at DESC, audit_id DESC LIMIT $%d OFFSET $%d", len(args)+1, len(args)+2))
	args = append(args, query.Limit, query.Offset)
	return builder.String(), args
}

func buildNotificationDeliveryHistorySQL(query AlertHistoryQuery) (string, []any) {
	var builder strings.Builder
	builder.WriteString(`SELECT delivery.id, instance.id, rule.id, rule.name, delivery.channel,
delivery.target, delivery.status, delivery.attempt_count, delivery.max_attempts,
delivery.available_at, delivery.next_retry_at, delivery.last_error, delivery.created_at,
delivery.updated_at, delivery.sent_at
FROM notification_deliveries delivery
JOIN alert_instances instance ON instance.id = delivery.alert_instance_id
JOIN alert_rules rule ON rule.id = instance.rule_id
WHERE rule.tenant_id = $1 AND delivery.created_at >= $2 AND delivery.created_at < $3 `)
	args := []any{query.TenantID, query.Start, query.End}
	if query.RuleID > 0 {
		args = append(args, query.RuleID)
		builder.WriteString(fmt.Sprintf("AND rule.id = $%d ", len(args)))
	}
	if query.Status != "" {
		args = append(args, query.Status)
		builder.WriteString(fmt.Sprintf("AND delivery.status = $%d ", len(args)))
	}
	builder.WriteString(fmt.Sprintf("ORDER BY delivery.created_at DESC, delivery.id DESC LIMIT $%d OFFSET $%d", len(args)+1, len(args)+2))
	args = append(args, query.Limit, query.Offset)
	return builder.String(), args
}

func appendHistoryFilters(builder *strings.Builder, args *[]any, query AlertHistoryQuery, audit bool) {
	if query.RuleID > 0 {
		*args = append(*args, query.RuleID)
		field := "rule.id"
		if audit {
			field = "rule_id"
		}
		builder.WriteString(fmt.Sprintf("AND %s = $%d ", field, len(*args)))
	}
	if query.Status != "" && !audit {
		*args = append(*args, query.Status)
		builder.WriteString(fmt.Sprintf("AND instance.status = $%d ", len(*args)))
	}
	if query.EventType != "" && audit {
		*args = append(*args, query.EventType)
		builder.WriteString(fmt.Sprintf("AND event_type = $%d ", len(*args)))
	}
}
