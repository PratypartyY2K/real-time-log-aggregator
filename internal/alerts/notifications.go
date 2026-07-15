package alerts

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	DeliveryStatusPending  = "pending"
	DeliveryStatusRetrying = "retrying"
	DeliveryStatusSent     = "sent"
	DeliveryStatusFailed   = "failed"

	DefaultNotificationChannel = "log"
	DefaultDeliveryAttempts    = 3
	DefaultRetryDelay          = 30 * time.Second
)

type NotificationDelivery struct {
	ID              int64
	AlertInstanceID int64
	Channel         string
	Target          string
	Status          string
	AttemptCount    int
	NextRetryAt     sql.NullTime
	LastError       sql.NullString
	CreatedAt       time.Time
}

type NotificationPayload struct {
	RuleID        int64             `json:"rule_id"`
	RuleName      string            `json:"rule_name"`
	Severity      string            `json:"severity"`
	EventType     string            `json:"event_type"`
	Status        string            `json:"status"`
	DedupeKey     string            `json:"dedupe_key"`
	GroupKey      string            `json:"group_key"`
	Group         map[string]string `json:"group"`
	MatchCount    int               `json:"match_count"`
	Service       string            `json:"service,omitempty"`
	Environment   string            `json:"environment,omitempty"`
	Target        string            `json:"target"`
	Channel       string            `json:"channel"`
	ObservedAtUTC time.Time         `json:"observed_at_utc"`
}

type NotificationDispatcher interface {
	Dispatch(context.Context, NotificationDelivery, NotificationPayload) error
}

func NewLogDispatcher(logger interface{ Info(string, ...any) }) NotificationDispatcher {
	return logDispatcherAdapter{logger: logger}
}

type logDispatcherAdapter struct {
	logger interface{ Info(string, ...any) }
}

func (d logDispatcherAdapter) Dispatch(_ context.Context, delivery NotificationDelivery, payload NotificationPayload) error {
	if d.logger != nil {
		d.logger.Info(
			"notification dispatched",
			"delivery_id", delivery.ID,
			"alert_instance_id", delivery.AlertInstanceID,
			"channel", delivery.Channel,
			"target", delivery.Target,
			"rule_id", payload.RuleID,
			"rule_name", payload.RuleName,
			"event_type", payload.EventType,
			"status", payload.Status,
			"match_count", payload.MatchCount,
		)
	}
	return nil
}

func (s *PostgresStore) enqueueNotifications(ctx context.Context, tx *sql.Tx, rules []Rule, changes []StateChange, observedAt time.Time) error {
	ruleByID := make(map[int64]Rule, len(rules))
	for _, rule := range rules {
		ruleByID[rule.ID] = rule
	}

	for _, change := range changes {
		if change.EventType != AlertEventTriggered && change.EventType != AlertEventResolved {
			continue
		}
		rule, ok := ruleByID[change.RuleID]
		if !ok {
			continue
		}

		channel, target := notificationDestination(rule)
		payload, err := json.Marshal(NotificationPayload{
			RuleID:        rule.ID,
			RuleName:      rule.Name,
			Severity:      rule.Severity,
			EventType:     change.EventType,
			Status:        change.Status,
			DedupeKey:     change.DedupeKey,
			GroupKey:      change.GroupKey,
			Group:         change.Group,
			MatchCount:    change.MatchCount,
			Service:       nullableString(rule.ServiceName),
			Environment:   nullableString(rule.Environment),
			Target:        target,
			Channel:       channel,
			ObservedAtUTC: observedAt.UTC(),
		})
		if err != nil {
			return fmt.Errorf("marshal notification payload: %w", err)
		}

		if err := insertNotificationDelivery(ctx, tx, change.AlertInstanceID, channel, target, payload); err != nil {
			return err
		}
	}

	return nil
}

func (s *PostgresStore) DispatchDueNotifications(ctx context.Context, dispatcher NotificationDispatcher, observedAt time.Time) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("alert notification store is not configured")
	}
	if dispatcher == nil {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin notification dispatch: %w", err)
	}
	defer tx.Rollback()

	deliveries, err := loadDueDeliveries(ctx, tx, observedAt.UTC())
	if err != nil {
		return err
	}

	for _, pending := range deliveries {
		if err := dispatcher.Dispatch(ctx, pending.delivery, pending.payload); err != nil {
			if err := markDeliveryFailure(ctx, tx, pending.delivery, err, observedAt.UTC()); err != nil {
				return err
			}
			continue
		}
		if err := markDeliverySent(ctx, tx, pending.delivery); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit notification dispatch: %w", err)
	}
	return nil
}

type pendingDelivery struct {
	delivery NotificationDelivery
	payload  NotificationPayload
}

func loadDueDeliveries(ctx context.Context, tx *sql.Tx, observedAt time.Time) ([]pendingDelivery, error) {
	const stmt = `
SELECT id, alert_instance_id, channel, target, status, attempt_count, next_retry_at, last_error, created_at
FROM notification_deliveries
WHERE status IN ($1, $2)
  AND (next_retry_at IS NULL OR next_retry_at <= $3)
ORDER BY created_at ASC, id ASC
FOR UPDATE
`

	rows, err := tx.QueryContext(ctx, stmt, DeliveryStatusPending, DeliveryStatusRetrying, observedAt)
	if err != nil {
		return nil, fmt.Errorf("query due notification deliveries: %w", err)
	}
	defer rows.Close()

	deliveries := make([]pendingDelivery, 0)
	for rows.Next() {
		var item pendingDelivery
		if err := rows.Scan(
			&item.delivery.ID,
			&item.delivery.AlertInstanceID,
			&item.delivery.Channel,
			&item.delivery.Target,
			&item.delivery.Status,
			&item.delivery.AttemptCount,
			&item.delivery.NextRetryAt,
			&item.delivery.LastError,
			&item.delivery.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan notification delivery: %w", err)
		}
		if err := loadNotificationPayload(ctx, tx, &item); err != nil {
			return nil, err
		}
		deliveries = append(deliveries, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate notification deliveries: %w", err)
	}

	return deliveries, nil
}

func loadNotificationPayload(ctx context.Context, tx *sql.Tx, item *pendingDelivery) error {
	const stmt = `
SELECT payload_json
FROM alert_events
WHERE alert_instance_id = $1
  AND event_type = $2
  AND created_at <= $3
ORDER BY created_at DESC, id DESC
LIMIT 1
`

	var raw string
	if err := tx.QueryRowContext(ctx, stmt, item.delivery.AlertInstanceID, "notification_enqueued", item.delivery.CreatedAt).Scan(&raw); err != nil {
		return fmt.Errorf("load notification payload: %w", err)
	}
	if err := json.Unmarshal([]byte(raw), &item.payload); err != nil {
		return fmt.Errorf("decode notification payload: %w", err)
	}
	item.payload.Target = item.delivery.Target
	item.payload.Channel = item.delivery.Channel
	return nil
}

func insertNotificationDelivery(ctx context.Context, tx *sql.Tx, alertInstanceID int64, channel, target string, payload json.RawMessage) error {
	const eventStmt = `
INSERT INTO alert_events (alert_instance_id, event_type, payload_json)
VALUES ($1, $2, $3::jsonb)
`
	if _, err := tx.ExecContext(ctx, eventStmt, alertInstanceID, "notification_enqueued", string(payload)); err != nil {
		return fmt.Errorf("insert notification enqueue event: %w", err)
	}

	const deliveryStmt = `
INSERT INTO notification_deliveries (
    alert_instance_id,
    channel,
    target,
    status,
    attempt_count
) VALUES ($1, $2, $3, $4, 0)
`
	if _, err := tx.ExecContext(ctx, deliveryStmt, alertInstanceID, channel, target, DeliveryStatusPending); err != nil {
		return fmt.Errorf("insert notification delivery: %w", err)
	}
	return nil
}

func markDeliverySent(ctx context.Context, tx *sql.Tx, delivery NotificationDelivery) error {
	const stmt = `
UPDATE notification_deliveries
SET status = $2,
    attempt_count = attempt_count + 1,
    next_retry_at = NULL,
    last_error = NULL
WHERE id = $1
`
	if _, err := tx.ExecContext(ctx, stmt, delivery.ID, DeliveryStatusSent); err != nil {
		return fmt.Errorf("mark delivery sent: %w", err)
	}
	return nil
}

func markDeliveryFailure(ctx context.Context, tx *sql.Tx, delivery NotificationDelivery, dispatchErr error, observedAt time.Time) error {
	attempts := delivery.AttemptCount + 1
	status := DeliveryStatusRetrying
	nextRetryAt := sql.NullTime{Time: observedAt.Add(DefaultRetryDelay), Valid: true}
	if attempts >= DefaultDeliveryAttempts {
		status = DeliveryStatusFailed
		nextRetryAt = sql.NullTime{}
	}

	const stmt = `
UPDATE notification_deliveries
SET status = $2,
    attempt_count = $3,
    next_retry_at = $4,
    last_error = $5
WHERE id = $1
`
	if _, err := tx.ExecContext(ctx, stmt, delivery.ID, status, attempts, nextRetryAt, strings.TrimSpace(dispatchErr.Error())); err != nil {
		return fmt.Errorf("mark delivery failure: %w", err)
	}
	return nil
}

func notificationDestination(rule Rule) (string, string) {
	target := strings.TrimSpace(nullableString(rule.Owner))
	if target != "" {
		return DefaultNotificationChannel, target
	}

	service := strings.TrimSpace(nullableString(rule.ServiceName))
	environment := strings.TrimSpace(nullableString(rule.Environment))
	switch {
	case service != "" && environment != "":
		return DefaultNotificationChannel, service + ":" + environment
	case service != "":
		return DefaultNotificationChannel, service
	default:
		return DefaultNotificationChannel, fmt.Sprintf("tenant:%d", rule.TenantID)
	}
}

func nullableString(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}
