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
	DeliveryStatusPending    = "pending"
	DeliveryStatusProcessing = "processing"
	DeliveryStatusRetrying   = "retrying"
	DeliveryStatusSent       = "sent"
	DeliveryStatusFailed     = "failed"

	DefaultNotificationChannel = "log"
	DefaultDeliveryAttempts    = 5
	DefaultRetryDelay          = 30 * time.Second
	DefaultMaxRetryDelay       = 30 * time.Minute
	DefaultDeliveryLease       = 2 * time.Minute
	DefaultDeliveryBatchSize   = 50
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
	PayloadJSON     json.RawMessage
	MaxAttempts     int
	AvailableAt     time.Time
	LockedAt        sql.NullTime
	LockedBy        sql.NullString
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
	return s.DispatchNotificationBatch(ctx, dispatcher, DeliveryPolicy{
		WorkerID: "processor-inline", MaxAttempts: DefaultDeliveryAttempts,
		BaseRetryDelay: DefaultRetryDelay, MaxRetryDelay: DefaultMaxRetryDelay,
		LeaseDuration: DefaultDeliveryLease, BatchSize: DefaultDeliveryBatchSize,
	}, observedAt)
}

type DeliveryPolicy struct {
	WorkerID       string
	MaxAttempts    int
	BaseRetryDelay time.Duration
	MaxRetryDelay  time.Duration
	LeaseDuration  time.Duration
	BatchSize      int
}

func (p DeliveryPolicy) normalized() DeliveryPolicy {
	if strings.TrimSpace(p.WorkerID) == "" {
		p.WorkerID = "notification-worker"
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = DefaultDeliveryAttempts
	}
	if p.BaseRetryDelay <= 0 {
		p.BaseRetryDelay = DefaultRetryDelay
	}
	if p.MaxRetryDelay < p.BaseRetryDelay {
		p.MaxRetryDelay = DefaultMaxRetryDelay
	}
	if p.LeaseDuration <= 0 {
		p.LeaseDuration = DefaultDeliveryLease
	}
	if p.BatchSize <= 0 {
		p.BatchSize = DefaultDeliveryBatchSize
	}
	if p.BatchSize > 500 {
		p.BatchSize = 500
	}
	return p
}

func (s *PostgresStore) DispatchNotificationBatch(ctx context.Context, dispatcher NotificationDispatcher, policy DeliveryPolicy, observedAt time.Time) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("alert notification store is not configured")
	}
	if dispatcher == nil {
		return nil
	}

	policy = policy.normalized()
	deliveries, err := s.claimDueDeliveries(ctx, policy, observedAt.UTC())
	if err != nil {
		return err
	}

	for _, pending := range deliveries {
		startedAt := time.Now().UTC()
		dispatchErr := dispatcher.Dispatch(ctx, pending.delivery, pending.payload)
		if err := s.recordDeliveryAttempt(ctx, pending.delivery, policy, startedAt, time.Now().UTC(), dispatchErr); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) claimDueDeliveries(ctx context.Context, policy DeliveryPolicy, observedAt time.Time) ([]pendingDelivery, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin notification claim: %w", err)
	}
	defer tx.Rollback()

	const stmt = `
WITH candidates AS (
    SELECT id
    FROM notification_deliveries
    WHERE (
        status IN ($1, $2) AND available_at <= $3
    ) OR (
        status = $4 AND locked_at < $5
    )
    ORDER BY available_at ASC, id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $6
)
UPDATE notification_deliveries delivery
SET status = $4,
    locked_at = $3,
    locked_by = $7,
    max_attempts = $8,
    updated_at = $3
FROM candidates
WHERE delivery.id = candidates.id
RETURNING delivery.id, delivery.alert_instance_id, delivery.channel, delivery.target,
          delivery.status, delivery.attempt_count, delivery.next_retry_at,
          delivery.last_error, delivery.created_at, delivery.payload_json::text,
          delivery.max_attempts, delivery.available_at, delivery.locked_at, delivery.locked_by
`
	rows, err := tx.QueryContext(ctx, stmt,
		DeliveryStatusPending, DeliveryStatusRetrying, observedAt,
		DeliveryStatusProcessing, observedAt.Add(-policy.LeaseDuration),
		policy.BatchSize, policy.WorkerID, policy.MaxAttempts,
	)
	if err != nil {
		return nil, fmt.Errorf("claim due notification deliveries: %w", err)
	}
	defer rows.Close()

	deliveries := make([]pendingDelivery, 0)
	for rows.Next() {
		var item pendingDelivery
		var payload string
		if err := rows.Scan(
			&item.delivery.ID, &item.delivery.AlertInstanceID, &item.delivery.Channel,
			&item.delivery.Target, &item.delivery.Status, &item.delivery.AttemptCount,
			&item.delivery.NextRetryAt, &item.delivery.LastError, &item.delivery.CreatedAt,
			&payload, &item.delivery.MaxAttempts, &item.delivery.AvailableAt,
			&item.delivery.LockedAt, &item.delivery.LockedBy,
		); err != nil {
			return nil, fmt.Errorf("scan claimed notification delivery: %w", err)
		}
		item.delivery.PayloadJSON = json.RawMessage(payload)
		if err := json.Unmarshal(item.delivery.PayloadJSON, &item.payload); err != nil {
			return nil, fmt.Errorf("decode claimed notification payload: %w", err)
		}
		item.payload.Target = item.delivery.Target
		item.payload.Channel = item.delivery.Channel
		deliveries = append(deliveries, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed notification deliveries: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close claimed notification deliveries: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit notification claim: %w", err)
	}
	return deliveries, nil
}

func (s *PostgresStore) recordDeliveryAttempt(ctx context.Context, delivery NotificationDelivery, policy DeliveryPolicy, startedAt, completedAt time.Time, dispatchErr error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delivery attempt record: %w", err)
	}
	defer tx.Rollback()

	attempt := delivery.AttemptCount + 1
	attemptStatus := DeliveryStatusSent
	errorMessage := ""
	if dispatchErr != nil {
		attemptStatus = DeliveryStatusFailed
		errorMessage = strings.TrimSpace(dispatchErr.Error())
	}
	const attemptStmt = `
INSERT INTO notification_delivery_attempts
    (delivery_id, attempt_number, status, error, started_at, completed_at)
VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6)
ON CONFLICT (delivery_id, attempt_number) DO NOTHING
`
	if _, err := tx.ExecContext(ctx, attemptStmt, delivery.ID, attempt, attemptStatus, errorMessage, startedAt, completedAt); err != nil {
		return fmt.Errorf("insert notification delivery attempt: %w", err)
	}

	maxAttempts := delivery.MaxAttempts
	status := DeliveryStatusSent
	availableAt := completedAt
	var nextRetryAt any
	var sentAt any = completedAt
	var lastError any
	if dispatchErr != nil {
		status = DeliveryStatusRetrying
		availableAt = completedAt.Add(retryDelay(delivery.ID, attempt, policy.BaseRetryDelay, policy.MaxRetryDelay))
		nextRetryAt = availableAt
		sentAt = nil
		lastError = errorMessage
		if attempt >= maxAttempts {
			status = DeliveryStatusFailed
			nextRetryAt = nil
		}
	}
	const deliveryStmt = `
UPDATE notification_deliveries
SET status = $2, attempt_count = $3, available_at = $4, next_retry_at = $5,
    last_error = $6, sent_at = $7, locked_at = NULL, locked_by = NULL, updated_at = $8
WHERE id = $1 AND status = $9 AND locked_by = $10
`
	result, err := tx.ExecContext(ctx, deliveryStmt, delivery.ID, status, attempt, availableAt, nextRetryAt, lastError, sentAt, completedAt, DeliveryStatusProcessing, policy.WorkerID)
	if err != nil {
		return fmt.Errorf("update notification delivery attempt: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("notification delivery %d lease was lost", delivery.ID)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delivery attempt record: %w", err)
	}
	return nil
}

func retryDelay(deliveryID int64, attempt int, base, maximum time.Duration) time.Duration {
	if base <= 0 {
		base = DefaultRetryDelay
	}
	if maximum < base {
		maximum = DefaultMaxRetryDelay
	}
	delay := base
	for index := 1; index < attempt && delay < maximum; index++ {
		if delay > maximum/2 {
			delay = maximum
			break
		}
		delay *= 2
	}
	if delay > maximum {
		delay = maximum
	}
	// Stable ±10% jitter prevents synchronized retries while keeping tests deterministic.
	jitterRange := delay / 10
	if jitterRange <= 0 {
		return delay
	}
	offset := time.Duration((deliveryID*31+int64(attempt)*17)%201-100) * jitterRange / 100
	delay += offset
	if delay < base {
		return base
	}
	if delay > maximum {
		return maximum
	}
	return delay
}

type pendingDelivery struct {
	delivery NotificationDelivery
	payload  NotificationPayload
}

func insertNotificationDelivery(ctx context.Context, tx *sql.Tx, alertInstanceID int64, channel, target string, payload json.RawMessage) error {
	const deliveryStmt = `
INSERT INTO notification_deliveries (
    alert_instance_id,
    channel,
    target,
    status,
    attempt_count,
    payload_json,
    max_attempts,
    available_at
) VALUES ($1, $2, $3, $4, 0, $5::jsonb, $6, NOW())
RETURNING id
`
	var deliveryID int64
	if err := tx.QueryRowContext(ctx, deliveryStmt, alertInstanceID, channel, target, DeliveryStatusPending, string(payload), DefaultDeliveryAttempts).Scan(&deliveryID); err != nil {
		return fmt.Errorf("insert notification delivery: %w", err)
	}

	eventPayload := map[string]any{}
	if err := json.Unmarshal(payload, &eventPayload); err != nil {
		return fmt.Errorf("decode notification enqueue payload: %w", err)
	}
	eventPayload["delivery_id"] = deliveryID
	encodedEvent, err := json.Marshal(eventPayload)
	if err != nil {
		return fmt.Errorf("encode notification enqueue payload: %w", err)
	}
	const eventStmt = `
INSERT INTO alert_events (alert_instance_id, event_type, payload_json)
VALUES ($1, $2, $3::jsonb)
`
	if _, err := tx.ExecContext(ctx, eventStmt, alertInstanceID, "notification_enqueued", string(encodedEvent)); err != nil {
		return fmt.Errorf("insert notification enqueue event: %w", err)
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
