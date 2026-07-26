package alerts

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	AlertStatusInactive = "inactive"
	AlertStatusFiring   = "firing"
	AlertStatusActive   = AlertStatusFiring
	AlertStatusResolved = "resolved"

	AlertEventTriggered  = "triggered"
	AlertEventSuppressed = "suppressed"
	AlertEventResolved   = "resolved"
)

type Instance struct {
	ID           int64
	RuleID       int64
	DedupeKey    string
	Status       string
	FirstFiredAt time.Time
	LastFiredAt  time.Time
	ResolvedAt   sql.NullTime
}

type StateChange struct {
	RuleID          int64
	RuleName        string
	DedupeKey       string
	Status          string
	EventType       string
	MatchCount      int
	MetricValue     float64
	Threshold       float64
	WindowSeconds   int
	Percentile      float64
	ValueField      string
	GroupKey        string
	Group           map[string]string
	AlertInstanceID int64
}

type statePlan struct {
	upserts []instanceUpsert
	events  []eventInsert
}

type instanceUpsert struct {
	instance Instance
	insert   bool
}

type eventInsert struct {
	InstanceID int64
	EventType  string
	Payload    json.RawMessage
}

func (s *PostgresStore) SyncAlertState(ctx context.Context, rules []Rule, triggers []Trigger, observedAt time.Time) ([]StateChange, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("alert state store is not configured")
	}
	if len(rules) == 0 {
		return nil, nil
	}

	ruleIDs := make([]int64, 0, len(rules))
	for _, rule := range rules {
		ruleIDs = append(ruleIDs, rule.ID)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin alert state sync: %w", err)
	}
	defer tx.Rollback()

	instances, err := loadInstances(ctx, tx, ruleIDs)
	if err != nil {
		return nil, err
	}

	plan, changes, err := reconcileState(rules, triggers, instances, observedAt.UTC())
	if err != nil {
		return nil, err
	}
	if err := applyPlan(ctx, tx, plan, changes); err != nil {
		return nil, err
	}
	if err := s.enqueueNotifications(ctx, tx, rules, changes, observedAt.UTC()); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit alert state sync: %w", err)
	}

	return changes, nil
}

func reconcileState(rules []Rule, triggers []Trigger, instances []Instance, observedAt time.Time) (statePlan, []StateChange, error) {
	ruleByID := make(map[int64]Rule, len(rules))
	for _, rule := range rules {
		ruleByID[rule.ID] = rule
	}

	instanceByKey := make(map[string]Instance, len(instances))
	for _, instance := range instances {
		instanceByKey[instanceKey(instance.RuleID, instance.DedupeKey)] = instance
	}

	triggered := make(map[string]struct{}, len(triggers))
	plan := statePlan{
		upserts: make([]instanceUpsert, 0, len(triggers)),
		events:  make([]eventInsert, 0, len(triggers)),
	}
	changes := make([]StateChange, 0, len(triggers))

	for _, trigger := range triggers {
		rule, ok := ruleByID[trigger.RuleID]
		if !ok {
			return statePlan{}, nil, fmt.Errorf("trigger references unknown rule %d", trigger.RuleID)
		}

		key := instanceKey(trigger.RuleID, trigger.GroupKey)
		triggered[key] = struct{}{}
		instance, exists := instanceByKey[key]
		change := StateChange{
			RuleID:        trigger.RuleID,
			RuleName:      trigger.RuleName,
			DedupeKey:     trigger.GroupKey,
			MatchCount:    trigger.MatchCount,
			MetricValue:   trigger.MetricValue,
			Threshold:     trigger.Threshold,
			WindowSeconds: trigger.WindowSeconds,
			Percentile:    trigger.Percentile,
			ValueField:    trigger.ValueField,
			GroupKey:      trigger.GroupKey,
			Group:         trigger.Group,
		}

		if !exists {
			change.Status = AlertStatusActive
			change.EventType = AlertEventTriggered
			plan.upserts = append(plan.upserts, instanceUpsert{
				insert: true,
				instance: Instance{
					RuleID:       trigger.RuleID,
					DedupeKey:    trigger.GroupKey,
					Status:       AlertStatusActive,
					FirstFiredAt: observedAt,
					LastFiredAt:  observedAt,
				},
			})
			changes = append(changes, change)
			continue
		}

		if instance.Status == AlertStatusResolved {
			lastFiredAt := instance.LastFiredAt
			instance.Status = AlertStatusActive
			instance.FirstFiredAt = observedAt
			instance.LastFiredAt = observedAt
			instance.ResolvedAt = sql.NullTime{}

			change.Status = AlertStatusActive
			change.AlertInstanceID = instance.ID
			cooldown := time.Duration(rule.CooldownSeconds) * time.Second
			if cooldown > 0 && observedAt.Sub(lastFiredAt) < cooldown {
				change.EventType = AlertEventSuppressed
			} else {
				change.EventType = AlertEventTriggered
			}
			plan.upserts = append(plan.upserts, instanceUpsert{instance: instance})
			changes = append(changes, change)
			continue
		}

		change.Status = AlertStatusActive
		change.EventType = AlertEventSuppressed
		change.AlertInstanceID = instance.ID
		changes = append(changes, change)
	}

	activeInstances := slices.Clone(instances)
	for _, instance := range activeInstances {
		if instance.Status != AlertStatusActive {
			continue
		}
		key := instanceKey(instance.RuleID, instance.DedupeKey)
		if _, ok := triggered[key]; ok {
			continue
		}
		rule, ok := ruleByID[instance.RuleID]
		if !ok {
			continue
		}

		instance.Status = AlertStatusResolved
		instance.ResolvedAt = sql.NullTime{Time: observedAt, Valid: true}

		change := StateChange{
			RuleID:          rule.ID,
			RuleName:        rule.Name,
			DedupeKey:       instance.DedupeKey,
			Status:          AlertStatusResolved,
			EventType:       AlertEventResolved,
			AlertInstanceID: instance.ID,
		}
		plan.upserts = append(plan.upserts, instanceUpsert{instance: instance})
		changes = append(changes, change)
	}

	return plan, changes, nil
}

func applyPlan(ctx context.Context, tx *sql.Tx, plan statePlan, changes []StateChange) error {
	changeByKey := make(map[string]*StateChange, len(changes))
	for i := range changes {
		changeByKey[instanceKey(changes[i].RuleID, changes[i].DedupeKey)] = &changes[i]
	}

	for _, upsert := range plan.upserts {
		if upsert.insert {
			id, err := insertInstance(ctx, tx, upsert.instance)
			if err != nil {
				return err
			}
			key := instanceKey(upsert.instance.RuleID, upsert.instance.DedupeKey)
			changeByKey[key].AlertInstanceID = id
		} else {
			if err := updateInstance(ctx, tx, upsert.instance); err != nil {
				return err
			}
		}
	}

	for _, change := range changes {
		payload, err := buildEventPayload(change)
		if err != nil {
			return fmt.Errorf("marshal alert event payload: %w", err)
		}
		if err := insertEvent(ctx, tx, change.AlertInstanceID, change.EventType, payload); err != nil {
			return err
		}
	}

	return nil
}

func loadInstances(ctx context.Context, tx *sql.Tx, ruleIDs []int64) ([]Instance, error) {
	query, args := buildRuleIDQuery(`
SELECT id, rule_id, dedupe_key, status, first_fired_at, last_fired_at, resolved_at
FROM alert_instances
WHERE rule_id IN (`, ruleIDs)

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query alert instances: %w", err)
	}
	defer rows.Close()

	instances := make([]Instance, 0)
	for rows.Next() {
		var instance Instance
		if err := rows.Scan(
			&instance.ID,
			&instance.RuleID,
			&instance.DedupeKey,
			&instance.Status,
			&instance.FirstFiredAt,
			&instance.LastFiredAt,
			&instance.ResolvedAt,
		); err != nil {
			return nil, fmt.Errorf("scan alert instance: %w", err)
		}
		instances = append(instances, instance)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate alert instances: %w", err)
	}

	return instances, nil
}

func insertInstance(ctx context.Context, tx *sql.Tx, instance Instance) (int64, error) {
	const stmt = `
INSERT INTO alert_instances (
    rule_id,
    dedupe_key,
    status,
    first_fired_at,
    last_fired_at,
    resolved_at
) VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id
`

	var id int64
	if err := tx.QueryRowContext(
		ctx,
		stmt,
		instance.RuleID,
		instance.DedupeKey,
		instance.Status,
		instance.FirstFiredAt,
		instance.LastFiredAt,
		instance.ResolvedAt,
	).Scan(&id); err != nil {
		return 0, fmt.Errorf("insert alert instance: %w", err)
	}
	return id, nil
}

func updateInstance(ctx context.Context, tx *sql.Tx, instance Instance) error {
	const stmt = `
UPDATE alert_instances
SET status = $2,
    first_fired_at = $3,
    last_fired_at = $4,
    resolved_at = $5
WHERE id = $1
`

	if _, err := tx.ExecContext(
		ctx,
		stmt,
		instance.ID,
		instance.Status,
		instance.FirstFiredAt,
		instance.LastFiredAt,
		instance.ResolvedAt,
	); err != nil {
		return fmt.Errorf("update alert instance: %w", err)
	}
	return nil
}

func insertEvent(ctx context.Context, tx *sql.Tx, instanceID int64, eventType string, payload json.RawMessage) error {
	const stmt = `
INSERT INTO alert_events (alert_instance_id, event_type, payload_json)
VALUES ($1, $2, $3::jsonb)
`

	if _, err := tx.ExecContext(ctx, stmt, instanceID, eventType, string(payload)); err != nil {
		return fmt.Errorf("insert alert event: %w", err)
	}
	return nil
}

func buildEventPayload(change StateChange) (json.RawMessage, error) {
	payload := map[string]any{
		"rule_id":        change.RuleID,
		"rule_name":      change.RuleName,
		"dedupe_key":     change.DedupeKey,
		"status":         change.Status,
		"event_type":     change.EventType,
		"match_count":    change.MatchCount,
		"metric_value":   change.MetricValue,
		"threshold":      change.Threshold,
		"window_seconds": change.WindowSeconds,
		"group_key":      change.GroupKey,
		"group":          change.Group,
	}
	if change.Percentile > 0 {
		payload["percentile"] = change.Percentile
	}
	if change.ValueField != "" {
		payload["value_field"] = change.ValueField
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func buildRuleIDQuery(prefix string, ruleIDs []int64) (string, []any) {
	var builder strings.Builder
	builder.WriteString(prefix)
	args := make([]any, 0, len(ruleIDs))
	for i, ruleID := range ruleIDs {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(fmt.Sprintf("$%d", i+1))
		args = append(args, ruleID)
	}
	builder.WriteString(")")
	return builder.String(), args
}

func instanceKey(ruleID int64, dedupeKey string) string {
	return fmt.Sprintf("%d:%s", ruleID, dedupeKey)
}
