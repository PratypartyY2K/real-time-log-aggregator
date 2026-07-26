package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/alerts"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
)

const defaultAlertEvaluationMaxRecords = 50000

type SchedulerOptions struct {
	Interval   time.Duration
	MaxRecords int
	Evaluator  ruleEvaluator
}

type scheduledRuleStore interface {
	LoadAllActiveRules(context.Context) ([]alerts.Rule, error)
	SyncAlertState(context.Context, []alerts.Rule, []alerts.Trigger, time.Time) ([]alerts.StateChange, error)
	DispatchDueNotifications(context.Context, alerts.NotificationDispatcher, time.Time) error
}

type alertRecordStore interface {
	QueryAlertRecords(context.Context, alerts.Rule, time.Time, time.Time, int) ([]alerts.Record, error)
}

func RunAlertScheduler(ctx context.Context, logger app.Logger, records alertRecordStore, rules scheduledRuleStore, dispatcher alerts.NotificationDispatcher, metrics AlertMetrics, options SchedulerOptions) {
	if options.Interval <= 0 {
		return
	}
	if options.MaxRecords <= 0 {
		options.MaxRecords = defaultAlertEvaluationMaxRecords
	}
	if options.Evaluator == nil {
		options.Evaluator = alerts.Evaluate
	}

	ticker := time.NewTicker(options.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if err := RunScheduledAlertEvaluation(ctx, records, rules, dispatcher, metrics, options, now.UTC()); err != nil && logger != nil {
				logger.Error("scheduled alert evaluation failed", "operation", "scheduled_alert_evaluation", "error", err)
			}
		}
	}
}

func RunScheduledAlertEvaluation(ctx context.Context, records alertRecordStore, rules scheduledRuleStore, dispatcher alerts.NotificationDispatcher, metrics AlertMetrics, options SchedulerOptions, observedAt time.Time) error {
	if records == nil {
		return fmt.Errorf("alert record store is not configured")
	}
	if rules == nil {
		return fmt.Errorf("alert rule store is not configured")
	}
	if options.MaxRecords <= 0 {
		options.MaxRecords = defaultAlertEvaluationMaxRecords
	}
	evaluator := options.Evaluator
	if evaluator == nil {
		evaluator = alerts.Evaluate
	}

	activeRules, err := rules.LoadAllActiveRules(ctx)
	if err != nil {
		return fmt.Errorf("load scheduled alert rules: %w", err)
	}

	triggers := make([]alerts.Trigger, 0)
	evaluatedRules := make([]alerts.Rule, 0, len(activeRules))
	for _, rule := range activeRules {
		if rule.WindowSeconds <= 0 {
			continue
		}
		start := observedAt.Add(-time.Duration(rule.WindowSeconds) * time.Second)
		alertRecords, err := records.QueryAlertRecords(ctx, rule, start, observedAt, options.MaxRecords)
		if err != nil {
			return fmt.Errorf("query alert records for rule %d: %w", rule.ID, err)
		}
		ruleTriggers, err := evaluator(rule, alertRecords)
		if err != nil {
			return fmt.Errorf("evaluate scheduled rule %d: %w", rule.ID, err)
		}
		evaluatedRules = append(evaluatedRules, rule)
		triggers = append(triggers, ruleTriggers...)
	}

	changes, err := rules.SyncAlertState(ctx, evaluatedRules, triggers, observedAt.UTC())
	if err != nil {
		return fmt.Errorf("sync scheduled alert state: %w", err)
	}
	if metrics != nil {
		metrics.ObserveStateChanges(changes)
	}
	if dispatcher == nil {
		return nil
	}
	return rules.DispatchDueNotifications(ctx, dispatcher, observedAt.UTC())
}

func recordsFromNormalized(records []NormalizedLogRecord) ([]alerts.Record, error) {
	alertRecords := make([]alerts.Record, 0, len(records))
	for _, record := range records {
		fields := map[string]any{}
		if record.FieldsJSON != "" {
			if err := json.Unmarshal([]byte(record.FieldsJSON), &fields); err != nil {
				return nil, fmt.Errorf("decode normalized fields_json: %w", err)
			}
		}
		alertRecords = append(alertRecords, alerts.Record{
			Timestamp:    record.Timestamp,
			TenantID:     record.TenantID,
			Service:      record.Service,
			Environment:  record.Environment,
			Source:       record.Source,
			Host:         record.Host,
			Level:        record.Level,
			TraceID:      record.TraceID,
			Fingerprint:  record.Fingerprint,
			Message:      record.Message,
			Fields:       fields,
			IngestID:     record.IngestID,
			RawSizeBytes: record.RawSizeBytes,
		})
	}
	return alertRecords, nil
}
