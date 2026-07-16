package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/alerts"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/stream"
)

func Run(ctx context.Context, logger app.Logger, cfg config.Config, metrics *Metrics, ruleStore AlertRuleStore) error {
	nc, consumer, err := stream.ConnectJetStreamConsumer(stream.ConsumerOptions{
		URL:        cfg.NATSURL,
		StreamName: cfg.NATSStream,
		Subject:    cfg.NATSSubject,
		DLQSubject: cfg.NATSDLQSubject,
		Durable:    cfg.NATSDurable,
		MaxDeliver: cfg.NATSMaxDeliver,
		ReplayMode: cfg.NATSReplayMode,
		ReplaySeq:  cfg.NATSReplaySeq,
		ReplayTime: cfg.NATSReplayTime,
	})
	if err != nil {
		return err
	}
	defer nc.Drain()

	writer := NewClickHouseWriter(cfg.ClickHouseDSN)
	dispatcher := alerts.NewLogDispatcher(logger)

	logger.Info("processor consumer started", "stream", cfg.NATSStream, "subject", cfg.NATSSubject, "durable", cfg.NATSDurable, "replay_mode", cfg.NATSReplayMode)

	return consumer.Consume(ctx, func(ctx context.Context, batch contracts.LogsRawEvent) error {
		start := time.Now()
		err := handleBatch(ctx, logger, writer, ruleStore, dispatcher, batch)
		result := resultSuccess
		if err != nil {
			result = resultRetryable
		}
		if err != nil && stream.IsPoisonBatchError(err) {
			result = resultInvalidBatch
		}
		metrics.ObserveBatch(result, len(batch.Logs), time.Since(start))
		return err
	}, func(_ context.Context, err error) {
		logger.Error("processor failed to handle batch", "error", err)
	})
}

type AlertRuleStore interface {
	LoadActiveRules(context.Context, uint64, string, string) ([]alerts.Rule, error)
	SyncAlertState(context.Context, []alerts.Rule, []alerts.Trigger, time.Time) ([]alerts.StateChange, error)
	DispatchDueNotifications(context.Context, alerts.NotificationDispatcher, time.Time) error
}

func handleBatch(ctx context.Context, logger app.Logger, writer LogWriter, ruleStore AlertRuleStore, dispatcher alerts.NotificationDispatcher, batch contracts.LogsRawEvent) error {
	if err := batch.Validate(); err != nil {
		return stream.MarkPoisonBatch(fmt.Errorf("invalid logs.raw event: %w", err))
	}

	normalized, err := normalizeBatch(batch)
	if err != nil {
		return stream.MarkPoisonBatch(fmt.Errorf("normalize logs.raw event: %w", err))
	}
	if writer == nil {
		return fmt.Errorf("processor writer is not configured")
	}
	alreadyProcessed, err := writer.AlreadyProcessed(ctx, batch.RequestID)
	if err != nil {
		return fmt.Errorf("check existing ingest id: %w", err)
	}
	if alreadyProcessed {
		logger.Info(
			"processor skipped replayed batch",
			"request_id", batch.RequestID,
			"fingerprint", batch.Fingerprint,
			"tenant_id", batch.TenantID,
		)
		return nil
	}
	rules, err := loadAlertRules(ctx, ruleStore, batch)
	if err != nil {
		return fmt.Errorf("load alert rules: %w", err)
	}
	triggers, err := evaluateAlertRules(rules, normalized)
	if err != nil {
		return fmt.Errorf("evaluate alert rules: %w", err)
	}
	if err := writer.WriteBatch(ctx, normalized); err != nil {
		return fmt.Errorf("persist normalized logs: %w", err)
	}
	stateChanges, err := syncAlertState(ctx, ruleStore, rules, triggers, alertObservedAt(batch, normalized))
	if err != nil {
		return fmt.Errorf("sync alert state: %w", err)
	}
	if err := dispatchDueNotifications(ctx, ruleStore, dispatcher, alertObservedAt(batch, normalized)); err != nil {
		return fmt.Errorf("dispatch notifications: %w", err)
	}

	for _, change := range stateChanges {
		logger.Info(
			"processor alert state updated",
			"rule_id", change.RuleID,
			"rule_name", change.RuleName,
			"dedupe_key", change.DedupeKey,
			"status", change.Status,
			"event_type", change.EventType,
			"match_count", change.MatchCount,
		)
	}

	logger.Info(
		"processor persisted batch",
		"request_id", batch.RequestID,
		"tenant_id", batch.TenantID,
		"received_at", batch.ReceivedAt,
		"schema_version", batch.SchemaVersion,
		"service", batch.Service,
		"env", batch.Env,
		"source", batch.Source,
		"log_count", len(batch.Logs),
		"normalized_log_count", len(normalized),
		"loaded_alert_rule_count", len(rules),
		"triggered_alert_count", len(triggers),
		"alert_state_change_count", len(stateChanges),
	)

	return nil
}

func loadAlertRules(ctx context.Context, store AlertRuleStore, batch contracts.LogsRawEvent) ([]alerts.Rule, error) {
	if store == nil {
		return nil, nil
	}
	return store.LoadActiveRules(ctx, batch.TenantID, batch.Service, batch.Env)
}

func evaluateAlertRules(rules []alerts.Rule, records []NormalizedLogRecord) ([]alerts.Trigger, error) {
	alertRecords := make([]alerts.Record, 0, len(records))
	for _, record := range records {
		fields := map[string]any{}
		if record.FieldsJSON != "" {
			if err := json.Unmarshal([]byte(record.FieldsJSON), &fields); err != nil {
				return nil, fmt.Errorf("decode normalized fields_json: %w", err)
			}
		}
		alertRecords = append(alertRecords, alerts.Record{
			Timestamp:   record.Timestamp,
			TenantID:    record.TenantID,
			Service:     record.Service,
			Environment: record.Environment,
			Source:      record.Source,
			Host:        record.Host,
			Level:       record.Level,
			TraceID:     record.TraceID,
			Fingerprint: record.Fingerprint,
			Message:     record.Message,
			Fields:      fields,
			IngestID:    record.IngestID,
		})
	}

	triggers := make([]alerts.Trigger, 0)
	for _, rule := range rules {
		ruleTriggers, err := alerts.Evaluate(rule, alertRecords)
		if err != nil {
			return nil, fmt.Errorf("rule %d: %w", rule.ID, err)
		}
		triggers = append(triggers, ruleTriggers...)
	}

	return triggers, nil
}

func syncAlertState(ctx context.Context, store AlertRuleStore, rules []alerts.Rule, triggers []alerts.Trigger, observedAt time.Time) ([]alerts.StateChange, error) {
	if store == nil || len(rules) == 0 {
		return nil, nil
	}
	return store.SyncAlertState(ctx, rules, triggers, observedAt)
}

func dispatchDueNotifications(ctx context.Context, store AlertRuleStore, dispatcher alerts.NotificationDispatcher, observedAt time.Time) error {
	if store == nil || dispatcher == nil {
		return nil
	}
	return store.DispatchDueNotifications(ctx, dispatcher, observedAt)
}

func alertObservedAt(batch contracts.LogsRawEvent, records []NormalizedLogRecord) time.Time {
	observedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(batch.ReceivedAt))
	if err == nil {
		return observedAt.UTC()
	}

	var latest time.Time
	for _, record := range records {
		if record.Timestamp.After(latest) {
			latest = record.Timestamp
		}
	}
	if !latest.IsZero() {
		return latest.UTC()
	}
	return time.Now().UTC()
}
