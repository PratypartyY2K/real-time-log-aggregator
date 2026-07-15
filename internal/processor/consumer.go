package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/alerts"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/stream"
)

func Run(ctx context.Context, logger app.Logger, cfg config.Config, metrics *Metrics, ruleStore AlertRuleStore) error {
	nc, consumer, err := stream.ConnectJetStreamConsumer(
		cfg.NATSURL,
		cfg.NATSStream,
		cfg.NATSSubject,
		cfg.NATSDLQSubject,
		cfg.NATSDurable,
		cfg.NATSMaxDeliver,
	)
	if err != nil {
		return err
	}
	defer nc.Drain()

	writer := NewClickHouseWriter(cfg.ClickHouseDSN)

	logger.Info("processor consumer started", "stream", cfg.NATSStream, "subject", cfg.NATSSubject, "durable", cfg.NATSDurable)

	return consumer.Consume(ctx, func(ctx context.Context, batch contracts.LogsRawEvent) error {
		start := time.Now()
		err := handleBatch(ctx, logger, writer, ruleStore, batch)
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
}

func handleBatch(ctx context.Context, logger app.Logger, writer LogWriter, ruleStore AlertRuleStore, batch contracts.LogsRawEvent) error {
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

	for _, trigger := range triggers {
		logger.Info(
			"processor alert rule triggered",
			"rule_id", trigger.RuleID,
			"rule_name", trigger.RuleName,
			"rule_type", trigger.RuleType,
			"severity", trigger.Severity,
			"group_key", trigger.GroupKey,
			"match_count", trigger.MatchCount,
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
