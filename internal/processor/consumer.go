package processor

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/alerts"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/logging"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/stream"
)

func Run(ctx context.Context, logger app.Logger, cfg config.Config, metrics *Metrics, alertMetrics AlertMetrics, ruleStore AlertRuleStore, dispatchers ...alerts.NotificationDispatcher) error {
	nc, consumer, err := stream.ConnectJetStreamConsumer(stream.ConsumerOptions{
		URL:           cfg.NATSURL,
		StreamName:    cfg.NATSStream,
		Subject:       cfg.NATSSubject,
		DLQSubject:    cfg.NATSDLQSubject,
		Durable:       cfg.NATSDurable,
		MaxDeliver:    cfg.NATSMaxDeliver,
		ReplayMode:    cfg.NATSReplayMode,
		ReplaySeq:     cfg.NATSReplaySeq,
		ReplayTime:    cfg.NATSReplayTime,
		DLQObserver:   metrics,
		RetryObserver: metrics,
	})
	if err != nil {
		return err
	}
	defer nc.Drain()

	writer := NewClickHouseWriter(cfg.ClickHouseDSN)
	writer.metrics = metrics
	dispatcher := alerts.NewLogDispatcher(logger)
	if len(dispatchers) > 0 && dispatchers[0] != nil {
		dispatcher = dispatchers[0]
	}
	ruleEngine := alerts.NewEngine()
	if schedulerRuleStore, ok := ruleStore.(scheduledRuleStore); ok && cfg.AlertEvaluationInterval > 0 {
		go RunAlertScheduler(ctx, logger, writer, schedulerRuleStore, dispatcher, alertMetrics, SchedulerOptions{
			Interval:   cfg.AlertEvaluationInterval,
			MaxRecords: cfg.AlertEvaluationMaxRecords,
			Evaluator:  ruleEngine.Evaluate,
		})
	}
	if deliveryStore, ok := ruleStore.(alerts.DeliveryBatchStore); ok {
		go alerts.RunDeliveryWorker(ctx, logger, deliveryStore, dispatcher, alerts.DeliveryPolicy{
			WorkerID:       alerts.NewDeliveryWorkerID(cfg.ServiceName),
			MaxAttempts:    cfg.NotificationMaxAttempts,
			BaseRetryDelay: cfg.NotificationRetryBase,
			MaxRetryDelay:  cfg.NotificationRetryMax,
			LeaseDuration:  cfg.NotificationLeaseDuration,
			BatchSize:      cfg.NotificationBatchSize,
		}, cfg.NotificationPollInterval)
	}

	logger.Info("processor consumer started", "stream", cfg.NATSStream, "subject", cfg.NATSSubject, "durable", cfg.NATSDurable, "replay_mode", cfg.NATSReplayMode)

	return consumer.Consume(ctx, func(ctx context.Context, batch contracts.LogsRawEvent) error {
		start := time.Now()
		err := handleBatchWithEvaluator(ctx, logger, writer, ruleStore, dispatcher, alertMetrics, ruleEngine.Evaluate, batch)
		result := resultSuccess
		if err != nil {
			result = resultRetryable
		}
		if err != nil && stream.IsPoisonBatchError(err) {
			result = resultInvalidBatch
		}
		completedAt := time.Now()
		metrics.ObserveBatch(result, len(batch.Logs), completedAt.Sub(start))
		if receivedAt, parseErr := time.Parse(time.RFC3339Nano, batch.ReceivedAt); parseErr == nil {
			metrics.ObserveEndToEnd(result, receivedAt, completedAt)
		}
		if err != nil {
			logger.Error(
				"processor failed to handle batch",
				"operation", "process_batch",
				"error", err,
				"result", result,
				"request_id", batch.RequestID,
				"correlation_id", batch.CorrelationID,
				"trace_id", batch.TraceID,
				"tenant_id", batch.TenantID,
				"schema_version", batch.SchemaVersion,
				"service", batch.Service,
				"env", batch.Env,
				"source", batch.Source,
				"log_count", len(batch.Logs),
				"duration_ms", completedAt.Sub(start).Milliseconds(),
			)
		}
		return err
	}, nil)
}

type AlertRuleStore interface {
	LoadActiveRules(context.Context, uint64, string, string) ([]alerts.Rule, error)
	SyncAlertState(context.Context, []alerts.Rule, []alerts.Trigger, time.Time) ([]alerts.StateChange, error)
	DispatchDueNotifications(context.Context, alerts.NotificationDispatcher, time.Time) error
}

type AlertMetrics interface {
	ObserveStateChanges([]alerts.StateChange)
}

func handleBatch(ctx context.Context, logger app.Logger, writer LogWriter, ruleStore AlertRuleStore, dispatcher alerts.NotificationDispatcher, alertMetrics AlertMetrics, batch contracts.LogsRawEvent) error {
	return handleBatchWithEvaluator(ctx, logger, writer, ruleStore, dispatcher, alertMetrics, alerts.Evaluate, batch)
}

type ruleEvaluator func(alerts.Rule, []alerts.Record) ([]alerts.Trigger, error)

func handleBatchWithEvaluator(ctx context.Context, logger app.Logger, writer LogWriter, ruleStore AlertRuleStore, dispatcher alerts.NotificationDispatcher, alertMetrics AlertMetrics, evaluator ruleEvaluator, batch contracts.LogsRawEvent) error {
	if err := batch.Validate(); err != nil {
		return stream.MarkPoisonBatch(fmt.Errorf("invalid logs.raw event: %w", err))
	}
	correlationID := strings.TrimSpace(batch.CorrelationID)
	if correlationID == "" {
		correlationID = batch.RequestID
	}
	ctx = logging.WithRequestID(ctx, correlationID)
	ctx = logging.WithTraceID(ctx, batch.TraceID)

	normalized, err := normalizeBatch(batch)
	if err != nil {
		return stream.MarkPoisonBatch(fmt.Errorf("normalize logs.raw event: %w", err))
	}
	if writer == nil {
		return fmt.Errorf("processor writer is not configured")
	}
	alreadyProcessed, err := writer.AlreadyProcessed(ctx, batch.TenantID, batch.RequestID)
	if err != nil {
		return fmt.Errorf("check existing ingest id: %w", err)
	}
	if alreadyProcessed {
		logger.Info(
			"processor skipped replayed batch",
			"operation", "dedupe_check",
			"request_id", batch.RequestID,
			"correlation_id", batch.CorrelationID,
			"trace_id", batch.TraceID,
			"fingerprint", batch.Fingerprint,
			"tenant_id", batch.TenantID,
			"service", batch.Service,
			"env", batch.Env,
		)
		return nil
	}
	rules, err := loadAlertRules(ctx, ruleStore, batch)
	if err != nil {
		return fmt.Errorf("load alert rules: %w", err)
	}
	triggers, err := evaluateAlertRules(rules, normalized, evaluator)
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
	if alertMetrics != nil {
		alertMetrics.ObserveStateChanges(stateChanges)
	}
	if _, handledByWorker := ruleStore.(alerts.DeliveryBatchStore); !handledByWorker {
		if err := dispatchDueNotifications(ctx, ruleStore, dispatcher, alertObservedAt(batch, normalized)); err != nil {
			return fmt.Errorf("dispatch notifications: %w", err)
		}
	}

	for _, change := range stateChanges {
		logger.Info(
			"processor alert state updated",
			"operation", "sync_alert_state",
			"request_id", batch.RequestID,
			"correlation_id", batch.CorrelationID,
			"trace_id", batch.TraceID,
			"tenant_id", batch.TenantID,
			"rule_id", change.RuleID,
			"rule_name", change.RuleName,
			"dedupe_key", change.DedupeKey,
			"status", change.Status,
			"event_type", change.EventType,
			"match_count", change.MatchCount,
			"metric_value", change.MetricValue,
			"threshold", change.Threshold,
			"window_seconds", change.WindowSeconds,
		)
	}

	logger.Info(
		"processor persisted batch",
		"operation", "process_batch",
		"request_id", batch.RequestID,
		"correlation_id", batch.CorrelationID,
		"trace_id", batch.TraceID,
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

func evaluateAlertRules(rules []alerts.Rule, records []NormalizedLogRecord, evaluators ...ruleEvaluator) ([]alerts.Trigger, error) {
	evaluator := ruleEvaluator(alerts.Evaluate)
	if len(evaluators) > 0 && evaluators[0] != nil {
		evaluator = evaluators[0]
	}
	alertRecords, err := recordsFromNormalized(records)
	if err != nil {
		return nil, err
	}

	triggers := make([]alerts.Trigger, 0)
	for _, rule := range rules {
		ruleTriggers, err := evaluator(rule, alertRecords)
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
