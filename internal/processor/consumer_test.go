package processor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/alerts"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/logging"
)

func TestHandleBatchAcceptsPublishedBatch(t *testing.T) {
	logger := &stubLogger{}
	writer := &stubLogWriter{}
	ruleStore := &stubAlertRuleStore{
		rules: []alerts.Rule{{
			ID:        7,
			Name:      "error spike",
			RuleType:  "count_threshold",
			Threshold: "10",
		}},
	}
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		Fingerprint:   "req-123",
		ReceivedAt:    "2026-07-09T20:12:07Z",
		TenantID:      42,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []contracts.LogsRawRecord{
			{
				Timestamp: "2026-07-07T16:00:00Z",
				Level:     "error",
				Message:   "database timeout",
			},
		},
	}

	if err := handleBatch(context.Background(), logger, writer, ruleStore, &stubNotificationDispatcher{}, nil, batch); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if logger.infoCalls != 1 {
		t.Fatalf("expected one info log call, got %d", logger.infoCalls)
	}
	if writer.calls != 1 {
		t.Fatalf("expected one writer call, got %d", writer.calls)
	}
	if len(writer.lastBatch) != 1 || writer.lastBatch[0].TenantID != 42 {
		t.Fatalf("expected tenant id 42 in written records, got %+v", writer.lastBatch)
	}
	if ruleStore.calls != 1 || ruleStore.tenantID != 42 || ruleStore.service != "checkout" || ruleStore.environment != "prod" {
		t.Fatalf("expected rules to load for tenant/service/env, got %+v", ruleStore)
	}
	if ruleStore.syncCalls != 1 {
		t.Fatalf("expected state sync, got %d", ruleStore.syncCalls)
	}
	if ruleStore.dispatchCalls != 1 {
		t.Fatalf("expected notification dispatch, got %d", ruleStore.dispatchCalls)
	}
}

func TestHandleBatchUsesRequestIDAsCorrelationFallback(t *testing.T) {
	logger := &stubLogger{}
	writer := &stubLogWriter{}
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "batch-req-123",
		Fingerprint:   "batch-req-123",
		ReceivedAt:    "2026-07-09T20:12:07Z",
		TenantID:      42,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []contracts.LogsRawRecord{
			{Timestamp: "2026-07-07T16:00:00Z", Level: "error", Message: "database timeout"},
		},
	}

	if err := handleBatch(context.Background(), logger, writer, nil, &stubNotificationDispatcher{}, nil, batch); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if got := logging.RequestIDFromContext(writer.alreadyProcessedCtx); got != "batch-req-123" {
		t.Fatalf("expected request id fallback in already-processed context, got %q", got)
	}
	if got := logging.RequestIDFromContext(writer.writeCtx); got != "batch-req-123" {
		t.Fatalf("expected request id fallback in write context, got %q", got)
	}
}

func TestHandleBatchRejectsMissingRequestID(t *testing.T) {
	logger := &stubLogger{}
	writer := &stubLogWriter{}

	err := handleBatch(context.Background(), logger, writer, nil, &stubNotificationDispatcher{}, nil, contracts.LogsRawEvent{})
	if err == nil {
		t.Fatal("expected error for missing request id")
	}
	if logger.infoCalls != 0 {
		t.Fatalf("expected no info logs, got %d", logger.infoCalls)
	}
	if writer.calls != 0 {
		t.Fatalf("expected no writer calls, got %d", writer.calls)
	}
}

func TestHandleBatchReturnsWriterError(t *testing.T) {
	logger := &stubLogger{}
	writer := &stubLogWriter{err: errors.New("clickhouse unavailable")}
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		Fingerprint:   "req-123",
		ReceivedAt:    "2026-07-09T20:12:07Z",
		TenantID:      42,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []contracts.LogsRawRecord{
			{
				Timestamp: "2026-07-07T16:00:00Z",
				Level:     "error",
				Message:   "database timeout",
			},
		},
	}

	err := handleBatch(context.Background(), logger, writer, nil, &stubNotificationDispatcher{}, nil, batch)
	if err == nil || !strings.Contains(err.Error(), "persist normalized logs") {
		t.Fatalf("expected persist error, got %v", err)
	}
	if logger.infoCalls != 0 {
		t.Fatalf("expected no info logs when write fails, got %d", logger.infoCalls)
	}
}

func TestHandleBatchReturnsAlertRuleLoadError(t *testing.T) {
	logger := &stubLogger{}
	writer := &stubLogWriter{}
	ruleStore := &stubAlertRuleStore{err: errors.New("postgres unavailable")}
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		Fingerprint:   "req-123",
		ReceivedAt:    "2026-07-09T20:12:07Z",
		TenantID:      42,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []contracts.LogsRawRecord{
			{
				Timestamp: "2026-07-07T16:00:00Z",
				Level:     "error",
				Message:   "database timeout",
			},
		},
	}

	err := handleBatch(context.Background(), logger, writer, ruleStore, &stubNotificationDispatcher{}, nil, batch)
	if err == nil || !strings.Contains(err.Error(), "load alert rules") {
		t.Fatalf("expected alert rule load error, got %v", err)
	}
	if writer.calls != 0 {
		t.Fatalf("expected no writer calls when rule loading fails, got %d", writer.calls)
	}
	if ruleStore.syncCalls != 0 {
		t.Fatalf("expected no state sync when rule loading fails, got %d", ruleStore.syncCalls)
	}
	if ruleStore.dispatchCalls != 0 {
		t.Fatalf("expected no dispatch when rule loading fails, got %d", ruleStore.dispatchCalls)
	}
}

func TestHandleBatchEvaluatesCountThresholdRule(t *testing.T) {
	logger := &stubLogger{}
	writer := &stubLogWriter{}
	ruleStore := &stubAlertRuleStore{
		rules: []alerts.Rule{
			{
				ID:         9,
				Name:       "error spike",
				RuleType:   "count_threshold",
				Severity:   "critical",
				FilterJSON: []byte(`{"level":"error"}`),
				Threshold:  "2",
			},
		},
	}
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		Fingerprint:   "req-123",
		ReceivedAt:    "2026-07-09T20:12:07Z",
		TenantID:      42,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []contracts.LogsRawRecord{
			{Timestamp: "2026-07-07T16:00:00Z", Level: "error", Message: "database timeout"},
			{Timestamp: "2026-07-07T16:00:01Z", Level: "error", Message: "database timeout"},
		},
	}

	if err := handleBatch(context.Background(), logger, writer, ruleStore, &stubNotificationDispatcher{}, nil, batch); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if logger.infoCalls != 2 {
		t.Fatalf("expected state change log plus batch log, got %d", logger.infoCalls)
	}
	if ruleStore.syncCalls != 1 || len(ruleStore.syncedTriggers) != 1 {
		t.Fatalf("expected synced triggers, got %+v", ruleStore)
	}
	if ruleStore.dispatchCalls != 1 {
		t.Fatalf("expected dispatch attempt, got %d", ruleStore.dispatchCalls)
	}
}

func TestHandleBatchReturnsAlertStateSyncError(t *testing.T) {
	logger := &stubLogger{}
	writer := &stubLogWriter{}
	ruleStore := &stubAlertRuleStore{
		rules: []alerts.Rule{
			{
				ID:         9,
				Name:       "error spike",
				RuleType:   "count_threshold",
				Severity:   "critical",
				FilterJSON: []byte(`{"level":"error"}`),
				Threshold:  "1",
			},
		},
		syncErr: errors.New("postgres write failed"),
	}
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		Fingerprint:   "req-123",
		ReceivedAt:    "2026-07-09T20:12:07Z",
		TenantID:      42,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []contracts.LogsRawRecord{
			{Timestamp: "2026-07-07T16:00:00Z", Level: "error", Message: "database timeout"},
		},
	}

	err := handleBatch(context.Background(), logger, writer, ruleStore, &stubNotificationDispatcher{}, nil, batch)
	if err == nil || !strings.Contains(err.Error(), "sync alert state") {
		t.Fatalf("expected alert state sync error, got %v", err)
	}
}

func TestHandleBatchReturnsNotificationDispatchError(t *testing.T) {
	logger := &stubLogger{}
	writer := &stubLogWriter{}
	ruleStore := &stubAlertRuleStore{
		rules: []alerts.Rule{
			{
				ID:         9,
				Name:       "error spike",
				RuleType:   "count_threshold",
				Severity:   "critical",
				FilterJSON: []byte(`{"level":"error"}`),
				Threshold:  "1",
			},
		},
		dispatchErr: errors.New("delivery failed"),
	}
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		Fingerprint:   "req-123",
		ReceivedAt:    "2026-07-09T20:12:07Z",
		TenantID:      42,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []contracts.LogsRawRecord{
			{Timestamp: "2026-07-07T16:00:00Z", Level: "error", Message: "database timeout"},
		},
	}

	err := handleBatch(context.Background(), logger, writer, ruleStore, &stubNotificationDispatcher{}, nil, batch)
	if err == nil || !strings.Contains(err.Error(), "dispatch notifications") {
		t.Fatalf("expected notification dispatch error, got %v", err)
	}
}

func TestNormalizeBatchNormalizesRecords(t *testing.T) {
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		Fingerprint:   "req-123",
		ReceivedAt:    "2026-07-09T20:12:07.612769-07:00",
		TenantID:      42,
		Service:       " Checkout ",
		Env:           " PROD ",
		Source:        " App ",
		Logs: []contracts.LogsRawRecord{
			{
				Timestamp: "2026-07-07T16:00:00-07:00",
				Level:     " WARNING ",
				Message:   " database timeout ",
				Fields: map[string]any{
					"trace_id": "trace-123",
					"host":     "api-1",
					"region":   "us-west-2",
					"attempt":  float64(3),
				},
			},
		},
	}

	records, err := normalizeBatch(batch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected one record, got %d", len(records))
	}

	record := records[0]
	expectedTimestamp := time.Date(2026, 7, 7, 23, 0, 0, 0, time.UTC)
	if !record.Timestamp.Equal(expectedTimestamp) {
		t.Fatalf("expected timestamp %s, got %s", expectedTimestamp, record.Timestamp)
	}
	if record.TenantID != 42 {
		t.Fatalf("expected tenant id 42, got %d", record.TenantID)
	}
	if record.Service != "checkout" {
		t.Fatalf("expected service checkout, got %q", record.Service)
	}
	if record.Environment != "prod" {
		t.Fatalf("expected environment prod, got %q", record.Environment)
	}
	if record.Source != "app" {
		t.Fatalf("expected source app, got %q", record.Source)
	}
	if record.Level != "warn" {
		t.Fatalf("expected level warn, got %q", record.Level)
	}
	if record.Host != "api-1" {
		t.Fatalf("expected host api-1, got %q", record.Host)
	}
	if record.TraceID != "trace-123" {
		t.Fatalf("expected trace id trace-123, got %q", record.TraceID)
	}
	if record.Message != "database timeout" {
		t.Fatalf("expected trimmed message, got %q", record.Message)
	}
	if record.FieldsJSON != `{"attempt":3,"region":"us-west-2"}` {
		t.Fatalf("unexpected fields json: %s", record.FieldsJSON)
	}
	if record.IngestID != "req-123" {
		t.Fatalf("expected ingest id req-123, got %q", record.IngestID)
	}
	if record.RawSizeBytes == 0 {
		t.Fatal("expected raw_size_bytes to be populated")
	}
	if len(record.Fingerprint) != 32 {
		t.Fatalf("expected 32 hex chars in fingerprint, got %q", record.Fingerprint)
	}
}

func TestNormalizeBatchUsesBatchTraceIDWhenRecordOmitsIt(t *testing.T) {
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		TraceID:       "0af7651916cd43dd8448eb211c80319c",
		Fingerprint:   "req-123",
		ReceivedAt:    "2026-07-09T20:12:07Z",
		TenantID:      42,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []contracts.LogsRawRecord{{
			Timestamp: "2026-07-09T20:12:07Z",
			Level:     "info",
			Message:   "request completed",
		}},
	}

	records, err := normalizeBatch(batch)
	if err != nil {
		t.Fatalf("normalize batch: %v", err)
	}
	if got := records[0].TraceID; got != batch.TraceID {
		t.Fatalf("expected inherited trace id %q, got %q", batch.TraceID, got)
	}
}

func TestNormalizeBatchFallsBackToReceivedAtForMissingTimestamp(t *testing.T) {
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		Fingerprint:   "req-123",
		ReceivedAt:    "2026-07-09T20:12:07.612769Z",
		TenantID:      42,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []contracts.LogsRawRecord{
			{
				Level:   "error",
				Message: "database timeout",
			},
		},
	}

	records, err := normalizeBatch(batch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if got := records[0].Timestamp.Format(time.RFC3339Nano); got != "2026-07-09T20:12:07.612769Z" {
		t.Fatalf("expected fallback timestamp from received_at, got %s", got)
	}
}

func TestNormalizeBatchRejectsInvalidRecordTimestamp(t *testing.T) {
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		Fingerprint:   "req-123",
		ReceivedAt:    "2026-07-09T20:12:07Z",
		TenantID:      42,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []contracts.LogsRawRecord{
			{
				Timestamp: "yesterday",
				Level:     "error",
				Message:   "database timeout",
			},
		},
	}

	_, err := normalizeBatch(batch)
	if err == nil {
		t.Fatal("expected invalid timestamp error")
	}
	if !strings.Contains(err.Error(), "parse log timestamp") {
		t.Fatalf("expected parse log timestamp error, got %v", err)
	}
}

func TestNormalizeBatchProducesStableFingerprint(t *testing.T) {
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		Fingerprint:   "req-123",
		ReceivedAt:    "2026-07-09T20:12:07Z",
		TenantID:      42,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []contracts.LogsRawRecord{
			{
				Timestamp: "2026-07-07T16:00:00Z",
				Level:     "ERROR",
				Message:   "database timeout",
				Fields: map[string]any{
					"b": "two",
					"a": "one",
				},
			},
			{
				Timestamp: "2026-07-08T16:00:00Z",
				Level:     "error",
				Message:   "database timeout",
				Fields: map[string]any{
					"a": "one",
					"b": "two",
				},
			},
		},
	}

	records, err := normalizeBatch(batch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if records[0].Fingerprint != records[1].Fingerprint {
		t.Fatalf("expected stable fingerprints, got %q and %q", records[0].Fingerprint, records[1].Fingerprint)
	}
}

func TestHandleBatchSkipsAlreadyProcessedBatch(t *testing.T) {
	logger := &stubLogger{}
	writer := &stubLogWriter{alreadyProcessed: true}
	batch := contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     "req-123",
		Fingerprint:   "req-123",
		ReceivedAt:    "2026-07-09T20:12:07Z",
		TenantID:      42,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []contracts.LogsRawRecord{
			{Timestamp: "2026-07-07T16:00:00Z", Level: "error", Message: "database timeout"},
		},
	}

	if err := handleBatch(context.Background(), logger, writer, nil, &stubNotificationDispatcher{}, nil, batch); err != nil {
		t.Fatalf("expected replayed batch to be skipped cleanly, got %v", err)
	}
	if writer.alreadyProcessedChecks != 1 {
		t.Fatalf("expected one already-processed check, got %d", writer.alreadyProcessedChecks)
	}
	if writer.calls != 0 {
		t.Fatalf("expected no writes for replayed batch, got %d", writer.calls)
	}
}

type stubLogger struct {
	infoCalls int
}

func (l *stubLogger) Info(_ string, _ ...any) {
	l.infoCalls++
}

func (l *stubLogger) Error(_ string, _ ...any) {}

type stubLogWriter struct {
	lastBatch              []NormalizedLogRecord
	alreadyProcessedCtx    context.Context
	writeCtx               context.Context
	calls                  int
	err                    error
	alreadyProcessed       bool
	alreadyProcessedErr    error
	alreadyProcessedChecks int
}

func (w *stubLogWriter) AlreadyProcessed(ctx context.Context, _ uint64, _ string) (bool, error) {
	w.alreadyProcessedChecks++
	w.alreadyProcessedCtx = ctx
	return w.alreadyProcessed, w.alreadyProcessedErr
}

func (w *stubLogWriter) WriteBatch(ctx context.Context, batch []NormalizedLogRecord) error {
	w.calls++
	w.writeCtx = ctx
	w.lastBatch = append([]NormalizedLogRecord(nil), batch...)
	return w.err
}

type stubAlertRuleStore struct {
	tenantID       uint64
	service        string
	environment    string
	rules          []alerts.Rule
	calls          int
	err            error
	syncedRules    []alerts.Rule
	syncedTriggers []alerts.Trigger
	syncObservedAt time.Time
	syncCalls      int
	syncErr        error
	dispatchCalls  int
	dispatchErr    error
}

func (s *stubAlertRuleStore) LoadActiveRules(_ context.Context, tenantID uint64, service, environment string) ([]alerts.Rule, error) {
	s.calls++
	s.tenantID = tenantID
	s.service = service
	s.environment = environment
	return s.rules, s.err
}

func (s *stubAlertRuleStore) SyncAlertState(_ context.Context, rules []alerts.Rule, triggers []alerts.Trigger, observedAt time.Time) ([]alerts.StateChange, error) {
	s.syncCalls++
	s.syncedRules = append([]alerts.Rule(nil), rules...)
	s.syncedTriggers = append([]alerts.Trigger(nil), triggers...)
	s.syncObservedAt = observedAt
	if s.syncErr != nil {
		return nil, s.syncErr
	}

	changes := make([]alerts.StateChange, 0, len(triggers))
	for _, trigger := range triggers {
		changes = append(changes, alerts.StateChange{
			RuleID:     trigger.RuleID,
			RuleName:   trigger.RuleName,
			DedupeKey:  trigger.GroupKey,
			Status:     alerts.AlertStatusActive,
			EventType:  alerts.AlertEventTriggered,
			MatchCount: trigger.MatchCount,
		})
	}
	return changes, nil
}

func (s *stubAlertRuleStore) DispatchDueNotifications(_ context.Context, _ alerts.NotificationDispatcher, _ time.Time) error {
	s.dispatchCalls++
	return s.dispatchErr
}

type stubNotificationDispatcher struct{}

func (s *stubNotificationDispatcher) Dispatch(context.Context, alerts.NotificationDelivery, alerts.NotificationPayload) error {
	return nil
}
