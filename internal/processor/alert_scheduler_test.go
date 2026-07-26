package processor

import (
	"context"
	"testing"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/alerts"
)

func TestRunScheduledAlertEvaluationSyncsFiringAndResolvedState(t *testing.T) {
	observedAt := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	rule := alerts.Rule{ID: 5, TenantID: 7, Name: "payment errors", RuleType: "count_threshold", WindowSeconds: 300, Threshold: "2"}
	recordStore := &stubScheduledAlertRecordStore{
		records: []alerts.Record{
			{Timestamp: observedAt.Add(-time.Minute), TenantID: 7, Service: "payment-api", Level: "error"},
			{Timestamp: observedAt.Add(-30 * time.Second), TenantID: 7, Service: "payment-api", Level: "error"},
		},
	}
	ruleStore := &stubScheduledAlertRuleStore{rules: []alerts.Rule{rule}}
	metrics := &stubScheduledAlertMetrics{}

	err := RunScheduledAlertEvaluation(context.Background(), recordStore, ruleStore, nil, metrics, SchedulerOptions{}, observedAt)
	if err != nil {
		t.Fatalf("scheduled evaluation failed: %v", err)
	}
	if recordStore.rule.ID != rule.ID || !recordStore.start.Equal(observedAt.Add(-5*time.Minute)) || !recordStore.end.Equal(observedAt) {
		t.Fatalf("unexpected record query: rule=%+v start=%s end=%s", recordStore.rule, recordStore.start, recordStore.end)
	}
	if len(ruleStore.syncedRules) != 1 || len(ruleStore.syncedTriggers) != 1 || ruleStore.syncedTriggers[0].MatchCount != 2 {
		t.Fatalf("unexpected state sync: rules=%+v triggers=%+v", ruleStore.syncedRules, ruleStore.syncedTriggers)
	}
	if len(metrics.changes) != 1 || metrics.changes[0].Status != alerts.AlertStatusFiring {
		t.Fatalf("unexpected metrics changes: %+v", metrics.changes)
	}
}

type stubScheduledAlertRecordStore struct {
	rule    alerts.Rule
	start   time.Time
	end     time.Time
	records []alerts.Record
}

func (s *stubScheduledAlertRecordStore) QueryAlertRecords(_ context.Context, rule alerts.Rule, start, end time.Time, _ int) ([]alerts.Record, error) {
	s.rule = rule
	s.start = start
	s.end = end
	return s.records, nil
}

type stubScheduledAlertRuleStore struct {
	rules           []alerts.Rule
	syncedRules     []alerts.Rule
	syncedTriggers  []alerts.Trigger
	dispatchedAt    time.Time
	dispatchInvoked bool
}

type stubScheduledAlertMetrics struct {
	changes []alerts.StateChange
}

func (s *stubScheduledAlertMetrics) ObserveStateChanges(changes []alerts.StateChange) {
	s.changes = append([]alerts.StateChange(nil), changes...)
}

func (s *stubScheduledAlertRuleStore) LoadAllActiveRules(context.Context) ([]alerts.Rule, error) {
	return s.rules, nil
}

func (s *stubScheduledAlertRuleStore) SyncAlertState(_ context.Context, rules []alerts.Rule, triggers []alerts.Trigger, observedAt time.Time) ([]alerts.StateChange, error) {
	s.syncedRules = rules
	s.syncedTriggers = triggers
	return []alerts.StateChange{{RuleID: rules[0].ID, Status: alerts.AlertStatusFiring, EventType: alerts.AlertEventTriggered}}, nil
}

func (s *stubScheduledAlertRuleStore) DispatchDueNotifications(_ context.Context, _ alerts.NotificationDispatcher, observedAt time.Time) error {
	s.dispatchInvoked = true
	s.dispatchedAt = observedAt
	return nil
}
