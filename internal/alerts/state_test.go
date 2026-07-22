package alerts

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"
)

func TestBuildEventPayloadIncludesMetricEvaluation(t *testing.T) {
	payload, err := buildEventPayload(StateChange{RuleID: 9, RuleName: "latency p95", MetricValue: 385, Threshold: 300, WindowSeconds: 300, Percentile: 95, ValueField: "field.duration_ms"})
	if err != nil {
		t.Fatalf("build payload: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if decoded["metric_value"] != 385.0 || decoded["threshold"] != 300.0 || decoded["percentile"] != 95.0 || decoded["value_field"] != "field.duration_ms" {
		t.Fatalf("unexpected metric payload: %+v", decoded)
	}
}

func TestReconcileStateCreatesNewActiveInstance(t *testing.T) {
	t.Parallel()

	observedAt := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	plan, changes, err := reconcileState(
		[]Rule{{ID: 7, Name: "error spike", CooldownSeconds: 600}},
		[]Trigger{{RuleID: 7, RuleName: "error spike", GroupKey: "service=checkout", MatchCount: 3}},
		nil,
		observedAt,
	)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(plan.upserts) != 1 || !plan.upserts[0].insert {
		t.Fatalf("expected insert plan, got %+v", plan)
	}
	if len(changes) != 1 || changes[0].EventType != AlertEventTriggered || changes[0].Status != AlertStatusActive {
		t.Fatalf("unexpected changes: %+v", changes)
	}
}

func TestReconcileStateSuppressesWithinCooldown(t *testing.T) {
	t.Parallel()

	observedAt := time.Date(2026, 7, 15, 12, 5, 0, 0, time.UTC)
	plan, changes, err := reconcileState(
		[]Rule{{ID: 7, Name: "error spike", CooldownSeconds: 600}},
		[]Trigger{{RuleID: 7, RuleName: "error spike", GroupKey: "service=checkout", MatchCount: 3}},
		[]Instance{{
			ID:           11,
			RuleID:       7,
			DedupeKey:    "service=checkout",
			Status:       AlertStatusActive,
			FirstFiredAt: observedAt.Add(-5 * time.Minute),
			LastFiredAt:  observedAt.Add(-5 * time.Minute),
		}},
		observedAt,
	)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(plan.upserts) != 0 {
		t.Fatalf("expected no instance update during cooldown, got %+v", plan)
	}
	if len(changes) != 1 || changes[0].EventType != AlertEventSuppressed {
		t.Fatalf("unexpected changes: %+v", changes)
	}
}

func TestReconcileStateRetriggersAfterCooldown(t *testing.T) {
	t.Parallel()

	observedAt := time.Date(2026, 7, 15, 12, 20, 0, 0, time.UTC)
	plan, changes, err := reconcileState(
		[]Rule{{ID: 7, Name: "error spike", CooldownSeconds: 600}},
		[]Trigger{{RuleID: 7, RuleName: "error spike", GroupKey: "service=checkout", MatchCount: 3}},
		[]Instance{{
			ID:           11,
			RuleID:       7,
			DedupeKey:    "service=checkout",
			Status:       AlertStatusActive,
			FirstFiredAt: observedAt.Add(-20 * time.Minute),
			LastFiredAt:  observedAt.Add(-20 * time.Minute),
		}},
		observedAt,
	)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(plan.upserts) != 1 || plan.upserts[0].insert {
		t.Fatalf("expected update plan, got %+v", plan)
	}
	if len(changes) != 1 || changes[0].EventType != AlertEventTriggered {
		t.Fatalf("unexpected changes: %+v", changes)
	}
}

func TestReconcileStateResolvesInactiveAlerts(t *testing.T) {
	t.Parallel()

	observedAt := time.Date(2026, 7, 15, 12, 20, 0, 0, time.UTC)
	plan, changes, err := reconcileState(
		[]Rule{{ID: 7, Name: "error spike", CooldownSeconds: 600}},
		nil,
		[]Instance{{
			ID:           11,
			RuleID:       7,
			DedupeKey:    "service=checkout",
			Status:       AlertStatusActive,
			FirstFiredAt: observedAt.Add(-20 * time.Minute),
			LastFiredAt:  observedAt.Add(-20 * time.Minute),
		}},
		observedAt,
	)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(plan.upserts) != 1 || plan.upserts[0].instance.Status != AlertStatusResolved {
		t.Fatalf("expected resolved plan, got %+v", plan)
	}
	if len(changes) != 1 || changes[0].EventType != AlertEventResolved {
		t.Fatalf("unexpected changes: %+v", changes)
	}
}

func TestReconcileStateReopensResolvedInstance(t *testing.T) {
	t.Parallel()

	observedAt := time.Date(2026, 7, 15, 12, 20, 0, 0, time.UTC)
	plan, changes, err := reconcileState(
		[]Rule{{ID: 7, Name: "error spike", CooldownSeconds: 600}},
		[]Trigger{{RuleID: 7, RuleName: "error spike", GroupKey: "service=checkout", MatchCount: 3}},
		[]Instance{{
			ID:           11,
			RuleID:       7,
			DedupeKey:    "service=checkout",
			Status:       AlertStatusResolved,
			FirstFiredAt: observedAt.Add(-20 * time.Minute),
			LastFiredAt:  observedAt.Add(-20 * time.Minute),
			ResolvedAt:   sql.NullTime{Time: observedAt.Add(-10 * time.Minute), Valid: true},
		}},
		observedAt,
	)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(plan.upserts) != 1 || plan.upserts[0].instance.Status != AlertStatusActive {
		t.Fatalf("expected reopened plan, got %+v", plan)
	}
	if len(changes) != 1 || changes[0].EventType != AlertEventTriggered {
		t.Fatalf("unexpected changes: %+v", changes)
	}
}
