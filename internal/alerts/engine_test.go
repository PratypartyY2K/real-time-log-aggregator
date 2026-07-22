package alerts

import (
	"testing"
	"time"
)

func TestEngineEvaluatesRateAcrossBatchesAndExpiresOldSamples(t *testing.T) {
	engine := NewEngine()
	rule := Rule{ID: 20, TenantID: 7, RuleType: "rate_threshold", WindowSeconds: 60, Threshold: "0.03"}
	start := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)

	triggers, err := engine.Evaluate(rule, []Record{{Timestamp: start, TenantID: 7}})
	if err != nil || len(triggers) != 0 {
		t.Fatalf("expected first batch below threshold, got triggers=%+v err=%v", triggers, err)
	}
	triggers, err = engine.Evaluate(rule, []Record{{Timestamp: start.Add(30 * time.Second), TenantID: 7}})
	if err != nil || len(triggers) != 1 || triggers[0].MatchCount != 2 {
		t.Fatalf("expected cross-batch trigger, got triggers=%+v err=%v", triggers, err)
	}
	triggers, err = engine.Evaluate(rule, []Record{{Timestamp: start.Add(2 * time.Minute), TenantID: 7}})
	if err != nil || len(triggers) != 0 {
		t.Fatalf("expected expired samples, got triggers=%+v err=%v", triggers, err)
	}
}

func TestEngineIsolatesTenantRuleWindows(t *testing.T) {
	engine := NewEngine()
	now := time.Now().UTC()
	rule := Rule{ID: 21, RuleType: "rate_threshold", WindowSeconds: 10, Threshold: "0.2"}
	_, _ = engine.Evaluate(rule, []Record{{Timestamp: now, TenantID: 1}})
	triggers, err := engine.Evaluate(rule, []Record{{Timestamp: now, TenantID: 2}})
	if err != nil {
		t.Fatalf("evaluate isolated window: %v", err)
	}
	if len(triggers) != 0 {
		t.Fatalf("expected tenant isolation, got %+v", triggers)
	}
}

func TestEngineDoesNotCountRedeliveredIngestBatchTwice(t *testing.T) {
	engine := NewEngine()
	now := time.Now().UTC()
	rule := Rule{ID: 22, TenantID: 7, RuleType: "rate_threshold", WindowSeconds: 10, Threshold: "0.2"}
	batch := []Record{{Timestamp: now, TenantID: 7, IngestID: "ingest-1"}}
	_, _ = engine.Evaluate(rule, batch)
	triggers, err := engine.Evaluate(rule, batch)
	if err != nil {
		t.Fatalf("evaluate redelivery: %v", err)
	}
	if len(triggers) != 0 {
		t.Fatalf("expected redelivery to remain below threshold, got %+v", triggers)
	}
}
