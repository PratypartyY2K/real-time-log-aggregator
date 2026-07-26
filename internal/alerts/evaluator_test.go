package alerts

import (
	"math"
	"testing"
	"time"
)

func TestEvaluateCountThresholdGroupsMatches(t *testing.T) {
	t.Parallel()

	triggers, err := Evaluate(Rule{
		ID:          7,
		Name:        "error spike",
		RuleType:    "count_threshold",
		Severity:    "critical",
		FilterJSON:  []byte(`{"level":"error"}`),
		GroupByJSON: []byte(`["field.region"]`),
		Threshold:   "2",
	}, []Record{
		{Level: "error", Message: "db timeout", Fields: map[string]any{"region": "us-west-2"}},
		{Level: "error", Message: "db timeout", Fields: map[string]any{"region": "us-west-2"}},
		{Level: "error", Message: "db timeout", Fields: map[string]any{"region": "us-east-1"}},
		{Level: "info", Message: "ok", Fields: map[string]any{"region": "us-west-2"}},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(triggers) != 1 {
		t.Fatalf("expected one trigger, got %+v", triggers)
	}
	if triggers[0].GroupKey != "field.region=us-west-2" || triggers[0].MatchCount != 2 {
		t.Fatalf("unexpected trigger: %+v", triggers[0])
	}
}

func TestEvaluateRateThresholdUsesConfiguredWindowAndGrouping(t *testing.T) {
	t.Parallel()
	latest := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	triggers, err := Evaluate(Rule{ID: 10, Name: "error rate", RuleType: "rate_threshold", Severity: "high", FilterJSON: []byte(`{"level":"error"}`), GroupByJSON: []byte(`["service"]`), WindowSeconds: 60, Threshold: "0.03"}, []Record{
		{Timestamp: latest.Add(-61 * time.Second), Service: "checkout", Level: "error"},
		{Timestamp: latest.Add(-30 * time.Second), Service: "checkout", Level: "error"},
		{Timestamp: latest, Service: "checkout", Level: "error"},
		{Timestamp: latest, Service: "billing", Level: "info"},
	})
	if err != nil {
		t.Fatalf("evaluate rate rule: %v", err)
	}
	if len(triggers) != 1 {
		t.Fatalf("expected one trigger, got %+v", triggers)
	}
	trigger := triggers[0]
	if trigger.GroupKey != "service=checkout" || trigger.MatchCount != 2 || math.Abs(trigger.MetricValue-(2.0/60.0)) > 1e-9 || trigger.WindowSeconds != 60 {
		t.Fatalf("unexpected rate trigger: %+v", trigger)
	}
}

func TestEvaluatePercentileThresholdUsesFieldValues(t *testing.T) {
	t.Parallel()
	latest := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	triggers, err := Evaluate(Rule{ID: 11, Name: "latency p95", RuleType: "percentile_threshold", Severity: "critical", FilterJSON: []byte(`{"value_field":"field.duration_ms","percentile":95}`), GroupByJSON: []byte(`["service"]`), WindowSeconds: 300, Threshold: "380.5"}, []Record{
		{Timestamp: latest, Service: "checkout", Fields: map[string]any{"duration_ms": 100.0}},
		{Timestamp: latest, Service: "checkout", Fields: map[string]any{"duration_ms": "200"}},
		{Timestamp: latest, Service: "checkout", Fields: map[string]any{"duration_ms": 300}},
		{Timestamp: latest, Service: "checkout", Fields: map[string]any{"duration_ms": 400.0}},
		{Timestamp: latest, Service: "checkout", Fields: map[string]any{"duration_ms": "not-a-number"}},
	})
	if err != nil {
		t.Fatalf("evaluate percentile rule: %v", err)
	}
	if len(triggers) != 1 {
		t.Fatalf("expected one trigger, got %+v", triggers)
	}
	trigger := triggers[0]
	if trigger.MatchCount != 4 || math.Abs(trigger.MetricValue-385) > 1e-9 || trigger.Percentile != 95 || trigger.ValueField != "field.duration_ms" {
		t.Fatalf("unexpected percentile trigger: %+v", trigger)
	}
}

func TestEvaluatePercentileThresholdSupportsRawSize(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	triggers, err := Evaluate(Rule{RuleType: "percentile_based", FilterJSON: []byte(`{"value_field":"raw_size_bytes","percentile":50}`), WindowSeconds: 60, Threshold: "150"}, []Record{{Timestamp: now, RawSizeBytes: 100}, {Timestamp: now, RawSizeBytes: 200}, {Timestamp: now, RawSizeBytes: 300}})
	if err != nil {
		t.Fatalf("evaluate percentile alias: %v", err)
	}
	if len(triggers) != 1 || triggers[0].MetricValue != 200 {
		t.Fatalf("unexpected trigger: %+v", triggers)
	}
}

func TestEvaluateMetricRulesValidateConfiguration(t *testing.T) {
	t.Parallel()
	tests := []Rule{
		{RuleType: "rate_threshold", WindowSeconds: 0, Threshold: "1"},
		{RuleType: "percentile_threshold", WindowSeconds: 60, Threshold: "1", FilterJSON: []byte(`{"percentile":95}`)},
		{RuleType: "percentile_threshold", WindowSeconds: 60, Threshold: "1", FilterJSON: []byte(`{"value_field":"field.duration_ms","percentile":100}`)},
		{RuleType: "rate_threshold", WindowSeconds: 60, Threshold: "NaN"},
	}
	for _, rule := range tests {
		if _, err := Evaluate(rule, nil); err == nil {
			t.Fatalf("expected validation error for %+v", rule)
		}
	}
}

func TestEvaluatePatternMatchUsesMessageByDefault(t *testing.T) {
	t.Parallel()

	triggers, err := Evaluate(Rule{
		ID:         8,
		Name:       "panic detector",
		RuleType:   "pattern_match",
		Severity:   "high",
		FilterJSON: []byte(`{"pattern":"panic","level":"error"}`),
		Threshold:  "1",
	}, []Record{
		{Level: "error", Message: "panic in handler"},
		{Level: "error", Message: "timeout"},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(triggers) != 1 || triggers[0].MatchCount != 1 {
		t.Fatalf("unexpected triggers: %+v", triggers)
	}
}

func TestEvaluatePatternMatchSupportsFieldTarget(t *testing.T) {
	t.Parallel()

	triggers, err := Evaluate(Rule{
		ID:          9,
		Name:        "region match",
		RuleType:    "pattern_match",
		Severity:    "medium",
		FilterJSON:  []byte(`{"pattern":"west","target":"field.region"}`),
		GroupByJSON: []byte(`["service"]`),
		Threshold:   "2",
	}, []Record{
		{Service: "checkout", Fields: map[string]any{"region": "us-west-2"}},
		{Service: "checkout", Fields: map[string]any{"region": "us-west-1"}},
		{Service: "checkout", Fields: map[string]any{"region": "us-east-1"}},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(triggers) != 1 || triggers[0].GroupKey != "service=checkout" || triggers[0].MatchCount != 2 {
		t.Fatalf("unexpected triggers: %+v", triggers)
	}
}

func TestEvaluateRejectsUnsupportedRuleType(t *testing.T) {
	t.Parallel()

	_, err := Evaluate(Rule{RuleType: "ratio", Threshold: "1"}, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestEvaluateRejectsPatternMatchWithoutPattern(t *testing.T) {
	t.Parallel()

	_, err := Evaluate(Rule{RuleType: "pattern_match", FilterJSON: []byte(`{"level":"error"}`), Threshold: "1"}, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestEvaluateRejectsNonPositiveThreshold(t *testing.T) {
	t.Parallel()

	_, err := Evaluate(Rule{RuleType: "count_threshold", Threshold: "0"}, []Record{{Timestamp: time.Now()}})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestEvaluateAppliesExplicitAlertRuleModelFields(t *testing.T) {
	t.Parallel()

	triggers, err := Evaluate(Rule{
		ID:          12,
		Name:        "payment errors",
		RuleType:    "count_threshold",
		Severity:    "critical",
		Service:     "payment-api",
		LogLevel:    "ERROR",
		Fingerprint: "db-timeout",
		Threshold:   "2",
	}, []Record{
		{Service: "payment-api", Environment: "prod", Level: "error", Fingerprint: "db-timeout"},
		{Service: "payment-api", Environment: "prod", Level: "error", Fingerprint: "db-timeout"},
		{Service: "payment-api", Environment: "prod", Level: "info", Fingerprint: "db-timeout"},
		{Service: "checkout-api", Environment: "prod", Level: "error", Fingerprint: "db-timeout"},
		{Service: "payment-api", Environment: "prod", Level: "error", Fingerprint: "other"},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(triggers) != 1 || triggers[0].MatchCount != 2 {
		t.Fatalf("unexpected triggers: %+v", triggers)
	}
}
