package alerts

import (
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
