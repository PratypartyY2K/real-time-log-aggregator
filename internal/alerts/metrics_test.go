package alerts

import (
	"strings"
	"testing"
)

func TestMetricsCollectorWritesStateChangeCounters(t *testing.T) {
	t.Parallel()

	collector := NewMetricsCollector("processor")
	collector.ObserveStateChanges([]StateChange{
		{EventType: AlertEventTriggered, Status: AlertStatusActive},
		{EventType: AlertEventTriggered, Status: AlertStatusActive},
		{EventType: AlertEventResolved, Status: AlertStatusResolved},
	})

	var body strings.Builder
	collector.WritePrometheus(&body)
	output := body.String()

	if !strings.Contains(output, "logagg_alert_state_changes_total") {
		t.Fatalf("expected alert state metrics in output, got %s", output)
	}
	if !strings.Contains(output, `event_type="triggered"`) || !strings.Contains(output, `status="firing"`) {
		t.Fatalf("expected triggered firing labels, got %s", output)
	}
	if !strings.Contains(output, `event_type="resolved"`) || !strings.Contains(output, `status="resolved"`) {
		t.Fatalf("expected resolved labels, got %s", output)
	}
}
