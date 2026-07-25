package processor

import (
	"strings"
	"testing"
	"time"
)

func TestMetricsExposeLegacyAndObservabilityMetrics(t *testing.T) {
	metrics := NewMetrics("processor")
	receivedAt := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)

	metrics.ObserveBatch(resultSuccess, 3, 125*time.Millisecond)
	metrics.ObserveEndToEnd(resultSuccess, receivedAt, receivedAt.Add(750*time.Millisecond))
	metrics.ObserveDLQ("retry_exhausted", "published")

	var body strings.Builder
	metrics.WritePrometheus(&body)
	output := body.String()

	expected := []struct {
		name   string
		value  string
		labels []string
	}{
		{"logagg_processor_batches_total", "1", []string{`result="success"`, `service="processor"`}},
		{"logagg_processor_logs_total", "3", []string{`result="success"`, `service="processor"`}},
		{"logagg_processor_batch_duration_seconds_sum", "0.125", []string{`result="success"`, `service="processor"`}},
		{"logagg_processor_end_to_end_latency_seconds_bucket", "1", []string{`le="1"`, `result="success"`, `service="processor"`}},
		{"logagg_processor_end_to_end_latency_seconds_count", "1", []string{`result="success"`, `service="processor"`}},
		{"logagg_dlq_publications_total", "1", []string{`outcome="published"`, `reason="retry_exhausted"`, `service="processor"`}},
	}
	for _, metric := range expected {
		if !containsMetric(output, metric.name, metric.value, metric.labels...) {
			t.Errorf("metrics output missing %s with labels %v and value %s:\n%s", metric.name, metric.labels, metric.value, output)
		}
	}
}

func TestMetricsIgnoreInvalidEndToEndTimestamps(t *testing.T) {
	metrics := NewMetrics("processor")
	now := time.Now()
	metrics.ObserveEndToEnd(resultSuccess, now, now.Add(-time.Second))

	var body strings.Builder
	metrics.WritePrometheus(&body)
	if strings.Contains(body.String(), "logagg_processor_end_to_end_latency_seconds_count{") {
		t.Fatalf("unexpected end-to-end observation:\n%s", body.String())
	}
}

func containsMetric(output, name, value string, labels ...string) bool {
	for _, line := range strings.Split(output, "\n") {
		if !strings.HasPrefix(line, name+"{") || !strings.HasSuffix(line, "} "+value) {
			continue
		}
		matches := true
		for _, label := range labels {
			if !strings.Contains(line, label) {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}
