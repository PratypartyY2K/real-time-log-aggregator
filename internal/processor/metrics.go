package processor

import (
	"strconv"
	"strings"
	"sync"
	"time"

	commonmetrics "github.com/PratypartyY2K/real-time-log-aggregator/internal/metrics"
)

const (
	resultSuccess      = "success"
	resultInvalidBatch = "invalid_batch"
	resultRetryable    = "retryable_error"
)

var endToEndLatencyBuckets = []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900}

type dlqMetricKey struct {
	reason  string
	outcome string
}

type Metrics struct {
	service string

	mu                  sync.Mutex
	batchCount          map[string]uint64
	logCount            map[string]uint64
	durationSum         map[string]float64
	endToEndCount       map[string]uint64
	endToEndSum         map[string]float64
	endToEndBuckets     map[string][]uint64
	dlqPublicationCount map[dlqMetricKey]uint64
}

func NewMetrics(service string) *Metrics {
	return &Metrics{
		service:             service,
		batchCount:          map[string]uint64{},
		logCount:            map[string]uint64{},
		durationSum:         map[string]float64{},
		endToEndCount:       map[string]uint64{},
		endToEndSum:         map[string]float64{},
		endToEndBuckets:     map[string][]uint64{},
		dlqPublicationCount: map[dlqMetricKey]uint64{},
	}
}

func (m *Metrics) ObserveBatch(result string, logCount int, duration time.Duration) {
	if m == nil {
		return
	}

	result = strings.TrimSpace(result)
	if result == "" {
		result = "unknown"
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.batchCount[result]++
	m.logCount[result] += uint64(max(logCount, 0))
	m.durationSum[result] += duration.Seconds()
}

func (m *Metrics) ObserveEndToEnd(result string, receivedAt, completedAt time.Time) {
	if m == nil || receivedAt.IsZero() || completedAt.Before(receivedAt) {
		return
	}
	result = normalizedLabel(result)
	duration := completedAt.Sub(receivedAt).Seconds()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.endToEndCount[result]++
	m.endToEndSum[result] += duration
	buckets := m.endToEndBuckets[result]
	if buckets == nil {
		buckets = make([]uint64, len(endToEndLatencyBuckets))
	}
	for index, upperBound := range endToEndLatencyBuckets {
		if duration <= upperBound {
			buckets[index]++
		}
	}
	m.endToEndBuckets[result] = buckets
}

func (m *Metrics) ObserveDLQ(reason, outcome string) {
	if m == nil {
		return
	}
	key := dlqMetricKey{reason: normalizedLabel(reason), outcome: normalizedLabel(outcome)}
	m.mu.Lock()
	m.dlqPublicationCount[key]++
	m.mu.Unlock()
}

func (m *Metrics) WritePrometheus(body *strings.Builder) {
	if m == nil {
		return
	}

	commonmetrics.WriteMetricHelp(body, "logagg_processor_batches_total", "Total processed log batches by result.", "counter")
	commonmetrics.WriteMetricHelp(body, "logagg_processor_logs_total", "Total processed log records by result.", "counter")
	commonmetrics.WriteMetricHelp(body, "logagg_processor_batch_duration_seconds_sum", "Cumulative processor batch duration in seconds.", "counter")
	commonmetrics.WriteMetricHelp(body, "logagg_processor_batch_duration_seconds_count", "Total processed batches recorded for duration aggregation.", "counter")
	commonmetrics.WriteMetricHelp(body, "logagg_processor_end_to_end_latency_seconds", "End-to-end latency from ingest receipt through processor completion.", "histogram")
	commonmetrics.WriteMetricHelp(body, "logagg_dlq_publications_total", "Total dead-letter queue publication attempts by reason and outcome.", "counter")

	m.mu.Lock()
	defer m.mu.Unlock()

	for result, count := range m.batchCount {
		labels := map[string]string{
			"service": m.service,
			"result":  result,
		}
		commonmetrics.WriteMetricLine(body, "logagg_processor_batches_total", labels, strconv.FormatUint(count, 10))
		commonmetrics.WriteMetricLine(body, "logagg_processor_logs_total", labels, strconv.FormatUint(m.logCount[result], 10))
		commonmetrics.WriteMetricLine(body, "logagg_processor_batch_duration_seconds_sum", labels, commonmetrics.FormatFloat(m.durationSum[result]))
		commonmetrics.WriteMetricLine(body, "logagg_processor_batch_duration_seconds_count", labels, strconv.FormatUint(count, 10))
	}
	for result, count := range m.endToEndCount {
		labels := map[string]string{"service": m.service, "result": result}
		for index, upperBound := range endToEndLatencyBuckets {
			bucketLabels := cloneMetricLabels(labels)
			bucketLabels["le"] = commonmetrics.FormatFloat(upperBound)
			commonmetrics.WriteMetricLine(body, "logagg_processor_end_to_end_latency_seconds_bucket", bucketLabels, strconv.FormatUint(m.endToEndBuckets[result][index], 10))
		}
		infiniteLabels := cloneMetricLabels(labels)
		infiniteLabels["le"] = "+Inf"
		commonmetrics.WriteMetricLine(body, "logagg_processor_end_to_end_latency_seconds_bucket", infiniteLabels, strconv.FormatUint(count, 10))
		commonmetrics.WriteMetricLine(body, "logagg_processor_end_to_end_latency_seconds_sum", labels, commonmetrics.FormatFloat(m.endToEndSum[result]))
		commonmetrics.WriteMetricLine(body, "logagg_processor_end_to_end_latency_seconds_count", labels, strconv.FormatUint(count, 10))
	}
	for key, count := range m.dlqPublicationCount {
		labels := map[string]string{"service": m.service, "reason": key.reason, "outcome": key.outcome}
		commonmetrics.WriteMetricLine(body, "logagg_dlq_publications_total", labels, strconv.FormatUint(count, 10))
	}
}

func normalizedLabel(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return value
}

func cloneMetricLabels(labels map[string]string) map[string]string {
	cloned := make(map[string]string, len(labels)+1)
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}
