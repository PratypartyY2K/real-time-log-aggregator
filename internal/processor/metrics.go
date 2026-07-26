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

var (
	endToEndLatencyBuckets = []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900}
	httpLikeLatencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
)

type dlqMetricKey struct {
	reason  string
	outcome string
}

type clickHouseMetricKey struct {
	operation string
	result    string
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
	retryCount          map[string]uint64
	clickHouseCount     map[clickHouseMetricKey]uint64
	clickHouseSum       map[clickHouseMetricKey]float64
	clickHouseBuckets   map[clickHouseMetricKey][]uint64
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
		retryCount:          map[string]uint64{},
		clickHouseCount:     map[clickHouseMetricKey]uint64{},
		clickHouseSum:       map[clickHouseMetricKey]float64{},
		clickHouseBuckets:   map[clickHouseMetricKey][]uint64{},
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

func (m *Metrics) ObserveRetry(reason string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.retryCount[normalizedLabel(reason)]++
	m.mu.Unlock()
}

func (m *Metrics) ObserveClickHouseWrite(result string, duration time.Duration) {
	m.observeClickHouse("write", result, duration)
}

func (m *Metrics) observeClickHouse(operation, result string, duration time.Duration) {
	if m == nil {
		return
	}
	key := clickHouseMetricKey{operation: normalizedLabel(operation), result: normalizedLabel(result)}
	seconds := duration.Seconds()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.clickHouseCount[key]++
	m.clickHouseSum[key] += seconds
	buckets := m.clickHouseBuckets[key]
	if buckets == nil {
		buckets = make([]uint64, len(httpLikeLatencyBuckets))
	}
	for index, upperBound := range httpLikeLatencyBuckets {
		if seconds <= upperBound {
			buckets[index]++
		}
	}
	m.clickHouseBuckets[key] = buckets
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
	commonmetrics.WriteMetricHelp(body, "logagg_processor_retries_total", "Total processor message retries requested by reason.", "counter")
	commonmetrics.WriteMetricHelp(body, "logagg_clickhouse_write_duration_seconds", "ClickHouse write latency in seconds by result.", "histogram")
	commonmetrics.WriteMetricHelp(body, "logagg_clickhouse_write_errors_total", "Total ClickHouse write failures.", "counter")

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
		commonmetrics.WriteHistogram(body, "logagg_processor_end_to_end_latency_seconds", labels, endToEndLatencyBuckets, m.endToEndBuckets[result], m.endToEndSum[result], count)
	}
	for key, count := range m.dlqPublicationCount {
		labels := map[string]string{"service": m.service, "reason": key.reason, "outcome": key.outcome}
		commonmetrics.WriteMetricLine(body, "logagg_dlq_publications_total", labels, strconv.FormatUint(count, 10))
	}
	for reason, count := range m.retryCount {
		commonmetrics.WriteMetricLine(body, "logagg_processor_retries_total", map[string]string{"service": m.service, "reason": reason}, strconv.FormatUint(count, 10))
	}
	for key, count := range m.clickHouseCount {
		labels := map[string]string{"service": m.service, "operation": key.operation, "result": key.result}
		commonmetrics.WriteHistogram(body, "logagg_clickhouse_write_duration_seconds", labels, httpLikeLatencyBuckets, m.clickHouseBuckets[key], m.clickHouseSum[key], count)
		if key.result != resultSuccess {
			commonmetrics.WriteMetricLine(body, "logagg_clickhouse_write_errors_total", labels, strconv.FormatUint(count, 10))
		}
	}
}

func normalizedLabel(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return value
}
