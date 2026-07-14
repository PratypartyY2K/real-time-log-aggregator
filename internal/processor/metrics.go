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

type Metrics struct {
	service string

	mu          sync.Mutex
	batchCount  map[string]uint64
	logCount    map[string]uint64
	durationSum map[string]float64
}

func NewMetrics(service string) *Metrics {
	return &Metrics{
		service:     service,
		batchCount:  map[string]uint64{},
		logCount:    map[string]uint64{},
		durationSum: map[string]float64{},
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

func (m *Metrics) WritePrometheus(body *strings.Builder) {
	if m == nil {
		return
	}

	commonmetrics.WriteMetricHelp(body, "logagg_processor_batches_total", "Total processed log batches by result.", "counter")
	commonmetrics.WriteMetricHelp(body, "logagg_processor_logs_total", "Total processed log records by result.", "counter")
	commonmetrics.WriteMetricHelp(body, "logagg_processor_batch_duration_seconds_sum", "Cumulative processor batch duration in seconds.", "counter")
	commonmetrics.WriteMetricHelp(body, "logagg_processor_batch_duration_seconds_count", "Total processed batches recorded for duration aggregation.", "counter")

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
}
