package alerts

import (
	"strconv"
	"strings"
	"sync"

	commonmetrics "github.com/PratypartyY2K/real-time-log-aggregator/internal/metrics"
)

type MetricsCollector struct {
	service string

	mu     sync.Mutex
	counts map[stateChangeMetricKey]uint64
}

type stateChangeMetricKey struct {
	EventType string
	Status    string
}

func NewMetricsCollector(service string) *MetricsCollector {
	return &MetricsCollector{
		service: service,
		counts:  map[stateChangeMetricKey]uint64{},
	}
}

func (m *MetricsCollector) ObserveStateChanges(changes []StateChange) {
	if m == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, change := range changes {
		key := stateChangeMetricKey{
			EventType: strings.TrimSpace(change.EventType),
			Status:    strings.TrimSpace(change.Status),
		}
		if key.EventType == "" {
			key.EventType = "unknown"
		}
		if key.Status == "" {
			key.Status = "unknown"
		}
		m.counts[key]++
	}
}

func (m *MetricsCollector) WritePrometheus(body *strings.Builder) {
	if m == nil {
		return
	}

	commonmetrics.WriteMetricHelp(body, "logagg_alert_state_changes_total", "Total alert state changes by event type and status.", "counter")

	m.mu.Lock()
	defer m.mu.Unlock()

	for key, count := range m.counts {
		commonmetrics.WriteMetricLine(body, "logagg_alert_state_changes_total", map[string]string{
			"service":    m.service,
			"event_type": key.EventType,
			"status":     key.Status,
		}, strconv.FormatUint(count, 10))
	}
}
