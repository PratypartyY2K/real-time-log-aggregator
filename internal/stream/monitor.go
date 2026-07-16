package stream

import (
	"context"
	"fmt"
	"strings"
	"time"

	commonmetrics "github.com/PratypartyY2K/real-time-log-aggregator/internal/metrics"
	"github.com/nats-io/nats.go"
)

type QueueStats struct {
	StreamMessages      uint64
	StreamBytes         uint64
	ConsumerPending     uint64
	ConsumerAckPending  uint64
	ConsumerWaiting     int
	ConsumerRedelivered uint64
}

type QueueStatsProvider interface {
	Stats(context.Context) (QueueStats, error)
}

type QueueMonitor struct {
	url        string
	streamName string
	durable    string
	timeout    time.Duration
}

type QueueLagCollector struct {
	service  string
	provider QueueStatsProvider
}

func NewQueueMonitor(url, streamName, durable string) *QueueMonitor {
	return &QueueMonitor{
		url:        strings.TrimSpace(url),
		streamName: strings.TrimSpace(streamName),
		durable:    strings.TrimSpace(durable),
		timeout:    2 * time.Second,
	}
}

func NewQueueLagCollector(service string, provider QueueStatsProvider) *QueueLagCollector {
	return &QueueLagCollector{
		service:  strings.TrimSpace(service),
		provider: provider,
	}
}

func (m *QueueMonitor) Stats(ctx context.Context) (QueueStats, error) {
	if m == nil {
		return QueueStats{}, fmt.Errorf("queue monitor is not configured")
	}
	nc, err := nats.Connect(m.url, nats.Timeout(m.timeout))
	if err != nil {
		return QueueStats{}, fmt.Errorf("connect to nats: %w", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		return QueueStats{}, fmt.Errorf("create jetstream context: %w", err)
	}

	streamInfo, err := js.StreamInfo(m.streamName, nats.Context(ctx))
	if err != nil {
		return QueueStats{}, fmt.Errorf("lookup stream %q: %w", m.streamName, err)
	}
	consumerInfo, err := js.ConsumerInfo(m.streamName, m.durable, nats.Context(ctx))
	if err != nil {
		return QueueStats{}, fmt.Errorf("lookup consumer %q on stream %q: %w", m.durable, m.streamName, err)
	}

	return QueueStats{
		StreamMessages:      streamInfo.State.Msgs,
		StreamBytes:         streamInfo.State.Bytes,
		ConsumerPending:     consumerInfo.NumPending,
		ConsumerAckPending:  uint64(consumerInfo.NumAckPending),
		ConsumerWaiting:     consumerInfo.NumWaiting,
		ConsumerRedelivered: uint64(consumerInfo.NumRedelivered),
	}, nil
}

func (c *QueueLagCollector) WritePrometheus(body *strings.Builder) {
	if c == nil || c.provider == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stats, err := c.provider.Stats(ctx)
	if err != nil {
		commonmetrics.WriteMetricHelp(body, "logagg_queue_monitor_up", "Whether queue lag monitoring succeeded on the last scrape.", "gauge")
		commonmetrics.WriteMetricLine(body, "logagg_queue_monitor_up", map[string]string{"service": c.service}, "0")
		return
	}

	commonmetrics.WriteMetricHelp(body, "logagg_queue_monitor_up", "Whether queue lag monitoring succeeded on the last scrape.", "gauge")
	commonmetrics.WriteMetricLine(body, "logagg_queue_monitor_up", map[string]string{"service": c.service}, "1")

	commonmetrics.WriteMetricHelp(body, "logagg_queue_stream_messages", "Current number of messages stored in the JetStream stream.", "gauge")
	commonmetrics.WriteMetricHelp(body, "logagg_queue_stream_bytes", "Current number of bytes stored in the JetStream stream.", "gauge")
	commonmetrics.WriteMetricHelp(body, "logagg_queue_consumer_pending", "Current number of messages pending for the consumer.", "gauge")
	commonmetrics.WriteMetricHelp(body, "logagg_queue_consumer_ack_pending", "Current number of in-flight messages awaiting ack.", "gauge")
	commonmetrics.WriteMetricHelp(body, "logagg_queue_consumer_waiting", "Current number of waiting pull requests on the consumer.", "gauge")
	commonmetrics.WriteMetricHelp(body, "logagg_queue_consumer_redelivered", "Current number of redelivered messages reported by the consumer.", "gauge")

	labels := map[string]string{"service": c.service}
	commonmetrics.WriteMetricLine(body, "logagg_queue_stream_messages", labels, fmt.Sprintf("%d", stats.StreamMessages))
	commonmetrics.WriteMetricLine(body, "logagg_queue_stream_bytes", labels, fmt.Sprintf("%d", stats.StreamBytes))
	commonmetrics.WriteMetricLine(body, "logagg_queue_consumer_pending", labels, fmt.Sprintf("%d", stats.ConsumerPending))
	commonmetrics.WriteMetricLine(body, "logagg_queue_consumer_ack_pending", labels, fmt.Sprintf("%d", stats.ConsumerAckPending))
	commonmetrics.WriteMetricLine(body, "logagg_queue_consumer_waiting", labels, fmt.Sprintf("%d", stats.ConsumerWaiting))
	commonmetrics.WriteMetricLine(body, "logagg_queue_consumer_redelivered", labels, fmt.Sprintf("%d", stats.ConsumerRedelivered))
}
