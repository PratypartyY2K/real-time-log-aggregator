package stream

import (
	"context"
	"strings"
	"testing"
)

type stubQueueStatsProvider struct {
	stats QueueStats
	err   error
}

func (s stubQueueStatsProvider) Stats(_ context.Context) (QueueStats, error) {
	return s.stats, s.err
}

func TestQueueLagCollectorWritesPrometheus(t *testing.T) {
	collector := NewQueueLagCollector("processor", stubQueueStatsProvider{
		stats: QueueStats{
			StreamMessages:      12,
			StreamBytes:         4096,
			ConsumerPending:     7,
			ConsumerAckPending:  2,
			ConsumerWaiting:     1,
			ConsumerRedelivered: 3,
		},
	})

	var body strings.Builder
	collector.WritePrometheus(&body)
	output := body.String()

	for _, expected := range []string{
		`logagg_queue_monitor_up{service="processor"} 1`,
		`logagg_queue_stream_messages{service="processor"} 12`,
		`logagg_queue_consumer_pending{service="processor"} 7`,
		`logagg_queue_consumer_ack_pending{service="processor"} 2`,
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("expected metric %q in output %q", expected, output)
		}
	}
}
