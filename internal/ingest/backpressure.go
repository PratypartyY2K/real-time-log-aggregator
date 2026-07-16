package ingest

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/stream"
)

var ErrBackpressureRejected = errors.New("queue lag above high watermark")

type BackpressureController interface {
	Apply(context.Context) error
}

type QueueLagBackpressure struct {
	Strategy      string
	HighWatermark uint64
	Delay         time.Duration
	Monitor       stream.QueueStatsProvider
}

func (b QueueLagBackpressure) Apply(ctx context.Context) error {
	strategy := strings.ToLower(strings.TrimSpace(b.Strategy))
	if strategy == "" || strategy == "off" || b.Monitor == nil || b.HighWatermark == 0 {
		return nil
	}

	stats, err := b.Monitor.Stats(ctx)
	if err != nil {
		return nil
	}
	if stats.ConsumerPending < b.HighWatermark {
		return nil
	}

	switch strategy {
	case "reject":
		return ErrBackpressureRejected
	case "delay":
		delay := b.Delay
		if delay <= 0 {
			delay = 250 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	default:
		return nil
	}
}
