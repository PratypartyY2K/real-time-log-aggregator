package alerts

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"
)

type DeliveryBatchStore interface {
	DispatchNotificationBatch(context.Context, NotificationDispatcher, DeliveryPolicy, time.Time) error
}

type DeliveryWorkerLogger interface {
	Info(string, ...any)
	Error(string, ...any)
}

func NewDeliveryWorkerID(service string) string {
	service = strings.TrimSpace(service)
	if service == "" {
		service = "notification-worker"
	}
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s:%s:%d", service, hostname, os.Getpid())
}

func RunDeliveryWorker(ctx context.Context, logger DeliveryWorkerLogger, store DeliveryBatchStore, dispatcher NotificationDispatcher, policy DeliveryPolicy, pollInterval time.Duration) {
	if store == nil || dispatcher == nil {
		return
	}
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	policy = policy.normalized()

	run := func() {
		if err := store.DispatchNotificationBatch(ctx, dispatcher, policy, time.Now().UTC()); err != nil {
			if ctx.Err() == nil && logger != nil {
				logger.Error("notification delivery batch failed", "worker_id", policy.WorkerID, "error", err)
			}
		}
	}

	if logger != nil {
		logger.Info("notification delivery worker started", "worker_id", policy.WorkerID, "poll_interval", pollInterval, "batch_size", policy.BatchSize)
	}
	run()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}
