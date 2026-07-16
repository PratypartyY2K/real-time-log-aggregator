package ingest

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/stream"
)

type stubQueueStatsProvider struct {
	stats stream.QueueStats
	err   error
}

func (s stubQueueStatsProvider) Stats(_ context.Context) (stream.QueueStats, error) {
	return s.stats, s.err
}

func TestQueueLagBackpressureRejectsWhenLagExceedsThreshold(t *testing.T) {
	controller := QueueLagBackpressure{
		Strategy:      "reject",
		HighWatermark: 10,
		Monitor: stubQueueStatsProvider{
			stats: stream.QueueStats{ConsumerPending: 11},
		},
	}

	err := controller.Apply(context.Background())
	if err == nil || err != ErrBackpressureRejected {
		t.Fatalf("expected ErrBackpressureRejected, got %v", err)
	}
}

func TestQueueLagBackpressureDelaysWhenLagExceedsThreshold(t *testing.T) {
	controller := QueueLagBackpressure{
		Strategy:      "delay",
		HighWatermark: 10,
		Delay:         20 * time.Millisecond,
		Monitor: stubQueueStatsProvider{
			stats: stream.QueueStats{ConsumerPending: 11},
		},
	}

	start := time.Now()
	if err := controller.Apply(context.Background()); err != nil {
		t.Fatalf("expected delay strategy to allow request, got %v", err)
	}
	if time.Since(start) < 15*time.Millisecond {
		t.Fatalf("expected noticeable delay, got %s", time.Since(start))
	}
}

func TestHandlerRejectsWhenBackpressureControllerRejects(t *testing.T) {
	publisher := &stubPublisher{}
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewBufferString(`{
		"service":"checkout",
		"env":"prod",
		"source":"app",
		"logs":[{"timestamp":"2026-07-07T16:00:00Z","level":"error","message":"database timeout"}]
	}`))
	req.Header.Set(apiKeyHeader, "local-dev-key")
	rec := httptest.NewRecorder()

	NewHandler(Config{
		MaxLogEntries: 1000,
		Authenticator: stubAuthenticator{authz: Authorization{Decision: AuthorizationAllowed, TenantID: 1, ServiceID: 2}},
		RateLimiter:   allowAllRateLimiter{},
		Backpressure: QueueLagBackpressure{
			Strategy:      "reject",
			HighWatermark: 10,
			Monitor: stubQueueStatsProvider{
				stats: stream.QueueStats{ConsumerPending: 11},
			},
		},
		Publisher: publisher,
	}).ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d: %s", rec.Code, rec.Body.String())
	}
	if publisher.batch != nil {
		t.Fatal("expected rejected request to avoid publish")
	}
}
