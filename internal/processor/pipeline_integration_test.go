package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/alerts"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/ingest"
)

func TestIngestQueueProcessorFlow(t *testing.T) {
	ctx := context.Background()
	publisher := &integrationPublisher{}
	observer := &integrationObserver{}
	handler := ingest.NewHandler(ingest.Config{
		MaxLogEntries: 1000,
		Authenticator: integrationAuthenticator{
			authz: ingest.Authorization{
				Decision:  ingest.AuthorizationAllowed,
				APIKeyID:  10,
				TenantID:  42,
				ServiceID: 7,
			},
		},
		Observer:    observer,
		RateLimiter: integrationRateLimiter{},
		Publisher:   publisher,
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewBufferString(`{
		"schema_version":"logs.ingest.v1",
		"service":"checkout",
		"env":"prod",
		"source":"app",
		"logs":[
			{
				"timestamp":"2026-07-07T16:00:00Z",
				"level":"error",
				"message":"database timeout",
				"fields":{"host":"api-1","trace_id":"trace-123","region":"us-west-2"}
			},
			{
				"timestamp":"2026-07-07T16:00:01Z",
				"level":"error",
				"message":"database timeout",
				"fields":{"host":"api-1","trace_id":"trace-123","region":"us-west-2"}
			}
		]
	}`))
	req.Header.Set("X-API-Key", "local-dev-key")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected ingest request to be accepted, got %d: %s", rec.Code, rec.Body.String())
	}
	if publisher.event == nil {
		t.Fatal("expected ingest request to publish an event")
	}
	if len(observer.observations) != 1 || observer.observations[0].Outcome != ingest.AuthOutcomeAuthorized {
		t.Fatalf("expected one authorized observation, got %#v", observer.observations)
	}

	queuedEvent := queueRoundTrip(t, *publisher.event)

	logger := &stubLogger{}
	writer := &integrationLogWriter{}
	ruleStore := &stubAlertRuleStore{
		rules: []alerts.Rule{
			{
				ID:         9,
				Name:       "error spike",
				RuleType:   "count_threshold",
				Severity:   "critical",
				FilterJSON: []byte(`{"level":"error"}`),
				Threshold:  "2",
			},
		},
	}

	if err := handleBatch(ctx, logger, writer, ruleStore, &stubNotificationDispatcher{}, nil, queuedEvent); err != nil {
		t.Fatalf("expected processor to accept queued event, got %v", err)
	}
	if writer.writeCalls != 1 {
		t.Fatalf("expected one ClickHouse write, got %d", writer.writeCalls)
	}
	if writer.alreadyProcessedChecks != 1 {
		t.Fatalf("expected one idempotency check, got %d", writer.alreadyProcessedChecks)
	}
	if len(writer.lastBatch) != 2 {
		t.Fatalf("expected two normalized records, got %d", len(writer.lastBatch))
	}
	if writer.lastBatch[0].IngestID != queuedEvent.RequestID {
		t.Fatalf("expected normalized ingest id %q, got %q", queuedEvent.RequestID, writer.lastBatch[0].IngestID)
	}
	if writer.lastBatch[0].TenantID != 42 {
		t.Fatalf("expected normalized tenant id 42, got %d", writer.lastBatch[0].TenantID)
	}
	if writer.lastBatch[0].Service != "checkout" || writer.lastBatch[0].Environment != "prod" || writer.lastBatch[0].Source != "app" {
		t.Fatalf("expected normalized routing tags, got %+v", writer.lastBatch[0])
	}
	if ruleStore.syncCalls != 1 || len(ruleStore.syncedTriggers) != 1 {
		t.Fatalf("expected one alert trigger sync, got %+v", ruleStore)
	}
	if ruleStore.dispatchCalls != 1 {
		t.Fatalf("expected one notification dispatch pass, got %d", ruleStore.dispatchCalls)
	}

	writer.markProcessed(queuedEvent.RequestID)
	if err := handleBatch(ctx, logger, writer, ruleStore, &stubNotificationDispatcher{}, nil, queuedEvent); err != nil {
		t.Fatalf("expected duplicate queued event to be skipped, got %v", err)
	}
	if writer.writeCalls != 1 {
		t.Fatalf("expected duplicate queued event to avoid rewrites, got %d writes", writer.writeCalls)
	}
	if ruleStore.syncCalls != 1 {
		t.Fatalf("expected duplicate queued event to avoid alert state sync, got %d syncs", ruleStore.syncCalls)
	}
	if ruleStore.dispatchCalls != 1 {
		t.Fatalf("expected duplicate queued event to avoid notification dispatch, got %d dispatches", ruleStore.dispatchCalls)
	}
}

func queueRoundTrip(t *testing.T, event contracts.LogsRawEvent) contracts.LogsRawEvent {
	t.Helper()

	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal queued event: %v", err)
	}

	var queued contracts.LogsRawEvent
	if err := json.Unmarshal(payload, &queued); err != nil {
		t.Fatalf("unmarshal queued event: %v", err)
	}

	return queued
}

type integrationAuthenticator struct {
	authz ingest.Authorization
	err   error
}

func (a integrationAuthenticator) Authorize(context.Context, string, ingest.BatchRequest) (ingest.Authorization, error) {
	return a.authz, a.err
}

type integrationRateLimiter struct{}

func (integrationRateLimiter) Allow(int64, int) bool {
	return true
}

type integrationPublisher struct {
	event *contracts.LogsRawEvent
	err   error
}

func (p *integrationPublisher) Publish(_ context.Context, event contracts.LogsRawEvent) error {
	if p.err != nil {
		return p.err
	}
	copied := event
	p.event = &copied
	return nil
}

type integrationObserver struct {
	observations []ingest.AuthObservation
}

func (o *integrationObserver) ObserveAuth(_ context.Context, obs ingest.AuthObservation) {
	o.observations = append(o.observations, obs)
}

type integrationLogWriter struct {
	lastBatch              []NormalizedLogRecord
	writeCalls             int
	alreadyProcessedChecks int
	processedIDs           map[string]struct{}
}

func (w *integrationLogWriter) AlreadyProcessed(_ context.Context, _ uint64, ingestID string) (bool, error) {
	w.alreadyProcessedChecks++
	_, ok := w.processedIDs[ingestID]
	return ok, nil
}

func (w *integrationLogWriter) WriteBatch(_ context.Context, batch []NormalizedLogRecord) error {
	w.writeCalls++
	w.lastBatch = append([]NormalizedLogRecord(nil), batch...)
	for _, record := range batch {
		w.markProcessed(record.IngestID)
	}
	return nil
}

func (w *integrationLogWriter) markProcessed(ingestID string) {
	if w.processedIDs == nil {
		w.processedIDs = map[string]struct{}{}
	}
	w.processedIDs[ingestID] = struct{}{}
}
