package ingest

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
)

func TestHandlerRejectsMissingAPIKey(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()

	NewHandler(Config{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandlerAcceptsValidBatch(t *testing.T) {
	publisher := &stubPublisher{}
	body := `{
		"service":"checkout",
		"env":"prod",
		"source":"app",
		"logs":[
			{
				"timestamp":"2026-07-07T16:00:00Z",
				"level":"error",
				"message":"database timeout"
			}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewBufferString(body))
	req.Header.Set(apiKeyHeader, "local-dev-key")
	rec := httptest.NewRecorder()

	NewHandler(Config{
		Authenticator: stubAuthenticator{ok: true},
		Publisher:     publisher,
	}).ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}
	if publisher.batch == nil {
		t.Fatal("expected batch to be published")
	}
	if publisher.batch.Service != "checkout" {
		t.Fatalf("expected published service checkout, got %q", publisher.batch.Service)
	}
	if publisher.batch.RequestID == "" {
		t.Fatal("expected request id to be set")
	}
	if publisher.batch.SchemaVersion != contracts.LogsRawSchemaVersion {
		t.Fatalf("expected schema version %q, got %q", contracts.LogsRawSchemaVersion, publisher.batch.SchemaVersion)
	}
}

func TestValidateRejectsBadTimestamp(t *testing.T) {
	req := BatchRequest{
		Service: "checkout",
		Env:     "prod",
		Logs: []LogRecord{
			{
				Timestamp: "07-07-2026",
				Level:     "info",
				Message:   "booting",
			},
		},
	}

	if err := req.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestHandlerReturnsServiceUnavailableWhenPublishFails(t *testing.T) {
	body := `{
		"service":"checkout",
		"env":"prod",
		"source":"app",
		"logs":[
			{
				"timestamp":"2026-07-07T16:00:00Z",
				"level":"error",
				"message":"database timeout"
			}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewBufferString(body))
	req.Header.Set(apiKeyHeader, "local-dev-key")
	rec := httptest.NewRecorder()

	NewHandler(Config{
		Authenticator: stubAuthenticator{ok: true},
		Publisher:     &stubPublisher{err: errors.New("nats unavailable")},
	}).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestHandlerRejectsInvalidAPIKey(t *testing.T) {
	body := `{"service":"checkout","env":"prod","logs":[{"timestamp":"2026-07-07T16:00:00Z","level":"info","message":"booting"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewBufferString(body))
	req.Header.Set(apiKeyHeader, "bad-key")
	rec := httptest.NewRecorder()

	NewHandler(Config{
		Authenticator: stubAuthenticator{ok: false},
		Publisher:     &stubPublisher{},
	}).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestHandlerReturnsServiceUnavailableWhenAuthFails(t *testing.T) {
	body := `{"service":"checkout","env":"prod","logs":[{"timestamp":"2026-07-07T16:00:00Z","level":"info","message":"booting"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewBufferString(body))
	req.Header.Set(apiKeyHeader, "local-dev-key")
	rec := httptest.NewRecorder()

	NewHandler(Config{
		Authenticator: stubAuthenticator{err: errors.New("postgres unavailable")},
		Publisher:     &stubPublisher{},
	}).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", rec.Code, rec.Body.String())
	}
}

type stubPublisher struct {
	err   error
	batch *contracts.LogsRawEvent
}

type stubAuthenticator struct {
	ok  bool
	err error
}

func (s stubAuthenticator) Authenticate(_ context.Context, _ string) (bool, error) {
	return s.ok, s.err
}

func (s *stubPublisher) Publish(_ context.Context, batch contracts.LogsRawEvent) error {
	if s.err != nil {
		return s.err
	}

	copy := batch
	s.batch = &copy
	return nil
}
