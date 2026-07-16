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
	observer := &stubObserver{}

	NewHandler(Config{Observer: observer}).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
	if len(observer.observations) != 1 || observer.observations[0].Outcome != AuthOutcomeMissingAPIKey {
		t.Fatalf("expected missing_api_key observation, got %#v", observer.observations)
	}
}

func TestHandlerAcceptsValidBatch(t *testing.T) {
	publisher := &stubPublisher{}
	observer := &stubObserver{}
	body := `{
		"schema_version":"logs.ingest.v1",
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
		MaxLogEntries: 1000,
		Authenticator: stubAuthenticator{authz: Authorization{Decision: AuthorizationAllowed, TenantID: 1, ServiceID: 2}},
		Observer:      observer,
		RateLimiter:   allowAllRateLimiter{},
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
	if publisher.batch.Fingerprint == "" {
		t.Fatal("expected fingerprint to be set")
	}
	if publisher.batch.RequestID != publisher.batch.Fingerprint {
		t.Fatalf("expected request id and fingerprint to match, got %q and %q", publisher.batch.RequestID, publisher.batch.Fingerprint)
	}
	if publisher.batch.SchemaVersion != contracts.LogsRawSchemaVersion {
		t.Fatalf("expected schema version %q, got %q", contracts.LogsRawSchemaVersion, publisher.batch.SchemaVersion)
	}
	if publisher.batch.TenantID != 1 {
		t.Fatalf("expected published tenant id 1, got %d", publisher.batch.TenantID)
	}
	if publisher.batch.Env != "prod" || publisher.batch.Source != "app" {
		t.Fatalf("expected canonical env/source, got env=%q source=%q", publisher.batch.Env, publisher.batch.Source)
	}
	if len(observer.observations) != 1 || observer.observations[0].Outcome != AuthOutcomeAuthorized {
		t.Fatalf("expected authorized observation, got %#v", observer.observations)
	}
}

func TestBatchFingerprintIsDeterministic(t *testing.T) {
	first := BatchRequest{
		Service: " Checkout ",
		Env:     " PROD ",
		Source:  " app ",
		Logs: []LogRecord{
			{
				Timestamp: "2026-07-07T16:00:00Z",
				Level:     " ERROR ",
				Message:   " database timeout ",
				Fields: map[string]any{
					"b": "two",
					"a": "one",
				},
			},
		},
	}
	second := BatchRequest{
		Service: "checkout",
		Env:     "prod",
		Source:  "app",
		Logs: []LogRecord{
			{
				Timestamp: "2026-07-07T16:00:00Z",
				Level:     "error",
				Message:   "database timeout",
				Fields: map[string]any{
					"a": "one",
					"b": "two",
				},
			},
		},
	}

	firstFingerprint := batchFingerprint(7, first)
	secondFingerprint := batchFingerprint(7, second)
	if firstFingerprint != secondFingerprint {
		t.Fatalf("expected deterministic fingerprint, got %q and %q", firstFingerprint, secondFingerprint)
	}
}

func TestValidateRejectsBadTimestamp(t *testing.T) {
	req := BatchRequest{
		SchemaVersion: IngestSchemaVersion,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []LogRecord{
			{
				Timestamp: "07-07-2026",
				Level:     "info",
				Message:   "booting",
			},
		},
	}

	if _, err := req.NormalizeAndValidate(1000); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestHandlerReturnsServiceUnavailableWhenPublishFails(t *testing.T) {
	body := `{
		"schema_version":"logs.ingest.v1",
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
	observer := &stubObserver{}

	NewHandler(Config{
		MaxLogEntries: 1000,
		Authenticator: stubAuthenticator{authz: Authorization{Decision: AuthorizationAllowed, TenantID: 1, ServiceID: 2}},
		Observer:      observer,
		RateLimiter:   allowAllRateLimiter{},
		Publisher:     &stubPublisher{err: errors.New("nats unavailable")},
	}).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", rec.Code, rec.Body.String())
	}
	if len(observer.observations) != 1 || observer.observations[0].Outcome != AuthOutcomeAuthorized {
		t.Fatalf("expected authorized observation, got %#v", observer.observations)
	}
}

func TestHandlerRejectsInvalidAPIKey(t *testing.T) {
	body := `{"schema_version":"logs.ingest.v1","service":"checkout","env":"prod","source":"app","logs":[{"timestamp":"2026-07-07T16:00:00Z","level":"info","message":"booting"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewBufferString(body))
	req.Header.Set(apiKeyHeader, "bad-key")
	rec := httptest.NewRecorder()
	observer := &stubObserver{}

	NewHandler(Config{
		MaxLogEntries: 1000,
		Authenticator: stubAuthenticator{authz: Authorization{Decision: AuthorizationInvalid}},
		Observer:      observer,
		RateLimiter:   allowAllRateLimiter{},
		Publisher:     &stubPublisher{},
	}).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", rec.Code, rec.Body.String())
	}
	if len(observer.observations) != 1 || observer.observations[0].Outcome != AuthOutcomeInvalidAPIKey {
		t.Fatalf("expected invalid_api_key observation, got %#v", observer.observations)
	}
}

func TestHandlerReturnsServiceUnavailableWhenAuthFails(t *testing.T) {
	body := `{"schema_version":"logs.ingest.v1","service":"checkout","env":"prod","source":"app","logs":[{"timestamp":"2026-07-07T16:00:00Z","level":"info","message":"booting"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewBufferString(body))
	req.Header.Set(apiKeyHeader, "local-dev-key")
	rec := httptest.NewRecorder()
	observer := &stubObserver{}

	NewHandler(Config{
		MaxLogEntries: 1000,
		Authenticator: stubAuthenticator{err: errors.New("postgres unavailable")},
		Observer:      observer,
		RateLimiter:   allowAllRateLimiter{},
		Publisher:     &stubPublisher{},
	}).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", rec.Code, rec.Body.String())
	}
	if len(observer.observations) != 1 || observer.observations[0].Outcome != AuthOutcomeBackendError {
		t.Fatalf("expected backend_error observation, got %#v", observer.observations)
	}
}

func TestHandlerRejectsUnauthorizedServiceScope(t *testing.T) {
	body := `{"schema_version":"logs.ingest.v1","service":"checkout","env":"prod","source":"app","logs":[{"timestamp":"2026-07-07T16:00:00Z","level":"info","message":"booting"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewBufferString(body))
	req.Header.Set(apiKeyHeader, "scoped-key")
	rec := httptest.NewRecorder()
	observer := &stubObserver{}

	NewHandler(Config{
		MaxLogEntries: 1000,
		Authenticator: stubAuthenticator{authz: Authorization{Decision: AuthorizationForbidden, TenantID: 1}},
		Observer:      observer,
		RateLimiter:   allowAllRateLimiter{},
		Publisher:     &stubPublisher{},
	}).ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", rec.Code, rec.Body.String())
	}
	if len(observer.observations) != 1 || observer.observations[0].Outcome != AuthOutcomeForbiddenScope {
		t.Fatalf("expected forbidden_scope observation, got %#v", observer.observations)
	}
}

func TestHandlerRejectsRateLimitedAPIKey(t *testing.T) {
	body := `{"schema_version":"logs.ingest.v1","service":"checkout","env":"prod","source":"app","logs":[{"timestamp":"2026-07-07T16:00:00Z","level":"info","message":"booting"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewBufferString(body))
	req.Header.Set(apiKeyHeader, "local-dev-key")
	rec := httptest.NewRecorder()
	observer := &stubObserver{}

	NewHandler(Config{
		MaxLogEntries: 1000,
		Authenticator: stubAuthenticator{authz: Authorization{Decision: AuthorizationAllowed, APIKeyID: 10, TenantID: 1, ServiceID: 2, RateLimitPerSec: 1}},
		Observer:      observer,
		RateLimiter:   denyAllRateLimiter{},
		Publisher:     &stubPublisher{},
	}).ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d: %s", rec.Code, rec.Body.String())
	}
	if len(observer.observations) != 2 || observer.observations[1].Outcome != AuthOutcomeRateLimited {
		t.Fatalf("expected rate_limited observation after authorize, got %#v", observer.observations)
	}
}

func TestHandlerRejectsTooManyLogsForConfiguredLimit(t *testing.T) {
	body := `{"schema_version":"logs.ingest.v1","service":"checkout","env":"prod","source":"app","logs":[{"timestamp":"2026-07-07T16:00:00Z","level":"info","message":"one"},{"timestamp":"2026-07-07T16:00:01Z","level":"info","message":"two"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewBufferString(body))
	req.Header.Set(apiKeyHeader, "local-dev-key")
	rec := httptest.NewRecorder()
	observer := &stubObserver{}

	NewHandler(Config{
		MaxLogEntries: 1,
		Authenticator: stubAuthenticator{authz: Authorization{Decision: AuthorizationAllowed, APIKeyID: 10, TenantID: 1, ServiceID: 2, RateLimitPerSec: 100}},
		Observer:      observer,
		RateLimiter:   allowAllRateLimiter{},
		Publisher:     &stubPublisher{},
	}).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if len(observer.observations) != 1 || observer.observations[0].Outcome != AuthOutcomeBatchTooLarge {
		t.Fatalf("expected batch_too_large observation, got %#v", observer.observations)
	}
}

func TestHandlerRejectsTooLargeRequestBody(t *testing.T) {
	body := `{"schema_version":"logs.ingest.v1","service":"checkout","env":"prod","source":"app","logs":[{"timestamp":"2026-07-07T16:00:00Z","level":"info","message":"` + string(bytes.Repeat([]byte("x"), 64)) + `"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewBufferString(body))
	req.Header.Set(apiKeyHeader, "local-dev-key")
	rec := httptest.NewRecorder()
	observer := &stubObserver{}

	NewHandler(Config{
		MaxBodyBytes:  64,
		MaxLogEntries: 1000,
		Authenticator: stubAuthenticator{authz: Authorization{Decision: AuthorizationAllowed}},
		Observer:      observer,
		RateLimiter:   allowAllRateLimiter{},
		Publisher:     &stubPublisher{},
	}).ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d: %s", rec.Code, rec.Body.String())
	}
	if len(observer.observations) != 1 || observer.observations[0].Outcome != AuthOutcomeRequestBodyTooLarge {
		t.Fatalf("expected request_body_too_large observation, got %#v", observer.observations)
	}
}

func TestNormalizeAndValidateTransformsCanonicalFields(t *testing.T) {
	req := BatchRequest{
		SchemaVersion: "logs.ingest.v1",
		Service:       " Checkout ",
		Env:           " PROD ",
		Source:        " App ",
		Logs: []LogRecord{
			{
				Timestamp: "2026-07-07T16:00:00-07:00",
				Level:     " WARNING ",
				Message:   " database timeout ",
				Fields: map[string]any{
					" host ": " api-1 ",
					"region": " us-west-2 ",
				},
			},
		},
	}

	normalized, err := req.NormalizeAndValidate(1000)
	if err != nil {
		t.Fatalf("expected normalized request, got %v", err)
	}
	if normalized.Service != "checkout" || normalized.Env != "prod" || normalized.Source != "app" {
		t.Fatalf("expected canonical tags, got %+v", normalized)
	}
	if normalized.Logs[0].Timestamp != "2026-07-07T23:00:00Z" {
		t.Fatalf("expected canonical UTC timestamp, got %q", normalized.Logs[0].Timestamp)
	}
	if normalized.Logs[0].Level != "warn" {
		t.Fatalf("expected canonical level warn, got %q", normalized.Logs[0].Level)
	}
	if normalized.Logs[0].Message != "database timeout" {
		t.Fatalf("expected trimmed message, got %q", normalized.Logs[0].Message)
	}
	if normalized.Logs[0].Fields["host"] != "api-1" {
		t.Fatalf("expected trimmed host field, got %#v", normalized.Logs[0].Fields["host"])
	}
}

func TestNormalizeAndValidateRejectsMissingSchemaVersion(t *testing.T) {
	req := BatchRequest{
		Service: "checkout",
		Env:     "prod",
		Source:  "app",
		Logs: []LogRecord{
			{Timestamp: "2026-07-07T16:00:00Z", Level: "info", Message: "booting"},
		},
	}

	if _, err := req.NormalizeAndValidate(1000); err == nil || err.Error() != "schema_version is required" {
		t.Fatalf("expected schema_version error, got %v", err)
	}
}

func TestNormalizeAndValidateRejectsUnsupportedSchemaVersion(t *testing.T) {
	req := BatchRequest{
		SchemaVersion: "logs.ingest.v2",
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []LogRecord{
			{Timestamp: "2026-07-07T16:00:00Z", Level: "info", Message: "booting"},
		},
	}

	if _, err := req.NormalizeAndValidate(1000); err == nil || err.Error() != `unsupported schema_version "logs.ingest.v2"` {
		t.Fatalf("expected unsupported schema_version error, got %v", err)
	}
}

func TestNormalizeAndValidateRejectsUnsupportedLevel(t *testing.T) {
	req := BatchRequest{
		SchemaVersion: IngestSchemaVersion,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []LogRecord{
			{Timestamp: "2026-07-07T16:00:00Z", Level: "verbose", Message: "booting"},
		},
	}

	if _, err := req.NormalizeAndValidate(1000); err == nil || err.Error() != "log[0]: level is unsupported" {
		t.Fatalf("expected unsupported level error, got %v", err)
	}
}

func TestNormalizeAndValidateRejectsNestedFieldObjects(t *testing.T) {
	req := BatchRequest{
		SchemaVersion: IngestSchemaVersion,
		Service:       "checkout",
		Env:           "prod",
		Source:        "app",
		Logs: []LogRecord{
			{
				Timestamp: "2026-07-07T16:00:00Z",
				Level:     "info",
				Message:   "booting",
				Fields: map[string]any{
					"region": map[string]any{"name": "us-west-2"},
				},
			},
		},
	}

	if _, err := req.NormalizeAndValidate(1000); err == nil || err.Error() != "log[0]: fields[\"region\"]: unsupported field value type map[string]interface {}" {
		t.Fatalf("expected nested fields error, got %v", err)
	}
}

type stubPublisher struct {
	err   error
	batch *contracts.LogsRawEvent
}

type stubAuthenticator struct {
	authz Authorization
	err   error
}

type stubObserver struct {
	observations []AuthObservation
}

type allowAllRateLimiter struct{}

type denyAllRateLimiter struct{}

func (s stubAuthenticator) Authorize(_ context.Context, _ string, _ BatchRequest) (Authorization, error) {
	return s.authz, s.err
}

func (s *stubObserver) ObserveAuth(_ context.Context, obs AuthObservation) {
	s.observations = append(s.observations, obs)
}

func (allowAllRateLimiter) Allow(_ int64, _ int) bool {
	return true
}

func (denyAllRateLimiter) Allow(_ int64, _ int) bool {
	return false
}

func (s *stubPublisher) Publish(_ context.Context, batch contracts.LogsRawEvent) error {
	if s.err != nil {
		return s.err
	}

	copy := batch
	s.batch = &copy
	return nil
}
