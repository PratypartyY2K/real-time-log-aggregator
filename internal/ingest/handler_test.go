package ingest

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
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

	NewHandler(Config{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
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
