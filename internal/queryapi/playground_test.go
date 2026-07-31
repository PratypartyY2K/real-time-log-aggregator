package queryapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPlaygroundHandlerServesUI(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/playground/", nil)
	rec := httptest.NewRecorder()
	PlaygroundHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected playground status 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, "Query playground") || !strings.Contains(body, "query-form") {
		t.Fatalf("expected playground HTML, got %q", body)
	}
}

func TestOpenAPIHandlerServesContract(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
	rec := httptest.NewRecorder()
	OpenAPIHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected OpenAPI status 200, got %d", rec.Code)
	}
	contract := rec.Body.String()
	for _, expected := range []string{"openapi: 3.1.0", "/v1/logs:", "/v1/incidents/summary:", "ApiKey:", "IngestBatch:", "IncidentSummaryResponse:"} {
		if !strings.Contains(contract, expected) {
			t.Fatalf("expected OpenAPI contract to contain %q", expected)
		}
	}
}
