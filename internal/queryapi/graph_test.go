package queryapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGraphHandlerBuildsDependenciesErrorsAndSessions(t *testing.T) {
	start := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	store := &stubGraphStore{records: []GraphRecord{
		{Timestamp: start, Service: "gateway", Level: "error", TraceID: "trace-1", Fields: map[string]any{"session_id": "session-1", "user_id": "user@example.com", "downstream_service": "checkout"}},
		{Timestamp: start.Add(time.Second), Service: "checkout", Level: "error", TraceID: "trace-1", Fields: map[string]any{"session_id": "session-1", "user_id": "user@example.com"}},
		{Timestamp: start.Add(2 * time.Second), Service: "payments", Level: "info", TraceID: "trace-1", Fields: map[string]any{"session_id": "session-1", "user_id": "user@example.com"}},
	}}
	req := tenantRequest(httptest.NewRequest(http.MethodGet, "/v1/graph?start=2026-07-22T10:00:00Z&end=2026-07-22T11:00:00Z&session_id=session-1", nil))
	rec := httptest.NewRecorder()

	NewGraphHandler(store).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var response graphResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if store.query.TenantID != 7 || store.query.SessionID != "session-1" {
		t.Fatalf("unexpected query: %+v", store.query)
	}
	if len(response.Nodes) != 3 || len(response.Edges) != 2 || len(response.Sessions) != 1 {
		t.Fatalf("unexpected graph: %+v", response)
	}
	var gatewayEdge *ServiceEdge
	for index := range response.Edges {
		if response.Edges[index].Source == "gateway" && response.Edges[index].Target == "checkout" {
			gatewayEdge = &response.Edges[index]
		}
	}
	if gatewayEdge == nil || gatewayEdge.PropagatedErrorCount != 1 {
		t.Fatalf("expected propagated gateway error, got %+v", response.Edges)
	}
	if response.Sessions[0].ID != "session-1" || response.Sessions[0].UserID != "user@example.com" || response.Sessions[0].ErrorCount != 2 {
		t.Fatalf("unexpected session: %+v", response.Sessions[0])
	}
}

func TestGraphHandlerGroupsRequestAndTraceFallbacks(t *testing.T) {
	now := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	nodes, edges, sessions := buildServiceGraph([]GraphRecord{
		{Timestamp: now, Service: "api", TraceID: "trace-1", Fields: map[string]any{"request-id": "request-1"}},
		{Timestamp: now.Add(time.Second), Service: "worker", TraceID: "trace-1", Fields: map[string]any{"request_id": "request-1"}},
		{Timestamp: now.Add(2 * time.Second), Service: "mailer", TraceID: "trace-2"},
	})
	if len(nodes) != 3 || len(edges) != 1 || len(sessions) != 2 {
		t.Fatalf("unexpected graph sizes: nodes=%d edges=%d sessions=%d", len(nodes), len(edges), len(sessions))
	}
	if sessions[0].Kind != "trace" && sessions[1].Kind != "trace" {
		t.Fatalf("expected trace fallback group, got %+v", sessions)
	}
}

func TestGraphHandlerUsesUserFlowAsFinalFallback(t *testing.T) {
	now := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	_, edges, sessions := buildServiceGraph([]GraphRecord{
		{Timestamp: now, Service: "web", Fields: map[string]any{"user_id": "user@example.com"}},
		{Timestamp: now.Add(time.Second), Service: "profile", Fields: map[string]any{"user-id": "user@example.com"}},
	})
	if len(edges) != 1 || len(sessions) != 1 || sessions[0].Kind != "user" || sessions[0].ID != "user@example.com" {
		t.Fatalf("expected user fallback flow, got edges=%+v sessions=%+v", edges, sessions)
	}
}

func TestGraphHandlerRejectsOversizedWindow(t *testing.T) {
	req := tenantRequest(httptest.NewRequest(http.MethodGet, "/v1/graph?start=2026-07-20T10:00:00Z&end=2026-07-22T10:00:00Z", nil))
	rec := httptest.NewRecorder()
	NewGraphHandler(&stubGraphStore{}).ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestGraphHandlerReportsTruncation(t *testing.T) {
	now := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	store := &stubGraphStore{records: []GraphRecord{{Timestamp: now, Service: "api", TraceID: "trace-1"}, {Timestamp: now.Add(time.Second), Service: "worker", TraceID: "trace-1"}}}
	req := tenantRequest(httptest.NewRequest(http.MethodGet, "/v1/graph?start=2026-07-22T10:00:00Z&end=2026-07-22T11:00:00Z&limit=1", nil))
	rec := httptest.NewRecorder()
	NewGraphHandler(store).ServeHTTP(rec, req)
	var response graphResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !response.Truncated || response.RecordCount != 1 {
		t.Fatalf("expected truncated single-record graph, got %+v", response)
	}
}

type stubGraphStore struct {
	query   GraphQuery
	records []GraphRecord
	err     error
}

func (s *stubGraphStore) QueryGraphRecords(_ context.Context, query GraphQuery) ([]GraphRecord, error) {
	s.query = query
	return s.records, s.err
}
