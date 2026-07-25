package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/ingest"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/queryapi"
)

func TestEndToEndIngestProcessQueryPipeline(t *testing.T) {
	publisher := &integrationPublisher{}
	logs := &functionalLogStore{}

	ingestHandler := ingest.NewHandler(ingest.Config{
		MaxLogEntries: 10,
		Authenticator: integrationAuthenticator{
			authz: ingest.Authorization{
				Decision:  ingest.AuthorizationAllowed,
				APIKeyID:  10,
				TenantID:  42,
				ServiceID: 7,
			},
		},
		RateLimiter: integrationRateLimiter{},
		Publisher:   publisher,
	})

	queryHandler := queryapi.TenantAuthMiddleware(
		functionalTenantResolver{keys: map[string]uint64{
			"tenant-42-key": 42,
			"tenant-99-key": 99,
		}},
		queryapi.NewHandler(logs),
	)
	ingestPayload := `{
		"schema_version":"logs.ingest.v1",
		"service":"Checkout",
		"env":"Prod",
		"source":"App",
		"logs":[
			{
				"timestamp":"2026-07-07T16:00:00Z",
				"level":"ERR",
				"message":"database timeout",
				"fields":{"host":"api-1","trace_id":"trace-123","region":"us-west-2"}
			},
			{
				"timestamp":"2026-07-07T16:00:01Z",
				"level":"info",
				"message":"retry scheduled",
				"fields":{"host":"api-1","attempt":1}
			}
		]
	}`
	ingestResponse := doFunctionalRequest(t, ingestHandler, http.MethodPost, "/v1/logs", "local-dev-key", ingestPayload)
	defer ingestResponse.Body.Close()
	if ingestResponse.StatusCode != http.StatusAccepted {
		t.Fatalf("expected ingest status 202, got %d", ingestResponse.StatusCode)
	}

	var accepted ingest.BatchResponse
	decodeFunctionalJSON(t, ingestResponse, &accepted)
	if accepted.Accepted != 2 || accepted.Status != "queued" || accepted.RequestID == "" {
		t.Fatalf("unexpected ingest response: %+v", accepted)
	}
	if publisher.event == nil {
		t.Fatal("expected accepted batch to cross the queue boundary")
	}

	queued := queueRoundTrip(t, *publisher.event)
	if err := handleBatch(context.Background(), &stubLogger{}, logs, nil, nil, nil, queued); err != nil {
		t.Fatalf("process queued batch: %v", err)
	}
	if err := handleBatch(context.Background(), &stubLogger{}, logs, nil, nil, nil, queued); err != nil {
		t.Fatalf("replay queued batch: %v", err)
	}
	if logs.writeCalls != 1 {
		t.Fatalf("expected replay-safe processing, got %d writes", logs.writeCalls)
	}

	queryURL := "/v1/logs?start=2026-07-07T15:59:00Z&end=2026-07-07T16:01:00Z&service=checkout&level=error&trace_id=trace-123"
	queryResponse := doFunctionalRequest(t, queryHandler, http.MethodGet, queryURL, "tenant-42-key", "")
	defer queryResponse.Body.Close()
	if queryResponse.StatusCode != http.StatusOK {
		t.Fatalf("expected query status 200, got %d", queryResponse.StatusCode)
	}

	var result struct {
		Logs    []queryapi.LogRecord `json:"logs"`
		Count   int                  `json:"count"`
		Partial bool                 `json:"partial"`
	}
	decodeFunctionalJSON(t, queryResponse, &result)
	if result.Count != 1 || len(result.Logs) != 1 {
		t.Fatalf("expected one filtered log, got %+v", result)
	}
	record := result.Logs[0]
	if record.TenantID != 42 || record.Service != "checkout" || record.Environment != "prod" || record.Level != "error" {
		t.Fatalf("unexpected normalized query record: %+v", record)
	}
	if record.Host != "api-1" || record.TraceID != "trace-123" || record.Fields["region"] != "us-west-2" {
		t.Fatalf("expected extracted routing fields and preserved custom fields, got %+v", record)
	}
	if record.IngestID != accepted.RequestID {
		t.Fatalf("expected ingest id %q, got %q", accepted.RequestID, record.IngestID)
	}

	isolatedResponse := doFunctionalRequest(t, queryHandler, http.MethodGet, queryURL, "tenant-99-key", "")
	defer isolatedResponse.Body.Close()
	if isolatedResponse.StatusCode != http.StatusOK {
		t.Fatalf("expected isolated query status 200, got %d", isolatedResponse.StatusCode)
	}
	var isolated struct {
		Count int `json:"count"`
	}
	decodeFunctionalJSON(t, isolatedResponse, &isolated)
	if isolated.Count != 0 {
		t.Fatalf("expected tenant isolation to hide records, got count %d", isolated.Count)
	}
}

func doFunctionalRequest(t *testing.T, handler http.Handler, method, target, apiKey, body string) *http.Response {
	t.Helper()

	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set("X-API-Key", apiKey)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder.Result()
}

func decodeFunctionalJSON(t *testing.T, response *http.Response, target any) {
	t.Helper()
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

type functionalTenantResolver struct {
	keys map[string]uint64
}

func (r functionalTenantResolver) ResolveTenant(_ context.Context, apiKey string) (uint64, bool, error) {
	tenantID, ok := r.keys[apiKey]
	return tenantID, ok, nil
}

type functionalLogStore struct {
	mu         sync.RWMutex
	records    []NormalizedLogRecord
	ingestIDs  map[string]struct{}
	writeCalls int
}

func (s *functionalLogStore) AlreadyProcessed(_ context.Context, tenantID uint64, ingestID string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.ingestIDs[functionalIngestKey(tenantID, ingestID)]
	return ok, nil
}

func (s *functionalLogStore) WriteBatch(_ context.Context, batch []NormalizedLogRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ingestIDs == nil {
		s.ingestIDs = make(map[string]struct{})
	}
	s.writeCalls++
	s.records = append(s.records, batch...)
	for _, record := range batch {
		s.ingestIDs[functionalIngestKey(record.TenantID, record.IngestID)] = struct{}{}
	}
	return nil
}

func (s *functionalLogStore) QueryLogs(_ context.Context, filter queryapi.QueryFilter) ([]queryapi.LogRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	matches := make([]queryapi.LogRecord, 0)
	for _, record := range s.records {
		if record.TenantID != filter.TenantID || record.Timestamp.Before(filter.Start) || !record.Timestamp.Before(filter.End) {
			continue
		}
		if filter.Service != "" && record.Service != filter.Service {
			continue
		}
		if filter.Level != "" && record.Level != filter.Level {
			continue
		}
		if filter.TraceID != "" && record.TraceID != filter.TraceID {
			continue
		}
		matches = append(matches, functionalQueryRecord(record))
	}
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Timestamp.After(matches[j].Timestamp)
	})
	if filter.Offset >= len(matches) {
		return []queryapi.LogRecord{}, nil
	}
	end := min(filter.Offset+filter.Limit, len(matches))
	return append([]queryapi.LogRecord(nil), matches[filter.Offset:end]...), nil
}

func (s *functionalLogStore) StreamLogs(ctx context.Context, filter queryapi.QueryFilter, emit func(queryapi.LogRecord) error) error {
	records, err := s.QueryLogs(ctx, filter)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := emit(record); err != nil {
			return err
		}
	}
	return nil
}

func functionalQueryRecord(record NormalizedLogRecord) queryapi.LogRecord {
	fields := map[string]any{}
	_ = json.NewDecoder(bytes.NewBufferString(record.FieldsJSON)).Decode(&fields)
	return queryapi.LogRecord{
		Timestamp:    record.Timestamp,
		TenantID:     record.TenantID,
		Service:      record.Service,
		Environment:  record.Environment,
		Source:       record.Source,
		Host:         record.Host,
		Level:        record.Level,
		TraceID:      record.TraceID,
		Fingerprint:  record.Fingerprint,
		Message:      record.Message,
		Fields:       fields,
		IngestID:     record.IngestID,
		RawSizeBytes: record.RawSizeBytes,
	}
}

func functionalIngestKey(tenantID uint64, ingestID string) string {
	return strconv.FormatUint(tenantID, 10) + ":" + ingestID
}
