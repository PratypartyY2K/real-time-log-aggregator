package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/ingest"
)

func main() {
	url := flag.String("url", "http://localhost:8080/v1/logs", "ingest endpoint")
	apiKey := flag.String("api-key", "local-dev-key", "ingest API key")
	at := flag.String("at", "", "incident time in RFC3339 (default: now)")
	traceID := flag.String("trace-id", "", "incident trace ID (default: unique from incident time)")
	printOnly := flag.Bool("print", false, "print batches instead of sending them")
	flag.Parse()

	incidentAt := time.Now().UTC()
	var err error
	if *at != "" {
		incidentAt, err = time.Parse(time.RFC3339, *at)
		if err != nil {
			exit(fmt.Errorf("parse -at: %w", err))
		}
	}

	if *traceID == "" {
		*traceID = fmt.Sprintf("rag-payment-%d", incidentAt.UnixMilli())
	}
	batches := buildBatches(incidentAt, *traceID)
	if *printOnly {
		if err := json.NewEncoder(os.Stdout).Encode(batches); err != nil {
			exit(err)
		}
		return
	}
	for _, batch := range batches {
		if err := send(*url, *apiKey, batch); err != nil {
			exit(err)
		}
		fmt.Printf("sent service=%s logs=%d\n", batch.Service, len(batch.Logs))
	}
	fmt.Printf("incident_at=%s trace_id=%s expected_cause=payments-db-connection-pool-exhaustion\n", incidentAt.Format(time.RFC3339), *traceID)
}

func buildBatches(at time.Time, incidentTraceID string) []ingest.BatchRequest {
	healthyTraceID := incidentTraceID + "-healthy"
	noiseTraceID := incidentTraceID + "-noise"
	log := func(offset time.Duration, level, message, traceID string, fields map[string]any) ingest.LogRecord {
		if fields == nil {
			fields = map[string]any{}
		}
		fields["trace_id"] = traceID
		return ingest.LogRecord{Timestamp: at.Add(offset).UTC().Format(time.RFC3339Nano), Level: level, Message: message, Fields: fields}
	}
	batch := func(service string, logs ...ingest.LogRecord) ingest.BatchRequest {
		return ingest.BatchRequest{SchemaVersion: ingest.IngestSchemaVersion, Service: service, Env: "prod", Source: "rag-fixture", Logs: logs}
	}

	return []ingest.BatchRequest{
		batch("catalog",
			log(-90*time.Second, "info", "catalog cache refreshed", noiseTraceID+"-1", map[string]any{"items": 842}),
			log(-20*time.Second, "warn", "search request completed with partial results", noiseTraceID+"-2", map[string]any{"duration_ms": 410})),
		batch("gateway",
			log(-4*time.Second, "info", "POST /checkout started", incidentTraceID, nil),
			log(4*time.Second, "error", "POST /checkout returned 500", incidentTraceID, map[string]any{"duration_ms": 8012, "error_code": "UPSTREAM_FAILURE"})),
		batch("checkout",
			log(-3*time.Second, "info", "checkout request started", incidentTraceID, map[string]any{"cart_items": 3}),
			log(3*time.Second, "error", "payment request failed after 8000 ms", incidentTraceID, map[string]any{"duration_ms": 8000, "error_code": "PAYMENT_TIMEOUT"}),
			log(15*time.Second, "info", "checkout request completed", healthyTraceID, map[string]any{"duration_ms": 184})),
		batch("payments",
			log(-2*time.Second, "info", "payment authorization started", incidentTraceID, nil),
			log(0, "error", "database connection pool exhausted", incidentTraceID, map[string]any{"active_connections": 50, "max_connections": 50, "error_code": "DB_POOL_EXHAUSTED"}),
			log(time.Second, "error", "database query timed out after 5000 ms", incidentTraceID, map[string]any{"duration_ms": 5000, "error_code": "DB_TIMEOUT"}),
			log(2*time.Second, "error", "payment authorization failed", incidentTraceID, map[string]any{"error_code": "PAYMENT_DB_UNAVAILABLE"}),
			log(16*time.Second, "info", "payment authorization completed", healthyTraceID, map[string]any{"duration_ms": 91})),
		batch("payments-db",
			log(-time.Second, "warn", "connection pool utilization reached 100 percent", incidentTraceID, map[string]any{"active_connections": 50, "max_connections": 50}),
			log(time.Second, "error", "query cancelled because connection acquisition timed out", incidentTraceID, map[string]any{"wait_ms": 5000, "error_code": "CONNECTION_ACQUIRE_TIMEOUT"})),
	}
}

func send(url, apiKey string, batch ingest.BatchRequest) error {
	body, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s: %s", resp.Status, bytes.TrimSpace(message))
	}
	return nil
}

func exit(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "rag-fixture:", err)
		os.Exit(1)
	}
}
