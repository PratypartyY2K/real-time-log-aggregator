package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	commonclickhouse "github.com/PratypartyY2K/real-time-log-aggregator/internal/clickhouse"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/logging"
)

type LogWriter interface {
	AlreadyProcessed(context.Context, uint64, string) (bool, error)
	WriteBatch(context.Context, []NormalizedLogRecord) error
}

type ClickHouseWriter struct {
	url     string
	client  *http.Client
	metrics *Metrics
}

type clickHouseInsertRow struct {
	Timestamp    string `json:"timestamp"`
	TenantID     uint64 `json:"tenant_id"`
	Service      string `json:"service"`
	Environment  string `json:"environment"`
	Source       string `json:"source"`
	Host         string `json:"host"`
	Level        string `json:"level"`
	TraceID      string `json:"trace_id"`
	Fingerprint  string `json:"fingerprint"`
	Message      string `json:"message"`
	FieldsJSON   string `json:"fields_json"`
	IngestID     string `json:"ingest_id"`
	RawSizeBytes uint32 `json:"raw_size_bytes"`
}

func NewClickHouseWriter(dsn string) *ClickHouseWriter {
	return &ClickHouseWriter{
		url: strings.TrimSpace(dsn),
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (w *ClickHouseWriter) Check(ctx context.Context) error {
	if w == nil {
		return fmt.Errorf("clickhouse writer is not configured")
	}
	return commonclickhouse.Probe(ctx, w.url, w.client)
}

func (w *ClickHouseWriter) AlreadyProcessed(ctx context.Context, tenantID uint64, ingestID string) (bool, error) {
	if tenantID == 0 {
		return false, fmt.Errorf("tenant id is required")
	}
	if strings.TrimSpace(ingestID) == "" {
		return false, fmt.Errorf("ingest id is required")
	}
	if w == nil || w.url == "" {
		return false, fmt.Errorf("clickhouse writer is not configured")
	}

	query := fmt.Sprintf("SELECT 1 FROM logs WHERE tenant_id = %d AND ingest_id = '%s' LIMIT 1 SETTINGS optimize_skip_unused_shards = 1 FORMAT JSONEachRow\n", tenantID, strings.ReplaceAll(ingestID, "'", "''"))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, strings.NewReader(query))
	if err != nil {
		return false, fmt.Errorf("build clickhouse exists request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	logging.PropagateContext(ctx, req.Header)

	resp, err := w.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("check clickhouse ingest id: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		payload, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return false, fmt.Errorf("clickhouse ingest id check failed with status %s", resp.Status)
		}
		return false, fmt.Errorf("clickhouse ingest id check failed with status %s: %s", resp.Status, strings.TrimSpace(string(payload)))
	}

	payload, err := io.ReadAll(io.LimitReader(resp.Body, 16))
	if err != nil {
		return false, fmt.Errorf("read clickhouse ingest id check response: %w", err)
	}
	return len(bytes.TrimSpace(payload)) > 0, nil
}

func (w *ClickHouseWriter) WriteBatch(ctx context.Context, records []NormalizedLogRecord) error {
	if len(records) == 0 {
		return nil
	}
	start := time.Now()
	result := resultSuccess
	defer func() {
		if w != nil && w.metrics != nil {
			w.metrics.ObserveClickHouseWrite(result, time.Since(start))
		}
	}()
	if w == nil || w.url == "" {
		result = "error"
		return fmt.Errorf("clickhouse writer is not configured")
	}

	var body bytes.Buffer
	body.WriteString("INSERT INTO logs SETTINGS insert_distributed_sync = 1 FORMAT JSONEachRow\n")

	encoder := json.NewEncoder(&body)
	for _, record := range records {
		row := clickHouseInsertRow{
			Timestamp:    record.Timestamp.UTC().Format(time.RFC3339Nano),
			TenantID:     record.TenantID,
			Service:      record.Service,
			Environment:  record.Environment,
			Source:       record.Source,
			Host:         record.Host,
			Level:        record.Level,
			TraceID:      record.TraceID,
			Fingerprint:  record.Fingerprint,
			Message:      record.Message,
			FieldsJSON:   record.FieldsJSON,
			IngestID:     record.IngestID,
			RawSizeBytes: record.RawSizeBytes,
		}
		if err := encoder.Encode(row); err != nil {
			result = "error"
			return fmt.Errorf("encode clickhouse row: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, &body)
	if err != nil {
		result = "error"
		return fmt.Errorf("build clickhouse request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	logging.PropagateContext(ctx, req.Header)

	resp, err := w.client.Do(req)
	if err != nil {
		result = "error"
		return fmt.Errorf("write clickhouse batch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		result = "error"
		payload, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return fmt.Errorf("clickhouse insert failed with status %s", resp.Status)
		}
		return fmt.Errorf("clickhouse insert failed with status %s: %s", resp.Status, strings.TrimSpace(string(payload)))
	}

	return nil
}
