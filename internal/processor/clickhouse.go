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

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/alerts"
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

func (w *ClickHouseWriter) QueryAlertRecords(ctx context.Context, rule alerts.Rule, start, end time.Time, limit int) ([]alerts.Record, error) {
	if w == nil || w.url == "" {
		return nil, fmt.Errorf("clickhouse writer is not configured")
	}
	if rule.TenantID <= 0 {
		return nil, fmt.Errorf("tenant id is required")
	}
	if !start.Before(end) {
		return nil, fmt.Errorf("alert query start must be before end")
	}
	if limit <= 0 {
		limit = defaultAlertEvaluationMaxRecords
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, strings.NewReader(buildAlertRecordsQuery(rule, start, end, limit)))
	if err != nil {
		return nil, fmt.Errorf("build clickhouse alert query request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	logging.PropagateContext(ctx, req.Header)

	resp, err := w.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query clickhouse alert records: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		payload, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return nil, fmt.Errorf("clickhouse alert query failed with status %s", resp.Status)
		}
		return nil, fmt.Errorf("clickhouse alert query failed with status %s: %s", resp.Status, strings.TrimSpace(string(payload)))
	}

	decoder := json.NewDecoder(resp.Body)
	records := make([]NormalizedLogRecord, 0)
	for {
		var row clickHouseInsertRow
		if err := decoder.Decode(&row); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("decode clickhouse alert row: %w", err)
		}
		timestamp, err := time.Parse(time.RFC3339Nano, row.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("parse clickhouse alert timestamp: %w", err)
		}
		records = append(records, NormalizedLogRecord{
			Timestamp:    timestamp.UTC(),
			TenantID:     row.TenantID,
			Service:      row.Service,
			Environment:  row.Environment,
			Source:       row.Source,
			Host:         row.Host,
			Level:        row.Level,
			TraceID:      row.TraceID,
			Fingerprint:  row.Fingerprint,
			Message:      row.Message,
			FieldsJSON:   row.FieldsJSON,
			IngestID:     row.IngestID,
			RawSizeBytes: row.RawSizeBytes,
		})
	}
	return recordsFromNormalized(records)
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

func buildAlertRecordsQuery(rule alerts.Rule, start, end time.Time, limit int) string {
	var builder strings.Builder
	builder.WriteString("SELECT timestamp, tenant_id, service, environment, source, host, level, trace_id, fingerprint, message, fields_json, ingest_id, raw_size_bytes FROM logs WHERE tenant_id = ")
	builder.WriteString(fmt.Sprintf("%d", rule.TenantID))
	builder.WriteString(" AND timestamp >= toDateTime64(")
	builder.WriteString(clickHouseQuoteLiteral(start.UTC().Format(time.RFC3339Nano)))
	builder.WriteString(", 3, 'UTC') AND timestamp < toDateTime64(")
	builder.WriteString(clickHouseQuoteLiteral(end.UTC().Format(time.RFC3339Nano)))
	builder.WriteString(", 3, 'UTC') ")
	if rule.ServiceName.Valid && strings.TrimSpace(rule.ServiceName.String) != "" {
		builder.WriteString("AND service = ")
		builder.WriteString(clickHouseQuoteLiteral(strings.TrimSpace(rule.ServiceName.String)))
		builder.WriteByte(' ')
	}
	if rule.Environment.Valid && strings.TrimSpace(rule.Environment.String) != "" {
		builder.WriteString("AND environment = ")
		builder.WriteString(clickHouseQuoteLiteral(strings.TrimSpace(rule.Environment.String)))
		builder.WriteByte(' ')
	}
	if strings.TrimSpace(rule.LogLevel) != "" {
		builder.WriteString("AND level = ")
		builder.WriteString(clickHouseQuoteLiteral(strings.ToLower(strings.TrimSpace(rule.LogLevel))))
		builder.WriteByte(' ')
	}
	if strings.TrimSpace(rule.Fingerprint) != "" {
		builder.WriteString("AND fingerprint = ")
		builder.WriteString(clickHouseQuoteLiteral(strings.TrimSpace(rule.Fingerprint)))
		builder.WriteByte(' ')
	}
	builder.WriteString("ORDER BY timestamp DESC LIMIT ")
	builder.WriteString(fmt.Sprintf("%d", limit))
	builder.WriteString(" SETTINGS optimize_skip_unused_shards = 1, skip_unavailable_shards = 1 FORMAT JSONEachRow")
	return builder.String()
}

func clickHouseQuoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
