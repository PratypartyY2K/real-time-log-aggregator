package queryapi

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
)

type ClickHouseStore struct {
	url    string
	client *http.Client
}

type clickHouseQueryResponse struct {
	Data []clickHouseQueryRow `json:"data"`
}

type clickHouseQueryRow struct {
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

func NewClickHouseStore(dsn string) *ClickHouseStore {
	return &ClickHouseStore{
		url: strings.TrimSpace(dsn),
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (s *ClickHouseStore) Check(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("clickhouse store is not configured")
	}
	return commonclickhouse.Probe(ctx, s.url, s.client)
}

func (s *ClickHouseStore) QueryLogs(ctx context.Context, filter QueryFilter) ([]LogRecord, error) {
	if s == nil || s.url == "" {
		return nil, fmt.Errorf("clickhouse store is not configured")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, strings.NewReader(buildLogsQuery(filter)))
	if err != nil {
		return nil, fmt.Errorf("build clickhouse query request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query clickhouse logs: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		payload, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return nil, fmt.Errorf("clickhouse query failed with status %s", resp.Status)
		}
		return nil, fmt.Errorf("clickhouse query failed with status %s: %s", resp.Status, strings.TrimSpace(string(payload)))
	}

	var result clickHouseQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode clickhouse query response: %w", err)
	}

	logs := make([]LogRecord, 0, len(result.Data))
	for _, row := range result.Data {
		record, err := toLogRecord(row)
		if err != nil {
			return nil, err
		}
		logs = append(logs, record)
	}

	return logs, nil
}

func buildLogsQuery(filter QueryFilter) string {
	var query bytes.Buffer
	query.WriteString("SELECT ")
	query.WriteString("timestamp, tenant_id, service, environment, source, host, level, trace_id, fingerprint, message, fields_json, ingest_id, raw_size_bytes ")
	query.WriteString("FROM logs ")
	query.WriteString("WHERE timestamp >= toDateTime64(")
	query.WriteString(quoteLiteral(filter.Start.UTC().Format(time.RFC3339Nano)))
	query.WriteString(", 3, 'UTC') ")
	query.WriteString("AND timestamp < toDateTime64(")
	query.WriteString(quoteLiteral(filter.End.UTC().Format(time.RFC3339Nano)))
	query.WriteString(", 3, 'UTC') ")
	if filter.Service != "" {
		query.WriteString("AND service = ")
		query.WriteString(quoteLiteral(filter.Service))
		query.WriteByte(' ')
	}
	if filter.Level != "" {
		query.WriteString("AND level = ")
		query.WriteString(quoteLiteral(filter.Level))
		query.WriteByte(' ')
	}
	query.WriteString("ORDER BY timestamp DESC ")
	query.WriteString(fmt.Sprintf("LIMIT %d ", filter.Limit))
	query.WriteString("FORMAT JSON")
	return query.String()
}

func toLogRecord(row clickHouseQueryRow) (LogRecord, error) {
	timestamp, err := time.Parse(time.RFC3339Nano, row.Timestamp)
	if err != nil {
		return LogRecord{}, fmt.Errorf("parse clickhouse timestamp: %w", err)
	}

	fields := map[string]any{}
	if strings.TrimSpace(row.FieldsJSON) != "" {
		if err := json.Unmarshal([]byte(row.FieldsJSON), &fields); err != nil {
			return LogRecord{}, fmt.Errorf("decode clickhouse fields_json: %w", err)
		}
	}

	return LogRecord{
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
		Fields:       fields,
		IngestID:     row.IngestID,
		RawSizeBytes: row.RawSizeBytes,
	}, nil
}

func quoteLiteral(value string) string {
	value = strings.ReplaceAll(value, `'`, `''`)
	return "'" + value + "'"
}
