package queryapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
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

type clickHouseAnalyticsQueryResponse struct {
	Data []map[string]json.RawMessage `json:"data"`
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

func (s *ClickHouseStore) StreamLogs(ctx context.Context, filter QueryFilter, emit func(LogRecord) error) error {
	if s == nil || s.url == "" {
		return fmt.Errorf("clickhouse store is not configured")
	}
	if emit == nil {
		return fmt.Errorf("stream emitter is not configured")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, strings.NewReader(buildLogsStreamQuery(filter)))
	if err != nil {
		return fmt.Errorf("build clickhouse stream query request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("stream clickhouse logs: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		payload, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return fmt.Errorf("clickhouse stream query failed with status %s", resp.Status)
		}
		return fmt.Errorf("clickhouse stream query failed with status %s: %s", resp.Status, strings.TrimSpace(string(payload)))
	}

	decoder := json.NewDecoder(resp.Body)
	for {
		var row clickHouseQueryRow
		if err := decoder.Decode(&row); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("decode clickhouse stream row: %w", err)
		}
		record, err := toLogRecord(row)
		if err != nil {
			return err
		}
		if err := emit(record); err != nil {
			return err
		}
	}
	return nil
}

func (s *ClickHouseStore) QueryAnalytics(ctx context.Context, query AnalyticsQuery) ([]AnalyticsPoint, error) {
	if s == nil || s.url == "" {
		return nil, fmt.Errorf("clickhouse store is not configured")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, strings.NewReader(buildAnalyticsQuery(query)))
	if err != nil {
		return nil, fmt.Errorf("build clickhouse analytics request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query clickhouse analytics: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		payload, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return nil, fmt.Errorf("clickhouse analytics query failed with status %s", resp.Status)
		}
		return nil, fmt.Errorf("clickhouse analytics query failed with status %s: %s", resp.Status, strings.TrimSpace(string(payload)))
	}

	var result clickHouseAnalyticsQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode clickhouse analytics response: %w", err)
	}

	points := make([]AnalyticsPoint, 0, len(result.Data))
	for _, row := range result.Data {
		point, err := toAnalyticsPoint(row)
		if err != nil {
			return nil, err
		}
		points = append(points, point)
	}

	return points, nil
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
	if filter.Offset > 0 {
		query.WriteString(fmt.Sprintf("OFFSET %d ", filter.Offset))
	}
	query.WriteString("FORMAT JSON")
	return query.String()
}

func buildLogsStreamQuery(filter QueryFilter) string {
	query := buildLogsQuery(filter)
	return strings.TrimSuffix(query, "FORMAT JSON") + "FORMAT JSONEachRow"
}

func buildAnalyticsQuery(query AnalyticsQuery) string {
	var builder strings.Builder

	selectExprs := make([]string, 0, len(query.GroupBy)+2)
	groupExprs := make([]string, 0, len(query.GroupBy)+1)
	orderExprs := make([]string, 0, len(query.GroupBy)+1)

	if bucketExpr := analyticsBucketExpr(query.Bucket); bucketExpr != "" {
		selectExprs = append(selectExprs, "formatDateTime("+bucketExpr+", '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS bucket")
		groupExprs = append(groupExprs, bucketExpr)
		orderExprs = append(orderExprs, "bucket DESC")
	}

	for _, field := range query.GroupBy {
		expr, alias := analyticsGroupExpr(field)
		selectExprs = append(selectExprs, expr+" AS "+alias)
		groupExprs = append(groupExprs, expr)
		orderExprs = append(orderExprs, alias+" ASC")
	}

	selectExprs = append(selectExprs, analyticsValueExpr(query)+" AS value")

	builder.WriteString("SELECT ")
	builder.WriteString(strings.Join(selectExprs, ", "))
	builder.WriteString(" FROM logs WHERE timestamp >= toDateTime64(")
	builder.WriteString(quoteLiteral(query.Start.UTC().Format(time.RFC3339Nano)))
	builder.WriteString(", 3, 'UTC') AND timestamp < toDateTime64(")
	builder.WriteString(quoteLiteral(query.End.UTC().Format(time.RFC3339Nano)))
	builder.WriteString(", 3, 'UTC') ")

	if query.Service != "" {
		builder.WriteString("AND service = ")
		builder.WriteString(quoteLiteral(query.Service))
		builder.WriteByte(' ')
	}
	if query.Environment != "" {
		builder.WriteString("AND environment = ")
		builder.WriteString(quoteLiteral(query.Environment))
		builder.WriteByte(' ')
	}
	if query.Level != "" {
		builder.WriteString("AND level = ")
		builder.WriteString(quoteLiteral(query.Level))
		builder.WriteByte(' ')
	}
	if query.ErrorCode != "" {
		builder.WriteString("AND JSONExtractString(fields_json, 'error_code') = ")
		builder.WriteString(quoteLiteral(query.ErrorCode))
		builder.WriteByte(' ')
	}

	if len(groupExprs) > 0 {
		builder.WriteString("GROUP BY ")
		builder.WriteString(strings.Join(groupExprs, ", "))
		builder.WriteByte(' ')
	}

	if query.TopK > 0 {
		builder.WriteString("ORDER BY value DESC")
	} else if len(orderExprs) > 0 {
		builder.WriteString("ORDER BY ")
		builder.WriteString(strings.Join(orderExprs, ", "))
	} else {
		builder.WriteString("ORDER BY value DESC")
	}
	builder.WriteByte(' ')

	if query.TopK > 0 {
		builder.WriteString(fmt.Sprintf("LIMIT %d ", query.TopK))
	} else {
		limit := query.Limit
		if limit <= 0 {
			limit = defaultLimit
		}
		builder.WriteString(fmt.Sprintf("LIMIT %d ", limit))
	}

	builder.WriteString("FORMAT JSON")
	return builder.String()
}

func analyticsBucketExpr(bucket string) string {
	switch bucket {
	case "minute":
		return "toStartOfMinute(timestamp)"
	case "hour":
		return "toStartOfHour(timestamp)"
	case "day":
		return "toStartOfDay(timestamp)"
	default:
		return ""
	}
}

func analyticsGroupExpr(field string) (expr, alias string) {
	switch field {
	case "service":
		return "service", "group_service"
	case "env":
		return "environment", "group_env"
	case "level":
		return "level", "group_level"
	case "error_code":
		return "JSONExtractString(fields_json, 'error_code')", "group_error_code"
	default:
		return "", ""
	}
}

func analyticsValueExpr(query AnalyticsQuery) string {
	switch query.Aggregation {
	case "count":
		return "toFloat64(count())"
	case "rate":
		return fmt.Sprintf("toFloat64(count()) / %d", analyticsBucketSeconds(query.Bucket))
	case "percentile":
		return fmt.Sprintf("toFloat64(quantileTDigest(%s)(%s))", strconv.FormatFloat(query.Percentile/100, 'f', -1, 64), analyticsValueFieldExpr(query.ValueField))
	default:
		return "toFloat64(count())"
	}
}

func analyticsBucketSeconds(bucket string) int {
	switch bucket {
	case "minute":
		return 60
	case "hour":
		return 3600
	case "day":
		return 86400
	default:
		return 1
	}
}

func analyticsValueFieldExpr(valueField string) string {
	if valueField == "raw_size_bytes" {
		return "toFloat64(raw_size_bytes)"
	}
	fieldName := strings.TrimPrefix(valueField, "field.")
	return "toFloat64OrNull(JSONExtractString(fields_json, " + quoteLiteral(fieldName) + "))"
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

func toAnalyticsPoint(row map[string]json.RawMessage) (AnalyticsPoint, error) {
	point := AnalyticsPoint{
		Group: map[string]string{},
	}

	if rawBucket, ok := row["bucket"]; ok {
		if err := json.Unmarshal(rawBucket, &point.Bucket); err != nil {
			return AnalyticsPoint{}, fmt.Errorf("decode analytics bucket: %w", err)
		}
	}

	for key, raw := range row {
		if !strings.HasPrefix(key, "group_") {
			continue
		}
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return AnalyticsPoint{}, fmt.Errorf("decode analytics group value: %w", err)
		}
		point.Group[strings.TrimPrefix(key, "group_")] = value
	}

	if len(point.Group) == 0 {
		point.Group = nil
	}

	if rawValue, ok := row["value"]; ok {
		if err := json.Unmarshal(rawValue, &point.Value); err != nil {
			return AnalyticsPoint{}, fmt.Errorf("decode analytics value: %w", err)
		}
	}

	return point, nil
}

func quoteLiteral(value string) string {
	value = strings.ReplaceAll(value, `'`, `''`)
	return "'" + value + "'"
}
