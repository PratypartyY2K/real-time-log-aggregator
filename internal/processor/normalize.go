package processor

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
)

type NormalizedLogRecord struct {
	Timestamp    time.Time
	TenantID     uint64
	Service      string
	Environment  string
	Source       string
	Level        string
	Host         string
	TraceID      string
	Fingerprint  string
	Message      string
	FieldsJSON   string
	IngestID     string
	RawSizeBytes uint32
}

func normalizeBatch(batch contracts.LogsRawEvent) ([]NormalizedLogRecord, error) {
	receivedAt, err := parseTimestamp(batch.ReceivedAt)
	if err != nil {
		return nil, fmt.Errorf("parse received_at: %w", err)
	}

	service := normalizeTag(batch.Service)
	environment := normalizeTag(batch.Env)
	source := normalizeTag(batch.Source)

	records := make([]NormalizedLogRecord, 0, len(batch.Logs))
	for _, record := range batch.Logs {
		normalized, err := normalizeRecord(batch.RequestID, batch.TraceID, batch.TenantID, receivedAt, service, environment, source, record)
		if err != nil {
			return nil, err
		}
		records = append(records, normalized)
	}

	return records, nil
}

func normalizeRecord(
	requestID string,
	batchTraceID string,
	tenantID uint64,
	receivedAt time.Time,
	service, environment, source string,
	record contracts.LogsRawRecord,
) (NormalizedLogRecord, error) {
	timestamp := receivedAt
	if strings.TrimSpace(record.Timestamp) != "" {
		parsed, err := parseTimestamp(record.Timestamp)
		if err != nil {
			return NormalizedLogRecord{}, fmt.Errorf("parse log timestamp: %w", err)
		}
		timestamp = parsed
	}

	fields := cloneFields(record.Fields)
	host := extractNormalizedField(fields, "host", "hostname")
	traceID := extractNormalizedField(fields, "trace_id", "traceid", "trace-id")
	if traceID == "" {
		traceID = strings.TrimSpace(batchTraceID)
	}
	fieldsJSON, err := marshalFields(fields)
	if err != nil {
		return NormalizedLogRecord{}, fmt.Errorf("marshal log fields: %w", err)
	}

	level := normalizeLevel(record.Level)
	message := strings.TrimSpace(record.Message)

	return NormalizedLogRecord{
		Timestamp:    timestamp,
		TenantID:     tenantID,
		Service:      service,
		Environment:  environment,
		Source:       source,
		Level:        level,
		Host:         host,
		TraceID:      traceID,
		Fingerprint:  fingerprint(service, environment, source, level, message, fieldsJSON),
		Message:      message,
		FieldsJSON:   fieldsJSON,
		IngestID:     requestID,
		RawSizeBytes: uint32(rawRecordSize(record)),
	}, nil
}

func parseTimestamp(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}

func normalizeLevel(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "information", "informational":
		return "info"
	case "warn", "warning":
		return "warn"
	case "err", "error":
		return "error"
	case "debug":
		return "debug"
	case "trace":
		return "trace"
	case "fatal", "critical", "panic":
		return "fatal"
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func normalizeTag(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func cloneFields(fields map[string]any) map[string]any {
	if len(fields) == 0 {
		return nil
	}

	cloned := make(map[string]any, len(fields))
	for key, value := range fields {
		cloned[key] = value
	}
	return cloned
}

func extractNormalizedField(fields map[string]any, names ...string) string {
	if len(fields) == 0 {
		return ""
	}

	type match struct {
		lookup string
		actual string
	}

	matches := make([]match, 0, len(fields))
	for key := range fields {
		normalizedKey := normalizeFieldKey(key)
		for _, name := range names {
			if normalizedKey == normalizeFieldKey(name) {
				matches = append(matches, match{lookup: normalizedKey, actual: key})
				break
			}
		}
	}
	if len(matches) == 0 {
		return ""
	}

	slices.SortFunc(matches, func(a, b match) int {
		return strings.Compare(a.actual, b.actual)
	})

	value := strings.TrimSpace(fmt.Sprint(fields[matches[0].actual]))
	for _, match := range matches {
		delete(fields, match.actual)
	}
	return value
}

func normalizeFieldKey(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "-", "")
	value = strings.ReplaceAll(value, "_", "")
	return value
}

func marshalFields(fields map[string]any) (string, error) {
	if len(fields) == 0 {
		return "{}", nil
	}

	payload, err := json.Marshal(fields)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func fingerprint(service, environment, source, level, message, fieldsJSON string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		service,
		environment,
		source,
		level,
		message,
		fieldsJSON,
	}, "\n")))
	return hex.EncodeToString(sum[:16])
}

func rawRecordSize(record contracts.LogsRawRecord) int {
	payload, err := json.Marshal(record)
	if err != nil {
		return 0
	}
	return len(payload)
}
