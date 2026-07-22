package contracts

import "errors"

const LogsRawSchemaVersion = "logs.raw.v1"

type LogsRawEvent struct {
	SchemaVersion string          `json:"schema_version"`
	RequestID     string          `json:"request_id"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	TraceID       string          `json:"trace_id,omitempty"`
	Fingerprint   string          `json:"fingerprint"`
	ReceivedAt    string          `json:"received_at"`
	TenantID      uint64          `json:"tenant_id"`
	Service       string          `json:"service"`
	Env           string          `json:"env"`
	Source        string          `json:"source"`
	Logs          []LogsRawRecord `json:"logs"`
}

type LogsRawRecord struct {
	Timestamp string         `json:"timestamp"`
	Level     string         `json:"level"`
	Message   string         `json:"message"`
	Fields    map[string]any `json:"fields,omitempty"`
}

func (e LogsRawEvent) Validate() error {
	if e.SchemaVersion != LogsRawSchemaVersion {
		return errors.New("invalid schema version")
	}
	if e.RequestID == "" {
		return errors.New("request_id is required")
	}
	if e.Fingerprint == "" {
		return errors.New("fingerprint is required")
	}
	if e.ReceivedAt == "" {
		return errors.New("received_at is required")
	}
	if e.TenantID == 0 {
		return errors.New("tenant_id is required")
	}
	if e.Service == "" {
		return errors.New("service is required")
	}
	if e.Env == "" {
		return errors.New("env is required")
	}
	if len(e.Logs) == 0 {
		return errors.New("at least one log entry is required")
	}

	return nil
}
