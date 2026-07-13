package contracts

const LogsDLQSchemaVersion = "logs.dlq.v1"

type LogsDLQEvent struct {
	SchemaVersion string        `json:"schema_version"`
	FailedAt      string        `json:"failed_at"`
	Reason        string        `json:"reason"`
	Error         string        `json:"error"`
	RequestID     string        `json:"request_id,omitempty"`
	TenantID      uint64        `json:"tenant_id,omitempty"`
	Service       string        `json:"service,omitempty"`
	Env           string        `json:"env,omitempty"`
	Source        string        `json:"source,omitempty"`
	Attempts      uint64        `json:"attempts"`
	RawPayload    []byte        `json:"raw_payload"`
	Event         *LogsRawEvent `json:"event,omitempty"`
}
