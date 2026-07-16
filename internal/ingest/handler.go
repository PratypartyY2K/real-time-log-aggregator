package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/contracts"
)

const apiKeyHeader = "X-API-Key"

const IngestSchemaVersion = "logs.ingest.v1"

type Config struct {
	MaxBodyBytes  int64
	MaxLogEntries int
	Authenticator Authenticator
	Observer      Observer
	RateLimiter   RateLimiter
	Backpressure  BackpressureController
	Publisher     Publisher
}

type Handler struct {
	maxBodyBytes  int64
	maxLogEntries int
	authenticator Authenticator
	observer      Observer
	rateLimiter   RateLimiter
	backpressure  BackpressureController
	publisher     Publisher
}

type AuthorizationDecision int

const (
	AuthorizationInvalid AuthorizationDecision = iota
	AuthorizationForbidden
	AuthorizationAllowed
)

type Authorization struct {
	Decision        AuthorizationDecision
	APIKeyID        int64
	TenantID        int64
	ServiceID       int64
	RateLimitPerSec int
}

type Authenticator interface {
	Authorize(context.Context, string, BatchRequest) (Authorization, error)
}

type RateLimiter interface {
	Allow(apiKeyID int64, limitPerSec int) bool
}

type Publisher interface {
	Publish(context.Context, contracts.LogsRawEvent) error
}

type BatchRequest struct {
	SchemaVersion string      `json:"schema_version"`
	Service       string      `json:"service"`
	Env           string      `json:"env"`
	Source        string      `json:"source"`
	Logs          []LogRecord `json:"logs"`
}

type LogRecord struct {
	Timestamp string         `json:"timestamp"`
	Level     string         `json:"level"`
	Message   string         `json:"message"`
	Fields    map[string]any `json:"fields,omitempty"`
}

type BatchResponse struct {
	RequestID string `json:"request_id"`
	Accepted  int    `json:"accepted"`
	Status    string `json:"status"`
}

func NewHandler(cfg Config) *Handler {
	limit := cfg.MaxBodyBytes
	if limit <= 0 {
		limit = 1 << 20
	}
	maxLogs := cfg.MaxLogEntries
	if maxLogs <= 0 {
		maxLogs = 1000
	}
	return &Handler{
		maxBodyBytes:  limit,
		maxLogEntries: maxLogs,
		authenticator: cfg.Authenticator,
		observer:      cfg.Observer,
		rateLimiter:   cfg.RateLimiter,
		backpressure:  cfg.Backpressure,
		publisher:     cfg.Publisher,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	apiKey := strings.TrimSpace(r.Header.Get(apiKeyHeader))
	if apiKey == "" {
		h.observeAuth(r.Context(), AuthObservation{Outcome: AuthOutcomeMissingAPIKey})
		writeError(w, http.StatusUnauthorized, "missing api key")
		return
	}
	if h.authenticator == nil {
		h.observeAuth(r.Context(), AuthObservation{Outcome: AuthOutcomeAuthenticatorMissing})
		writeError(w, http.StatusServiceUnavailable, "api key authenticator unavailable")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)
	defer r.Body.Close()

	var req BatchRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	decoder.UseNumber()

	if err := decoder.Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			h.observeAuth(r.Context(), AuthObservation{Outcome: AuthOutcomeRequestBodyTooLarge})
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		h.observeAuth(r.Context(), AuthObservation{Outcome: AuthOutcomeInvalidRequestBody})
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if decoder.More() {
		h.observeAuth(r.Context(), AuthObservation{Outcome: AuthOutcomeInvalidRequestBody})
		writeError(w, http.StatusBadRequest, "request must contain a single JSON object")
		return
	}

	req, err := req.NormalizeAndValidate(h.maxLogEntries)
	if err != nil {
		if errors.Is(err, errBatchTooLarge) {
			h.observeAuth(r.Context(), AuthObservation{
				Outcome: AuthOutcomeBatchTooLarge,
				Service: req.Service,
				Env:     req.Env,
			})
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	authz, err := h.authenticator.Authorize(r.Context(), apiKey, req)
	if err != nil {
		h.observeAuth(r.Context(), AuthObservation{
			Outcome: AuthOutcomeBackendError,
			Service: req.Service,
			Env:     req.Env,
		})
		writeError(w, http.StatusServiceUnavailable, "failed to validate api key")
		return
	}
	switch authz.Decision {
	case AuthorizationAllowed:
		h.observeAuth(r.Context(), AuthObservation{
			Outcome:   AuthOutcomeAuthorized,
			Service:   req.Service,
			Env:       req.Env,
			TenantID:  authz.TenantID,
			ServiceID: authz.ServiceID,
		})
	case AuthorizationForbidden:
		h.observeAuth(r.Context(), AuthObservation{
			Outcome:   AuthOutcomeForbiddenScope,
			Service:   req.Service,
			Env:       req.Env,
			TenantID:  authz.TenantID,
			ServiceID: authz.ServiceID,
		})
		writeError(w, http.StatusForbidden, "api key not authorized for service/env")
		return
	default:
		h.observeAuth(r.Context(), AuthObservation{
			Outcome: AuthOutcomeInvalidAPIKey,
			Service: req.Service,
			Env:     req.Env,
		})
		writeError(w, http.StatusUnauthorized, "invalid api key")
		return
	}
	if h.rateLimiter != nil && !h.rateLimiter.Allow(authz.APIKeyID, authz.RateLimitPerSec) {
		h.observeAuth(r.Context(), AuthObservation{
			Outcome:   AuthOutcomeRateLimited,
			Service:   req.Service,
			Env:       req.Env,
			TenantID:  authz.TenantID,
			ServiceID: authz.ServiceID,
			APIKeyID:  authz.APIKeyID,
		})
		writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
		return
	}

	if h.publisher == nil {
		writeError(w, http.StatusServiceUnavailable, "ingest publisher unavailable")
		return
	}
	if h.backpressure != nil {
		if err := h.backpressure.Apply(r.Context()); err != nil {
			if errors.Is(err, ErrBackpressureRejected) {
				writeError(w, http.StatusTooManyRequests, "ingest queue is backpressured")
				return
			}
			writeError(w, http.StatusServiceUnavailable, "backpressure check failed")
			return
		}
	}

	requestID := batchFingerprint(authz.TenantID, req)
	event := toLogsRawEvent(requestID, time.Now().UTC().Format(time.RFC3339Nano), authz.TenantID, req)
	if err := h.publisher.Publish(r.Context(), event); err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to queue logs")
		return
	}

	writeJSON(w, http.StatusAccepted, BatchResponse{
		RequestID: requestID,
		Accepted:  len(req.Logs),
		Status:    "queued",
	})
}

func (h *Handler) observeAuth(ctx context.Context, obs AuthObservation) {
	if h.observer != nil {
		h.observer.ObserveAuth(ctx, obs)
	}
}

func toLogsRawEvent(requestID, receivedAt string, tenantID int64, req BatchRequest) contracts.LogsRawEvent {
	logs := make([]contracts.LogsRawRecord, 0, len(req.Logs))
	for _, record := range req.Logs {
		logs = append(logs, contracts.LogsRawRecord{
			Timestamp: record.Timestamp,
			Level:     record.Level,
			Message:   record.Message,
			Fields:    record.Fields,
		})
	}

	return contracts.LogsRawEvent{
		SchemaVersion: contracts.LogsRawSchemaVersion,
		RequestID:     requestID,
		Fingerprint:   requestID,
		ReceivedAt:    receivedAt,
		TenantID:      uint64(tenantID),
		Service:       req.Service,
		Env:           req.Env,
		Source:        req.Source,
		Logs:          logs,
	}
}

var errBatchTooLarge = errors.New("batch exceeds max log entries")

func (r BatchRequest) NormalizeAndValidate(maxLogEntries int) (BatchRequest, error) {
	r.SchemaVersion = strings.TrimSpace(r.SchemaVersion)
	if r.SchemaVersion == "" {
		return BatchRequest{}, errors.New("schema_version is required")
	}
	if r.SchemaVersion != IngestSchemaVersion {
		return BatchRequest{}, fmt.Errorf("unsupported schema_version %q", r.SchemaVersion)
	}

	r.Service = normalizeTagValue(r.Service)
	if r.Service == "" {
		return BatchRequest{}, errors.New("service is required")
	}
	if !isSafeTagValue(r.Service) {
		return BatchRequest{}, errors.New("service contains unsupported characters")
	}

	r.Env = normalizeTagValue(r.Env)
	if r.Env == "" {
		return BatchRequest{}, errors.New("env is required")
	}
	if !isSafeTagValue(r.Env) {
		return BatchRequest{}, errors.New("env contains unsupported characters")
	}

	r.Source = normalizeTagValue(r.Source)
	if r.Source == "" {
		return BatchRequest{}, errors.New("source is required")
	}
	if !isSafeTagValue(r.Source) {
		return BatchRequest{}, errors.New("source contains unsupported characters")
	}
	if len(r.Logs) == 0 {
		return BatchRequest{}, errors.New("at least one log entry is required")
	}
	if maxLogEntries <= 0 {
		maxLogEntries = 1000
	}
	if len(r.Logs) > maxLogEntries {
		return BatchRequest{}, errors.Join(errBatchTooLarge, errors.New("batch exceeds "+itoa(maxLogEntries)+" log entries"))
	}

	normalizedLogs := make([]LogRecord, 0, len(r.Logs))
	for i, log := range r.Logs {
		normalizedLog, err := log.NormalizeAndValidate()
		if err != nil {
			return BatchRequest{}, errors.New("log[" + itoa(i) + "]: " + err.Error())
		}
		normalizedLogs = append(normalizedLogs, normalizedLog)
	}
	r.Logs = normalizedLogs
	return r, nil
}

func (r LogRecord) NormalizeAndValidate() (LogRecord, error) {
	if strings.TrimSpace(r.Timestamp) == "" {
		return LogRecord{}, errors.New("timestamp is required")
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(r.Timestamp))
	if err != nil {
		return LogRecord{}, errors.New("timestamp must be RFC3339")
	}

	level, ok := normalizeIngestLevel(r.Level)
	if !ok {
		return LogRecord{}, errors.New("level is unsupported")
	}
	message := strings.TrimSpace(r.Message)
	if message == "" {
		return LogRecord{}, errors.New("message is required")
	}

	fields, err := normalizeFields(r.Fields)
	if err != nil {
		return LogRecord{}, err
	}

	return LogRecord{
		Timestamp: parsed.UTC().Format(time.RFC3339Nano),
		Level:     level,
		Message:   message,
		Fields:    fields,
	}, nil
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

type fingerprintBatch struct {
	TenantID uint64               `json:"tenant_id"`
	Service  string               `json:"service"`
	Env      string               `json:"env"`
	Source   string               `json:"source"`
	Logs     []fingerprintLogItem `json:"logs"`
}

type fingerprintLogItem struct {
	Timestamp string         `json:"timestamp"`
	Level     string         `json:"level"`
	Message   string         `json:"message"`
	Fields    map[string]any `json:"fields,omitempty"`
}

func batchFingerprint(tenantID int64, req BatchRequest) string {
	payload := fingerprintBatch{
		TenantID: uint64(tenantID),
		Service:  normalizeTagValue(req.Service),
		Env:      normalizeTagValue(req.Env),
		Source:   normalizeTagValue(req.Source),
		Logs:     make([]fingerprintLogItem, 0, len(req.Logs)),
	}

	for _, record := range req.Logs {
		payload.Logs = append(payload.Logs, fingerprintLogItem{
			Timestamp: strings.TrimSpace(record.Timestamp),
			Level:     normalizeTagValue(record.Level),
			Message:   strings.TrimSpace(record.Message),
			Fields:    record.Fields,
		})
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		sum := sha256.Sum256([]byte(strings.Join([]string{
			strconv(int(tenantID)),
			req.Service,
			req.Env,
			req.Source,
			itoa(len(req.Logs)),
		}, "\n")))
		return hex.EncodeToString(sum[:16])
	}

	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:16])
}

func normalizeTagValue(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func isSafeTagValue(value string) bool {
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '.', r == '_', r == '-', r == ':':
		default:
			return false
		}
	}
	return true
}

func normalizeIngestLevel(value string) (string, bool) {
	switch normalizeTagValue(value) {
	case "info", "information", "informational":
		return "info", true
	case "warn", "warning":
		return "warn", true
	case "error", "err":
		return "error", true
	case "debug":
		return "debug", true
	case "trace":
		return "trace", true
	case "fatal", "critical", "panic":
		return "fatal", true
	default:
		return "", false
	}
}

func normalizeFields(fields map[string]any) (map[string]any, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	keys := make([]string, 0, len(fields))
	normalized := make(map[string]any, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		normalizedKey := strings.TrimSpace(key)
		if normalizedKey == "" {
			return nil, errors.New("fields keys must be non-empty")
		}
		if !isSafeFieldKey(normalizedKey) {
			return nil, fmt.Errorf("fields key %q contains unsupported characters", normalizedKey)
		}
		value, err := normalizeFieldValue(fields[key])
		if err != nil {
			return nil, fmt.Errorf("fields[%q]: %w", normalizedKey, err)
		}
		normalized[normalizedKey] = value
	}

	return normalized, nil
}

func isSafeFieldKey(value string) bool {
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '.', r == '_', r == '-', r == ':':
		default:
			return false
		}
	}
	return true
}

func normalizeFieldValue(value any) (any, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case bool:
		return typed, nil
	case string:
		return strings.TrimSpace(typed), nil
	case json.Number:
		return typed, nil
	case float64:
		return typed, nil
	case []any:
		normalized := make([]any, 0, len(typed))
		for _, item := range typed {
			value, err := normalizeFieldValue(item)
			if err != nil {
				return nil, err
			}
			normalized = append(normalized, value)
		}
		return normalized, nil
	default:
		return nil, fmt.Errorf("unsupported field value type %T", value)
	}
}

func itoa(v int) string {
	return strconv(v)
}

func strconv(v int) string {
	if v == 0 {
		return "0"
	}

	var digits [20]byte
	pos := len(digits)
	for v > 0 {
		pos--
		digits[pos] = byte('0' + (v % 10))
		v /= 10
	}
	return string(digits[pos:])
}
