package queryapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	defaultLimit      = 100
	maxLimit          = 1000
	maxOffset         = 10000
	maxRawQueryWindow = 7 * 24 * time.Hour
)

type LogStore interface {
	QueryLogs(context.Context, QueryFilter) ([]LogRecord, error)
	StreamLogs(context.Context, QueryFilter, func(LogRecord) error) error
}

type Handler struct {
	store LogStore
}

type ClusterStatus struct {
	Partial           bool     `json:"partial"`
	UnavailableShards []string `json:"unavailable_shards,omitempty"`
}

type ClusterStatusProvider interface {
	ClusterStatus(context.Context) ClusterStatus
}

type QueryFilter struct {
	TenantID uint64
	Start    time.Time
	End      time.Time
	Service  string
	Level    string
	Limit    int
	Offset   int
	Stream   bool
}

type LogRecord struct {
	Timestamp    time.Time      `json:"timestamp"`
	TenantID     uint64         `json:"tenant_id"`
	Service      string         `json:"service"`
	Environment  string         `json:"environment"`
	Source       string         `json:"source"`
	Host         string         `json:"host"`
	Level        string         `json:"level"`
	TraceID      string         `json:"trace_id"`
	Fingerprint  string         `json:"fingerprint"`
	Message      string         `json:"message"`
	Fields       map[string]any `json:"fields"`
	IngestID     string         `json:"ingest_id"`
	RawSizeBytes uint32         `json:"raw_size_bytes"`
}

type queryResponse struct {
	Logs              []LogRecord `json:"logs"`
	Count             int         `json:"count"`
	NextOffset        *int        `json:"next_offset,omitempty"`
	Partial           bool        `json:"partial"`
	UnavailableShards []string    `json:"unavailable_shards,omitempty"`
}

func NewHandler(store LogStore) *Handler {
	return &Handler{store: store}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "log store unavailable")
		return
	}
	tenantID, ok := TenantIDFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "tenant identity required")
		return
	}

	filter, err := parseQueryFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	filter.TenantID = tenantID
	clusterStatus := clusterStatus(r.Context(), h.store)

	if filter.Stream {
		h.streamLogs(w, r, filter, clusterStatus)
		return
	}

	logs, err := h.store.QueryLogs(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to query logs")
		return
	}

	var nextOffset *int
	if len(logs) == filter.Limit && filter.Offset+filter.Limit <= maxOffset {
		next := filter.Offset + filter.Limit
		nextOffset = &next
	}

	writeJSON(w, http.StatusOK, queryResponse{
		Logs:              logs,
		Count:             len(logs),
		NextOffset:        nextOffset,
		Partial:           clusterStatus.Partial,
		UnavailableShards: clusterStatus.UnavailableShards,
	})
}

func (h *Handler) streamLogs(w http.ResponseWriter, r *http.Request, filter QueryFilter, status ClusterStatus) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	setClusterStatusHeaders(w.Header(), status)
	w.WriteHeader(http.StatusOK)

	encoder := json.NewEncoder(w)
	count := 0
	err := h.store.StreamLogs(r.Context(), filter, func(record LogRecord) error {
		if count >= filter.Limit {
			return nil
		}
		count++
		if err := encoder.Encode(record); err != nil {
			return fmt.Errorf("encode stream record: %w", err)
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		return nil
	})
	if err != nil {
		_, _ = w.Write([]byte(`{"error":"failed to stream logs"}` + "\n"))
	}
}

func clusterStatus(ctx context.Context, store any) ClusterStatus {
	provider, ok := store.(ClusterStatusProvider)
	if !ok {
		return ClusterStatus{}
	}
	return provider.ClusterStatus(ctx)
}

func setClusterStatusHeaders(header http.Header, status ClusterStatus) {
	header.Set("X-Logagg-Partial-Results", strconv.FormatBool(status.Partial))
	if len(status.UnavailableShards) > 0 {
		header.Set("X-Logagg-Unavailable-Shards", strings.Join(status.UnavailableShards, ","))
	}
}

func parseQueryFilter(r *http.Request) (QueryFilter, error) {
	query := r.URL.Query()

	start, err := parseRequiredTimestamp(query.Get("start"), "start")
	if err != nil {
		return QueryFilter{}, err
	}
	end, err := parseRequiredTimestamp(query.Get("end"), "end")
	if err != nil {
		return QueryFilter{}, err
	}
	if !start.Before(end) {
		return QueryFilter{}, errors.New("start must be before end")
	}
	if end.Sub(start) > maxRawQueryWindow {
		return QueryFilter{}, errors.New("time range cannot exceed 7 days")
	}

	service := strings.TrimSpace(query.Get("service"))
	if service != "" && !isSafeTagFilter(service) {
		return QueryFilter{}, errors.New("service contains unsupported characters")
	}

	level := strings.ToLower(strings.TrimSpace(query.Get("level")))
	if level != "" && !isSafeTagFilter(level) {
		return QueryFilter{}, errors.New("level contains unsupported characters")
	}

	limit, err := parseBoundedPositiveInt(firstNonEmpty(query.Get("page_size"), query.Get("limit")), defaultLimit, maxLimit, "limit")
	if err != nil {
		return QueryFilter{}, err
	}

	offset := 0
	if rawOffset := strings.TrimSpace(query.Get("offset")); rawOffset != "" {
		parsed, err := strconv.Atoi(rawOffset)
		if err != nil || parsed < 0 {
			return QueryFilter{}, errors.New("offset must be a non-negative integer")
		}
		if parsed > maxOffset {
			return QueryFilter{}, errors.New("offset exceeds maximum")
		}
		offset = parsed
	}

	return QueryFilter{
		Start:   start,
		End:     end,
		Service: service,
		Level:   level,
		Limit:   limit,
		Offset:  offset,
		Stream:  parseBool(query.Get("stream")),
	}, nil
}

func parseBoundedPositiveInt(raw string, defaultValue, maxValue int, name string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return 0, errors.New(name + " must be a positive integer")
	}
	if parsed > maxValue {
		parsed = maxValue
	}
	return parsed, nil
}

func parseBool(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func parseRequiredTimestamp(value, name string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, errors.New(name + " is required")
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, errors.New(name + " must be a valid RFC3339 timestamp")
	}
	return parsed.UTC(), nil
}

func isSafeTagFilter(value string) bool {
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

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
