package queryapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	defaultLimit = 100
	maxLimit     = 1000
)

type LogStore interface {
	QueryLogs(context.Context, QueryFilter) ([]LogRecord, error)
}

type Handler struct {
	store LogStore
}

type QueryFilter struct {
	Start   time.Time
	End     time.Time
	Service string
	Level   string
	Limit   int
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
	Logs  []LogRecord `json:"logs"`
	Count int         `json:"count"`
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

	filter, err := parseQueryFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	logs, err := h.store.QueryLogs(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to query logs")
		return
	}

	writeJSON(w, http.StatusOK, queryResponse{
		Logs:  logs,
		Count: len(logs),
	})
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

	service := strings.TrimSpace(query.Get("service"))
	if service != "" && !isSafeTagFilter(service) {
		return QueryFilter{}, errors.New("service contains unsupported characters")
	}

	level := strings.ToLower(strings.TrimSpace(query.Get("level")))
	if level != "" && !isSafeTagFilter(level) {
		return QueryFilter{}, errors.New("level contains unsupported characters")
	}

	limit := defaultLimit
	if rawLimit := strings.TrimSpace(query.Get("limit")); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 {
			return QueryFilter{}, errors.New("limit must be a positive integer")
		}
		if parsed > maxLimit {
			parsed = maxLimit
		}
		limit = parsed
	}

	return QueryFilter{
		Start:   start,
		End:     end,
		Service: service,
		Level:   level,
		Limit:   limit,
	}, nil
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
