package ingest

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

const apiKeyHeader = "X-API-Key"

type Config struct {
	MaxBodyBytes int64
}

type Handler struct {
	maxBodyBytes int64
}

type BatchRequest struct {
	Service string      `json:"service"`
	Env     string      `json:"env"`
	Source  string      `json:"source"`
	Logs    []LogRecord `json:"logs"`
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
	return &Handler{maxBodyBytes: limit}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.TrimSpace(r.Header.Get(apiKeyHeader)) == "" {
		writeError(w, http.StatusUnauthorized, "missing api key")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)
	defer r.Body.Close()

	var req BatchRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if decoder.More() {
		writeError(w, http.StatusBadRequest, "request must contain a single JSON object")
		return
	}

	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, BatchResponse{
		RequestID: randomID(),
		Accepted:  len(req.Logs),
		Status:    "queued_stub",
	})
}

func (r BatchRequest) Validate() error {
	if strings.TrimSpace(r.Service) == "" {
		return errors.New("service is required")
	}
	if strings.TrimSpace(r.Env) == "" {
		return errors.New("env is required")
	}
	if len(r.Logs) == 0 {
		return errors.New("at least one log entry is required")
	}
	if len(r.Logs) > 1000 {
		return errors.New("batch exceeds 1000 log entries")
	}

	for i, log := range r.Logs {
		if err := log.Validate(); err != nil {
			return errors.New("log[" + itoa(i) + "]: " + err.Error())
		}
	}
	return nil
}

func (r LogRecord) Validate() error {
	if strings.TrimSpace(r.Timestamp) == "" {
		return errors.New("timestamp is required")
	}
	if _, err := time.Parse(time.RFC3339, r.Timestamp); err != nil {
		return errors.New("timestamp must be RFC3339")
	}
	if strings.TrimSpace(r.Level) == "" {
		return errors.New("level is required")
	}
	if strings.TrimSpace(r.Message) == "" {
		return errors.New("message is required")
	}
	return nil
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func randomID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "bootstrap-request"
	}
	return hex.EncodeToString(buf[:])
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
