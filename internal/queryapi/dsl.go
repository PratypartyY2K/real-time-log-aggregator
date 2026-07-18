package queryapi

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type QueryDSLHandler struct {
	logs      *Handler
	analytics *AnalyticsHandler
}

type QueryDSLRequest struct {
	Type        string   `json:"type"`
	Start       string   `json:"start"`
	End         string   `json:"end"`
	Service     string   `json:"service,omitempty"`
	Environment string   `json:"env,omitempty"`
	Level       string   `json:"level,omitempty"`
	ErrorCode   string   `json:"error_code,omitempty"`
	Aggregation string   `json:"aggregation,omitempty"`
	GroupBy     []string `json:"group_by,omitempty"`
	Bucket      string   `json:"bucket,omitempty"`
	TopK        int      `json:"top_k,omitempty"`
	Percentile  float64  `json:"percentile,omitempty"`
	ValueField  string   `json:"value_field,omitempty"`
	Limit       int      `json:"limit,omitempty"`
	PageSize    int      `json:"page_size,omitempty"`
	Offset      int      `json:"offset,omitempty"`
	Stream      bool     `json:"stream,omitempty"`
}

func NewQueryDSLHandler(store interface {
	LogStore
	AnalyticsStore
}) *QueryDSLHandler {
	return &QueryDSLHandler{
		logs:      NewHandler(store),
		analytics: NewAnalyticsHandler(store),
	}
}

func (h *QueryDSLHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h == nil || h.logs == nil || h.analytics == nil {
		writeError(w, http.StatusServiceUnavailable, "query store unavailable")
		return
	}

	var payload QueryDSLRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid query DSL")
		return
	}

	switch strings.ToLower(strings.TrimSpace(payload.Type)) {
	case "logs":
		h.logs.ServeHTTP(w, dslRequest(r, http.MethodGet, "/v1/logs", logsDSLValues(payload)))
	case "analytics":
		h.analytics.ServeHTTP(w, dslRequest(r, http.MethodGet, "/v1/analytics", analyticsDSLValues(payload)))
	default:
		writeError(w, http.StatusBadRequest, "type must be logs or analytics")
	}
}

func dslRequest(parent *http.Request, method, path string, values url.Values) *http.Request {
	next := parent.Clone(parent.Context())
	next.Method = method
	next.URL.Path = path
	next.URL.RawQuery = values.Encode()
	return next
}

func logsDSLValues(payload QueryDSLRequest) url.Values {
	values := baseDSLValues(payload)
	addPositiveInt(values, "page_size", payload.PageSize)
	addPositiveInt(values, "limit", payload.Limit)
	if payload.Offset > 0 {
		values.Set("offset", strconv.Itoa(payload.Offset))
	}
	if payload.Stream {
		values.Set("stream", "true")
	}
	return values
}

func analyticsDSLValues(payload QueryDSLRequest) url.Values {
	values := baseDSLValues(payload)
	values.Set("aggregation", payload.Aggregation)
	if len(payload.GroupBy) > 0 {
		values.Set("group_by", strings.Join(payload.GroupBy, ","))
	}
	if payload.Bucket != "" {
		values.Set("bucket", payload.Bucket)
	}
	addPositiveInt(values, "top_k", payload.TopK)
	if payload.Percentile > 0 {
		values.Set("percentile", strconv.FormatFloat(payload.Percentile, 'f', -1, 64))
	}
	if payload.ValueField != "" {
		values.Set("value_field", payload.ValueField)
	}
	addPositiveInt(values, "limit", payload.Limit)
	return values
}

func baseDSLValues(payload QueryDSLRequest) url.Values {
	values := url.Values{}
	values.Set("start", payload.Start)
	values.Set("end", payload.End)
	if payload.Service != "" {
		values.Set("service", payload.Service)
	}
	if payload.Environment != "" {
		values.Set("env", payload.Environment)
	}
	if payload.Level != "" {
		values.Set("level", payload.Level)
	}
	if payload.ErrorCode != "" {
		values.Set("error_code", payload.ErrorCode)
	}
	return values
}

func addPositiveInt(values url.Values, name string, value int) {
	if value > 0 {
		values.Set(name, strconv.Itoa(value))
	}
}
