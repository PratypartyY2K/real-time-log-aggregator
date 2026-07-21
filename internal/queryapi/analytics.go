package queryapi

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	maxTopK              = 100
	maxAnalyticsWindow   = 31 * 24 * time.Hour
	maxAnalyticsBuckets  = 5000
	maxAnalyticsGroupBys = 3
)

type AnalyticsStore interface {
	QueryAnalytics(context.Context, AnalyticsQuery) ([]AnalyticsPoint, error)
}

type AnalyticsHandler struct {
	store AnalyticsStore
}

type AnalyticsQuery struct {
	TenantID    uint64
	Start       time.Time
	End         time.Time
	Aggregation string
	Service     string
	Environment string
	Level       string
	ErrorCode   string
	GroupBy     []string
	Bucket      string
	TopK        int
	Percentile  float64
	ValueField  string
	Limit       int
}

type AnalyticsPoint struct {
	Bucket string            `json:"bucket,omitempty"`
	Group  map[string]string `json:"group,omitempty"`
	Value  float64           `json:"value"`
}

type analyticsResponse struct {
	Aggregation       string           `json:"aggregation"`
	Bucket            string           `json:"bucket,omitempty"`
	GroupBy           []string         `json:"group_by,omitempty"`
	TopK              int              `json:"top_k,omitempty"`
	Percentile        float64          `json:"percentile,omitempty"`
	ValueField        string           `json:"value_field,omitempty"`
	Limit             int              `json:"limit,omitempty"`
	Results           []AnalyticsPoint `json:"results"`
	Count             int              `json:"count"`
	Partial           bool             `json:"partial"`
	UnavailableShards []string         `json:"unavailable_shards,omitempty"`
}

var allowedGroupBys = []string{"service", "env", "level", "error_code"}

func NewAnalyticsHandler(store AnalyticsStore) *AnalyticsHandler {
	return &AnalyticsHandler{store: store}
}

func (h *AnalyticsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "analytics store unavailable")
		return
	}
	tenantID, ok := TenantIDFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "tenant identity required")
		return
	}

	query, err := parseAnalyticsQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	query.TenantID = tenantID
	clusterStatus := clusterStatus(r.Context(), h.store)

	results, err := h.store.QueryAnalytics(r.Context(), query)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to query analytics")
		return
	}

	writeJSON(w, http.StatusOK, analyticsResponse{
		Aggregation:       query.Aggregation,
		Bucket:            query.Bucket,
		GroupBy:           query.GroupBy,
		TopK:              query.TopK,
		Percentile:        query.Percentile,
		ValueField:        query.ValueField,
		Limit:             query.Limit,
		Results:           results,
		Count:             len(results),
		Partial:           clusterStatus.Partial,
		UnavailableShards: clusterStatus.UnavailableShards,
	})
}

func parseAnalyticsQuery(r *http.Request) (AnalyticsQuery, error) {
	query := r.URL.Query()

	start, err := parseRequiredTimestamp(query.Get("start"), "start")
	if err != nil {
		return AnalyticsQuery{}, err
	}
	end, err := parseRequiredTimestamp(query.Get("end"), "end")
	if err != nil {
		return AnalyticsQuery{}, err
	}
	if !start.Before(end) {
		return AnalyticsQuery{}, errors.New("start must be before end")
	}
	if end.Sub(start) > maxAnalyticsWindow {
		return AnalyticsQuery{}, errors.New("time range cannot exceed 31 days")
	}

	aggregation := strings.ToLower(strings.TrimSpace(query.Get("aggregation")))
	switch aggregation {
	case "count", "rate", "percentile":
	default:
		return AnalyticsQuery{}, errors.New("aggregation must be one of count, rate, or percentile")
	}

	service, err := parseSafeOptionalFilter(query.Get("service"), "service")
	if err != nil {
		return AnalyticsQuery{}, err
	}
	environment, err := parseSafeOptionalFilter(query.Get("env"), "env")
	if err != nil {
		return AnalyticsQuery{}, err
	}
	level, err := parseSafeOptionalFilter(strings.ToLower(query.Get("level")), "level")
	if err != nil {
		return AnalyticsQuery{}, err
	}
	errorCode, err := parseSafeOptionalFilter(query.Get("error_code"), "error_code")
	if err != nil {
		return AnalyticsQuery{}, err
	}

	groupBy, err := parseGroupBy(query["group_by"])
	if err != nil {
		return AnalyticsQuery{}, err
	}
	if len(groupBy) > maxAnalyticsGroupBys {
		return AnalyticsQuery{}, errors.New("group_by cannot contain more than 3 fields")
	}

	bucket := strings.ToLower(strings.TrimSpace(query.Get("bucket")))
	switch bucket {
	case "", "minute", "hour", "day":
	default:
		return AnalyticsQuery{}, errors.New("bucket must be one of minute, hour, or day")
	}
	if bucket != "" && estimatedBucketCount(start, end, bucket) > maxAnalyticsBuckets {
		return AnalyticsQuery{}, errors.New("bucketed query would produce too many time buckets")
	}

	topK := 0
	if rawTopK := strings.TrimSpace(query.Get("top_k")); rawTopK != "" {
		parsed, err := strconv.Atoi(rawTopK)
		if err != nil || parsed <= 0 {
			return AnalyticsQuery{}, errors.New("top_k must be a positive integer")
		}
		if parsed > maxTopK {
			parsed = maxTopK
		}
		topK = parsed
		if len(groupBy) == 0 {
			return AnalyticsQuery{}, errors.New("top_k requires at least one group_by field")
		}
		if bucket != "" {
			return AnalyticsQuery{}, errors.New("top_k does not support time bucketing")
		}
	}

	limit := defaultLimit
	if topK > 0 {
		limit = topK
	} else {
		parsed, err := parseBoundedPositiveInt(query.Get("limit"), defaultLimit, maxLimit, "limit")
		if err != nil {
			return AnalyticsQuery{}, err
		}
		limit = parsed
	}

	percentile := 0.0
	valueField := ""
	if aggregation == "rate" && bucket == "" {
		return AnalyticsQuery{}, errors.New("rate aggregation requires a bucket")
	}
	if aggregation == "percentile" {
		rawPercentile := strings.TrimSpace(query.Get("percentile"))
		if rawPercentile == "" {
			return AnalyticsQuery{}, errors.New("percentile is required for percentile aggregation")
		}
		parsed, err := strconv.ParseFloat(rawPercentile, 64)
		if err != nil || parsed <= 0 || parsed >= 100 {
			return AnalyticsQuery{}, errors.New("percentile must be a number between 0 and 100")
		}
		percentile = parsed

		valueField = strings.TrimSpace(query.Get("value_field"))
		if !isAllowedValueField(valueField) {
			return AnalyticsQuery{}, errors.New("value_field must be raw_size_bytes or field.<name>")
		}
	}

	return AnalyticsQuery{
		Start:       start,
		End:         end,
		Aggregation: aggregation,
		Service:     service,
		Environment: environment,
		Level:       level,
		ErrorCode:   errorCode,
		GroupBy:     groupBy,
		Bucket:      bucket,
		TopK:        topK,
		Percentile:  percentile,
		ValueField:  valueField,
		Limit:       limit,
	}, nil
}

func parseSafeOptionalFilter(value, name string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	if !isSafeTagFilter(value) {
		return "", errors.New(name + " contains unsupported characters")
	}
	return value, nil
}

func parseGroupBy(values []string) ([]string, error) {
	groupBy := make([]string, 0)
	seen := map[string]struct{}{}

	for _, raw := range values {
		for _, part := range strings.Split(raw, ",") {
			field := strings.ToLower(strings.TrimSpace(part))
			if field == "" {
				continue
			}
			if !slices.Contains(allowedGroupBys, field) {
				return nil, errors.New("group_by contains unsupported field")
			}
			if _, ok := seen[field]; ok {
				continue
			}
			seen[field] = struct{}{}
			groupBy = append(groupBy, field)
		}
	}

	return groupBy, nil
}

func isAllowedValueField(value string) bool {
	value = strings.TrimSpace(value)
	if value == "raw_size_bytes" {
		return true
	}
	if strings.HasPrefix(value, "field.") {
		return isSafeTagFilter(strings.TrimPrefix(value, "field."))
	}
	return false
}

func estimatedBucketCount(start, end time.Time, bucket string) int {
	var size time.Duration
	switch bucket {
	case "minute":
		size = time.Minute
	case "hour":
		size = time.Hour
	case "day":
		size = 24 * time.Hour
	default:
		return 0
	}
	return int(end.Sub(start)/size) + 1
}
