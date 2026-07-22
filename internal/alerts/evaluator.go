package alerts

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Record struct {
	Timestamp    time.Time
	TenantID     uint64
	Service      string
	Environment  string
	Source       string
	Host         string
	Level        string
	TraceID      string
	Fingerprint  string
	Message      string
	Fields       map[string]any
	IngestID     string
	RawSizeBytes uint32
}

type Trigger struct {
	RuleID        int64
	RuleName      string
	RuleType      string
	Severity      string
	GroupKey      string
	Group         map[string]string
	MatchCount    int
	MetricValue   float64
	Threshold     float64
	WindowSeconds int
	Percentile    float64
	ValueField    string
}

type Filter struct {
	Level           string            `json:"level"`
	Source          string            `json:"source"`
	Host            string            `json:"host"`
	TraceID         string            `json:"trace_id"`
	MessageContains string            `json:"message_contains"`
	Pattern         string            `json:"pattern"`
	Target          string            `json:"target"`
	FieldEquals     map[string]string `json:"field_equals"`
	ValueField      string            `json:"value_field"`
	Percentile      float64           `json:"percentile"`
}

func Evaluate(rule Rule, records []Record) ([]Trigger, error) {
	filter, err := parseFilter(rule.FilterJSON)
	if err != nil {
		return nil, fmt.Errorf("parse filter_json: %w", err)
	}

	groupBy, err := parseGroupBy(rule.GroupByJSON)
	if err != nil {
		return nil, fmt.Errorf("parse group_by: %w", err)
	}

	switch strings.ToLower(strings.TrimSpace(rule.RuleType)) {
	case "count_threshold":
		threshold, err := parseCountThreshold(rule.Threshold)
		if err != nil {
			return nil, fmt.Errorf("parse threshold: %w", err)
		}
		return evaluateCountThreshold(rule, filter, groupBy, threshold, records), nil
	case "pattern_match":
		threshold, err := parseCountThreshold(rule.Threshold)
		if err != nil {
			return nil, fmt.Errorf("parse threshold: %w", err)
		}
		return evaluatePatternMatch(rule, filter, groupBy, threshold, records)
	case "rate_threshold", "rate_based":
		threshold, err := parseMetricThreshold(rule.Threshold)
		if err != nil {
			return nil, fmt.Errorf("parse threshold: %w", err)
		}
		if rule.WindowSeconds <= 0 {
			return nil, fmt.Errorf("rate_threshold rules require positive window_seconds")
		}
		return evaluateRateThreshold(rule, filter, groupBy, threshold, records), nil
	case "percentile_threshold", "percentile_based":
		threshold, err := parseMetricThreshold(rule.Threshold)
		if err != nil {
			return nil, fmt.Errorf("parse threshold: %w", err)
		}
		if rule.WindowSeconds <= 0 {
			return nil, fmt.Errorf("percentile_threshold rules require positive window_seconds")
		}
		return evaluatePercentileThreshold(rule, filter, groupBy, threshold, records)
	default:
		return nil, fmt.Errorf("unsupported rule_type %q", rule.RuleType)
	}
}

func evaluateCountThreshold(rule Rule, filter Filter, groupBy []string, threshold int, records []Record) []Trigger {
	groups := map[string]*Trigger{}

	for _, record := range records {
		if !matchesFilter(record, filter) {
			continue
		}

		group, groupKey := buildGroup(record, groupBy)
		trigger := groups[groupKey]
		if trigger == nil {
			trigger = &Trigger{
				RuleID:   rule.ID,
				RuleName: rule.Name,
				RuleType: rule.RuleType,
				Severity: rule.Severity,
				GroupKey: groupKey,
				Group:    group,
			}
			groups[groupKey] = trigger
		}
		trigger.MatchCount++
	}

	return collectTriggers(groups, threshold)
}

func evaluatePatternMatch(rule Rule, filter Filter, groupBy []string, threshold int, records []Record) ([]Trigger, error) {
	pattern := strings.TrimSpace(filter.Pattern)
	if pattern == "" {
		return nil, fmt.Errorf("pattern_match rules require filter_json.pattern")
	}

	groups := map[string]*Trigger{}
	for _, record := range records {
		if !matchesFilter(record, filter) {
			continue
		}
		if !matchesPattern(record, filter.Target, pattern) {
			continue
		}

		group, groupKey := buildGroup(record, groupBy)
		trigger := groups[groupKey]
		if trigger == nil {
			trigger = &Trigger{
				RuleID:   rule.ID,
				RuleName: rule.Name,
				RuleType: rule.RuleType,
				Severity: rule.Severity,
				GroupKey: groupKey,
				Group:    group,
			}
			groups[groupKey] = trigger
		}
		trigger.MatchCount++
	}

	return collectTriggers(groups, threshold), nil
}

func evaluateRateThreshold(rule Rule, filter Filter, groupBy []string, threshold float64, records []Record) []Trigger {
	groups := map[string]*Trigger{}
	for _, record := range recordsInWindow(records, rule.WindowSeconds) {
		if !matchesFilter(record, filter) {
			continue
		}
		group, groupKey := buildGroup(record, groupBy)
		trigger := groups[groupKey]
		if trigger == nil {
			trigger = newMetricTrigger(rule, group, groupKey, threshold)
			groups[groupKey] = trigger
		}
		trigger.MatchCount++
	}
	keys := sortedTriggerKeys(groups)
	result := make([]Trigger, 0, len(keys))
	for _, key := range keys {
		trigger := groups[key]
		trigger.MetricValue = float64(trigger.MatchCount) / float64(rule.WindowSeconds)
		if trigger.MetricValue >= threshold {
			result = append(result, *trigger)
		}
	}
	return result
}

type percentileGroup struct {
	trigger *Trigger
	values  []float64
}

func evaluatePercentileThreshold(rule Rule, filter Filter, groupBy []string, threshold float64, records []Record) ([]Trigger, error) {
	valueField := strings.TrimSpace(filter.ValueField)
	if valueField != "raw_size_bytes" && !strings.HasPrefix(valueField, "field.") {
		return nil, fmt.Errorf("percentile_threshold rules require filter_json.value_field as raw_size_bytes or field.<name>")
	}
	if filter.Percentile <= 0 || filter.Percentile >= 100 {
		return nil, fmt.Errorf("percentile_threshold rules require filter_json.percentile between 0 and 100")
	}
	groups := map[string]*percentileGroup{}
	for _, record := range recordsInWindow(records, rule.WindowSeconds) {
		if !matchesFilter(record, filter) {
			continue
		}
		value, ok := numericValue(record, valueField)
		if !ok {
			continue
		}
		group, groupKey := buildGroup(record, groupBy)
		state := groups[groupKey]
		if state == nil {
			trigger := newMetricTrigger(rule, group, groupKey, threshold)
			trigger.Percentile = filter.Percentile
			trigger.ValueField = valueField
			state = &percentileGroup{trigger: trigger}
			groups[groupKey] = state
		}
		state.values = append(state.values, value)
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]Trigger, 0, len(keys))
	for _, key := range keys {
		state := groups[key]
		state.trigger.MatchCount = len(state.values)
		state.trigger.MetricValue = percentile(state.values, filter.Percentile)
		if state.trigger.MetricValue >= threshold {
			result = append(result, *state.trigger)
		}
	}
	return result, nil
}

func newMetricTrigger(rule Rule, group map[string]string, groupKey string, threshold float64) *Trigger {
	return &Trigger{RuleID: rule.ID, RuleName: rule.Name, RuleType: rule.RuleType, Severity: rule.Severity, GroupKey: groupKey, Group: group, Threshold: threshold, WindowSeconds: rule.WindowSeconds}
}

func sortedTriggerKeys(groups map[string]*Trigger) []string {
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func recordsInWindow(records []Record, windowSeconds int) []Record {
	if windowSeconds <= 0 || len(records) == 0 {
		return records
	}
	latest := records[0].Timestamp
	for _, record := range records[1:] {
		if record.Timestamp.After(latest) {
			latest = record.Timestamp
		}
	}
	cutoff := latest.Add(-time.Duration(windowSeconds) * time.Second)
	windowed := make([]Record, 0, len(records))
	for _, record := range records {
		if !record.Timestamp.Before(cutoff) && !record.Timestamp.After(latest) {
			windowed = append(windowed, record)
		}
	}
	return windowed
}

func numericValue(record Record, valueField string) (float64, bool) {
	if valueField == "raw_size_bytes" {
		return float64(record.RawSizeBytes), true
	}
	value, ok := record.Fields[strings.TrimPrefix(valueField, "field.")]
	if !ok {
		return 0, false
	}
	switch typed := value.(type) {
	case float64:
		return typed, !math.IsNaN(typed) && !math.IsInf(typed, 0)
	case float32:
		value := float64(typed)
		return value, !math.IsNaN(value) && !math.IsInf(value, 0)
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil && !math.IsNaN(parsed) && !math.IsInf(parsed, 0)
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil && !math.IsNaN(parsed) && !math.IsInf(parsed, 0)
	default:
		return 0, false
	}
}

func percentile(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if len(sorted) == 1 {
		return sorted[0]
	}
	position := (percentile / 100) * float64(len(sorted)-1)
	lower := int(position)
	upper := lower + 1
	if upper >= len(sorted) {
		return sorted[len(sorted)-1]
	}
	fraction := position - float64(lower)
	return sorted[lower] + fraction*(sorted[upper]-sorted[lower])
}

func collectTriggers(groups map[string]*Trigger, threshold int) []Trigger {
	keys := make([]string, 0, len(groups))
	for key, trigger := range groups {
		if trigger.MatchCount >= threshold {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	triggers := make([]Trigger, 0, len(keys))
	for _, key := range keys {
		triggers = append(triggers, *groups[key])
	}
	return triggers
}

func parseFilter(raw json.RawMessage) (Filter, error) {
	if len(raw) == 0 {
		return Filter{}, nil
	}

	var filter Filter
	if err := json.Unmarshal(raw, &filter); err != nil {
		return Filter{}, err
	}
	if filter.FieldEquals == nil {
		filter.FieldEquals = map[string]string{}
	}
	return filter, nil
}

func parseGroupBy(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	var groupBy []string
	if err := json.Unmarshal(raw, &groupBy); err != nil {
		return nil, err
	}
	return groupBy, nil
}

func parseCountThreshold(raw string) (int, error) {
	parsed, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, err
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("threshold must be positive")
	}
	return parsed, nil
}

func parseMetricThreshold(raw string) (float64, error) {
	parsed, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0, err
	}
	if parsed < 0 || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, fmt.Errorf("threshold must be a finite non-negative number")
	}
	return parsed, nil
}

func matchesFilter(record Record, filter Filter) bool {
	if filter.Level != "" && record.Level != strings.ToLower(strings.TrimSpace(filter.Level)) {
		return false
	}
	if filter.Source != "" && record.Source != strings.ToLower(strings.TrimSpace(filter.Source)) {
		return false
	}
	if filter.Host != "" && record.Host != strings.TrimSpace(filter.Host) {
		return false
	}
	if filter.TraceID != "" && record.TraceID != strings.TrimSpace(filter.TraceID) {
		return false
	}
	if filter.MessageContains != "" && !strings.Contains(strings.ToLower(record.Message), strings.ToLower(strings.TrimSpace(filter.MessageContains))) {
		return false
	}

	for key, value := range filter.FieldEquals {
		if strings.TrimSpace(fmt.Sprint(record.Fields[key])) != strings.TrimSpace(value) {
			return false
		}
	}

	return true
}

func matchesPattern(record Record, target, pattern string) bool {
	value := targetValue(record, target)
	return strings.Contains(strings.ToLower(value), strings.ToLower(pattern))
}

func buildGroup(record Record, groupBy []string) (map[string]string, string) {
	if len(groupBy) == 0 {
		return map[string]string{"scope": "all"}, "scope=all"
	}

	group := make(map[string]string, len(groupBy))
	parts := make([]string, 0, len(groupBy))
	for _, field := range groupBy {
		value := targetValue(record, field)
		group[field] = value
		parts = append(parts, field+"="+value)
	}
	return group, strings.Join(parts, "|")
}

func targetValue(record Record, target string) string {
	target = strings.TrimSpace(target)
	switch {
	case target == "", target == "message":
		return record.Message
	case target == "service":
		return record.Service
	case target == "environment":
		return record.Environment
	case target == "source":
		return record.Source
	case target == "host":
		return record.Host
	case target == "level":
		return record.Level
	case target == "trace_id":
		return record.TraceID
	case target == "fingerprint":
		return record.Fingerprint
	case strings.HasPrefix(target, "field."):
		return strings.TrimSpace(fmt.Sprint(record.Fields[strings.TrimPrefix(target, "field.")]))
	default:
		return ""
	}
}
