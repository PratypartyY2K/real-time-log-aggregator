package alerts

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Record struct {
	Timestamp   time.Time
	TenantID    uint64
	Service     string
	Environment string
	Source      string
	Host        string
	Level       string
	TraceID     string
	Fingerprint string
	Message     string
	Fields      map[string]any
	IngestID    string
}

type Trigger struct {
	RuleID     int64
	RuleName   string
	RuleType   string
	Severity   string
	GroupKey   string
	Group      map[string]string
	MatchCount int
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

	threshold, err := parseThreshold(rule.Threshold)
	if err != nil {
		return nil, fmt.Errorf("parse threshold: %w", err)
	}

	switch strings.ToLower(strings.TrimSpace(rule.RuleType)) {
	case "count_threshold":
		return evaluateCountThreshold(rule, filter, groupBy, threshold, records), nil
	case "pattern_match":
		return evaluatePatternMatch(rule, filter, groupBy, threshold, records)
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

func parseThreshold(raw string) (int, error) {
	parsed, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, err
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("threshold must be positive")
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
