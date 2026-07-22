package alerts

import (
	"fmt"
	"sync"
	"time"
)

// Engine retains a bounded, per-rule event-time window across processor batches.
// A processor replay is filtered by ingest id before reaching the engine.
type Engine struct {
	mu      sync.Mutex
	windows map[string][]Record
}

func NewEngine() *Engine { return &Engine{windows: map[string][]Record{}} }

func (e *Engine) Evaluate(rule Rule, records []Record) ([]Trigger, error) {
	if e == nil || rule.WindowSeconds <= 0 {
		return Evaluate(rule, records)
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	key := engineRuleKey(rule, records)
	existing := e.windows[key]
	existingIngestIDs := make(map[string]struct{})
	for _, record := range existing {
		if record.IngestID != "" {
			existingIngestIDs[record.IngestID] = struct{}{}
		}
	}
	combined := append([]Record(nil), existing...)
	for _, record := range records {
		if record.IngestID != "" {
			if _, duplicate := existingIngestIDs[record.IngestID]; duplicate {
				continue
			}
		}
		combined = append(combined, record)
	}
	latest := latestRecordTime(combined)
	cutoff := latest.Add(-time.Duration(rule.WindowSeconds) * time.Second)
	window := combined[:0]
	for _, record := range combined {
		if !record.Timestamp.Before(cutoff) && !record.Timestamp.After(latest) {
			window = append(window, record)
		}
	}
	e.windows[key] = append([]Record(nil), window...)
	return Evaluate(rule, window)
}

func engineRuleKey(rule Rule, records []Record) string {
	tenantID := rule.TenantID
	if tenantID == 0 && len(records) > 0 {
		tenantID = int64(records[0].TenantID)
	}
	return fmt.Sprintf("%d:%d", tenantID, rule.ID)
}

func latestRecordTime(records []Record) time.Time {
	if len(records) == 0 {
		return time.Time{}
	}
	latest := records[0].Timestamp
	for _, record := range records[1:] {
		if record.Timestamp.After(latest) {
			latest = record.Timestamp
		}
	}
	return latest
}
