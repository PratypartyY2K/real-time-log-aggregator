package alerts

import (
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
)

func TestScanRuleMapsFields(t *testing.T) {
	t.Parallel()

	rule, err := scanRule(stubScanner{
		values: []any{
			int64(7),
			int64(42),
			sql.NullInt64{Int64: 13, Valid: true},
			sql.NullString{String: "checkout", Valid: true},
			sql.NullString{String: "prod", Valid: true},
			"checkout",
			"error",
			"fp-123",
			sql.NullString{String: "ops@example.com", Valid: true},
			"error spike",
			"threshold",
			"critical",
			[]byte(`{"level":"error"}`),
			[]byte(`["service"]`),
			300,
			"25",
			600,
			"active",
		},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if rule.ID != 7 || rule.TenantID != 42 || !rule.ServiceID.Valid || rule.ServiceID.Int64 != 13 {
		t.Fatalf("unexpected ids: %+v", rule)
	}
	if !rule.ServiceName.Valid || rule.ServiceName.String != "checkout" || !rule.Environment.Valid || rule.Environment.String != "prod" || !rule.Owner.Valid || rule.Owner.String != "ops@example.com" {
		t.Fatalf("unexpected service scope: %+v", rule)
	}
	if rule.Service != "checkout" || rule.LogLevel != "error" || rule.Fingerprint != "fp-123" {
		t.Fatalf("unexpected explicit rule model fields: %+v", rule)
	}
	if rule.Name != "error spike" || rule.RuleType != "threshold" || rule.Severity != "critical" {
		t.Fatalf("unexpected rule metadata: %+v", rule)
	}
	if string(rule.FilterJSON) != `{"level":"error"}` {
		t.Fatalf("unexpected filter json: %s", rule.FilterJSON)
	}
	if string(rule.GroupByJSON) != `["service"]` {
		t.Fatalf("unexpected group_by json: %s", rule.GroupByJSON)
	}
	if rule.WindowSeconds != 300 || rule.Threshold != "25" || rule.CooldownSeconds != 600 || rule.Status != "active" {
		t.Fatalf("unexpected rule timing fields: %+v", rule)
	}
	if !json.Valid(rule.FilterJSON) || !json.Valid(rule.GroupByJSON) {
		t.Fatalf("expected valid json payloads: %+v", rule)
	}
}

func TestScanRuleReturnsScanError(t *testing.T) {
	t.Parallel()

	_, err := scanRule(stubScanner{err: errors.New("scan failed")})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

type stubScanner struct {
	values []any
	err    error
}

func (s stubScanner) Scan(dest ...any) error {
	if s.err != nil {
		return s.err
	}
	for i := range dest {
		switch target := dest[i].(type) {
		case *int64:
			*target = s.values[i].(int64)
		case *int:
			*target = s.values[i].(int)
		case *string:
			*target = s.values[i].(string)
		case *sql.NullInt64:
			*target = s.values[i].(sql.NullInt64)
		case *sql.NullString:
			*target = s.values[i].(sql.NullString)
		case *json.RawMessage:
			*target = append((*target)[:0], s.values[i].([]byte)...)
		default:
			return errors.New("unsupported scan target")
		}
	}
	return nil
}
