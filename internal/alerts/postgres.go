package alerts

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

type Rule struct {
	ID              int64
	TenantID        int64
	ServiceID       sql.NullInt64
	ServiceName     sql.NullString
	Environment     sql.NullString
	Owner           sql.NullString
	Name            string
	RuleType        string
	Severity        string
	FilterJSON      json.RawMessage
	GroupByJSON     json.RawMessage
	WindowSeconds   int
	Threshold       string
	CooldownSeconds int
	Status          string
}

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) LoadActiveRules(ctx context.Context, tenantID uint64, service, environment string) ([]Rule, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("alert rule store is not configured")
	}

	const stmt = `
SELECT
    ar.id,
    ar.tenant_id,
    ar.service_id,
    svc.name,
    svc.environment,
    svc.owner,
    ar.name,
    ar.rule_type,
    ar.severity,
    ar.filter_json::text,
    ar.group_by::text,
    ar.window_seconds,
    ar.threshold::text,
    ar.cooldown_seconds,
    ar.status
FROM alert_rules ar
LEFT JOIN services svc
    ON svc.id = ar.service_id
WHERE ar.tenant_id = $1
  AND ar.status = 'active'
  AND (
      ar.service_id IS NULL
      OR (svc.name = $2 AND svc.environment = $3)
  )
ORDER BY ar.id ASC
`

	rows, err := s.db.QueryContext(ctx, stmt, tenantID, service, environment)
	if err != nil {
		return nil, fmt.Errorf("query active alert rules: %w", err)
	}
	defer rows.Close()

	rules := make([]Rule, 0)
	for rows.Next() {
		rule, err := scanRule(rows)
		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate alert rules: %w", err)
	}

	return rules, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRule(row rowScanner) (Rule, error) {
	var rule Rule
	if err := row.Scan(
		&rule.ID,
		&rule.TenantID,
		&rule.ServiceID,
		&rule.ServiceName,
		&rule.Environment,
		&rule.Owner,
		&rule.Name,
		&rule.RuleType,
		&rule.Severity,
		&rule.FilterJSON,
		&rule.GroupByJSON,
		&rule.WindowSeconds,
		&rule.Threshold,
		&rule.CooldownSeconds,
		&rule.Status,
	); err != nil {
		return Rule{}, fmt.Errorf("scan alert rule: %w", err)
	}
	return rule, nil
}
