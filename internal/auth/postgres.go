package auth

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/ingest"
)

// ResolveTenant validates an active API key and returns its tenant scope.
func (a *PostgresAuthenticator) ResolveTenant(ctx context.Context, apiKey string) (uint64, bool, error) {
	key, err := a.lookupAPIKey(ctx, apiKey)
	if err != nil {
		return 0, false, err
	}
	if key == nil || key.tenantID <= 0 {
		return 0, false, nil
	}
	if err := a.touchAPIKey(ctx, key.id); err != nil {
		return 0, false, err
	}
	return uint64(key.tenantID), true, nil
}

type PostgresAuthenticator struct {
	db *sql.DB
}

type apiKeyRecord struct {
	id              int64
	tenantID        int64
	serviceID       sql.NullInt64
	rateLimitPerSec int
}

func NewPostgresAuthenticator(db *sql.DB) *PostgresAuthenticator {
	return &PostgresAuthenticator{db: db}
}

func (a *PostgresAuthenticator) Authorize(ctx context.Context, apiKey string, req ingest.BatchRequest) (ingest.Authorization, error) {
	key, err := a.lookupAPIKey(ctx, apiKey)
	if err != nil {
		return ingest.Authorization{}, err
	}
	if key == nil {
		return ingest.Authorization{Decision: ingest.AuthorizationInvalid}, nil
	}

	serviceID, ok, err := a.lookupAuthorizedService(ctx, *key, req.Service, req.Env)
	if err != nil {
		return ingest.Authorization{}, err
	}
	if !ok {
		return ingest.Authorization{Decision: ingest.AuthorizationForbidden, TenantID: key.tenantID}, nil
	}
	if err := a.touchAPIKey(ctx, key.id); err != nil {
		return ingest.Authorization{}, err
	}

	return ingest.Authorization{
		Decision:        ingest.AuthorizationAllowed,
		APIKeyID:        key.id,
		TenantID:        key.tenantID,
		ServiceID:       serviceID,
		RateLimitPerSec: key.rateLimitPerSec,
	}, nil
}

func (a *PostgresAuthenticator) lookupAPIKey(ctx context.Context, apiKey string) (*apiKeyRecord, error) {
	const stmt = `
SELECT id, tenant_id, service_id, rate_limit_per_sec
FROM api_keys
WHERE key_hash = $1
  AND status = 'active'
`

	var key apiKeyRecord
	err := a.db.QueryRowContext(ctx, stmt, hashAPIKey(apiKey)).Scan(&key.id, &key.tenantID, &key.serviceID, &key.rateLimitPerSec)
	if err == nil {
		return &key, nil
	}
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return nil, fmt.Errorf("query api key: %w", err)
}

func (a *PostgresAuthenticator) lookupAuthorizedService(ctx context.Context, key apiKeyRecord, serviceName, environment string) (int64, bool, error) {
	const stmt = `
SELECT id
FROM services
WHERE tenant_id = $1
  AND name = $2
  AND environment = $3
  AND ($4::BIGINT IS NULL OR id = $4)
`

	var serviceID int64
	err := a.db.QueryRowContext(ctx, stmt, key.tenantID, serviceName, environment, key.serviceID).Scan(&serviceID)
	if err == nil {
		return serviceID, true, nil
	}
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	return 0, false, fmt.Errorf("query authorized service: %w", err)
}

func (a *PostgresAuthenticator) touchAPIKey(ctx context.Context, apiKeyID int64) error {
	const stmt = `
UPDATE api_keys
SET last_used_at = NOW()
WHERE id = $1
`

	if _, err := a.db.ExecContext(ctx, stmt, apiKeyID); err != nil {
		return fmt.Errorf("update api key last_used_at: %w", err)
	}
	return nil
}

func hashAPIKey(apiKey string) string {
	sum := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(sum[:])
}
