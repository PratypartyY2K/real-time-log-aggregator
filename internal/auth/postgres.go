package auth

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
)

type PostgresAuthenticator struct {
	db *sql.DB
}

func NewPostgresAuthenticator(db *sql.DB) *PostgresAuthenticator {
	return &PostgresAuthenticator{db: db}
}

func (a *PostgresAuthenticator) Authenticate(ctx context.Context, apiKey string) (bool, error) {
	const stmt = `
UPDATE api_keys
SET last_used_at = NOW()
WHERE key_hash = $1
  AND status = 'active'
RETURNING id
`

	var id int64
	err := a.db.QueryRowContext(ctx, stmt, hashAPIKey(apiKey)).Scan(&id)
	if err == nil {
		return true, nil
	}
	if err == sql.ErrNoRows {
		return false, nil
	}
	return false, fmt.Errorf("query api key: %w", err)
}

func hashAPIKey(apiKey string) string {
	sum := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(sum[:])
}
