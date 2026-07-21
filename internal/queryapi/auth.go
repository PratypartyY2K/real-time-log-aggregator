package queryapi

import (
	"context"
	"net/http"
	"strings"
)

const apiKeyHeader = "X-API-Key"

type TenantResolver interface {
	ResolveTenant(context.Context, string) (uint64, bool, error)
}

type tenantContextKey struct{}

func WithTenantID(ctx context.Context, tenantID uint64) context.Context {
	return context.WithValue(ctx, tenantContextKey{}, tenantID)
}

func TenantIDFromContext(ctx context.Context) (uint64, bool) {
	tenantID, ok := ctx.Value(tenantContextKey{}).(uint64)
	return tenantID, ok && tenantID > 0
}

func TenantAuthMiddleware(resolver TenantResolver, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := strings.TrimSpace(r.Header.Get(apiKeyHeader))
		if apiKey == "" {
			writeError(w, http.StatusUnauthorized, "missing api key")
			return
		}
		if resolver == nil {
			writeError(w, http.StatusServiceUnavailable, "api key authenticator unavailable")
			return
		}
		tenantID, valid, err := resolver.ResolveTenant(r.Context(), apiKey)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "failed to validate api key")
			return
		}
		if !valid {
			writeError(w, http.StatusUnauthorized, "invalid api key")
			return
		}
		next.ServeHTTP(w, r.WithContext(WithTenantID(r.Context(), tenantID)))
	})
}
