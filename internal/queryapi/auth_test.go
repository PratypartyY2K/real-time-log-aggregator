package queryapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type stubTenantResolver struct {
	tenantID uint64
	valid    bool
	err      error
}

func (s stubTenantResolver) ResolveTenant(context.Context, string) (uint64, bool, error) {
	return s.tenantID, s.valid, s.err
}

func TestTenantAuthMiddlewareAddsTenantIdentity(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantID, ok := TenantIDFromContext(r.Context())
		if !ok || tenantID != 42 {
			t.Fatalf("unexpected tenant identity: %d, %v", tenantID, ok)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/logs", nil)
	req.Header.Set("X-API-Key", "secret")
	rec := httptest.NewRecorder()

	TenantAuthMiddleware(stubTenantResolver{tenantID: 42, valid: true}, next).ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}
}

func TestTenantAuthMiddlewareRejectsMissingOrInvalidKey(t *testing.T) {
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("protected handler must not be called")
	})

	for _, tc := range []struct {
		name     string
		resolver TenantResolver
		apiKey   string
		want     int
	}{
		{name: "missing", resolver: stubTenantResolver{tenantID: 1, valid: true}, want: http.StatusUnauthorized},
		{name: "invalid", resolver: stubTenantResolver{}, apiKey: "bad", want: http.StatusUnauthorized},
		{name: "resolver failure", resolver: stubTenantResolver{err: errors.New("postgres unavailable")}, apiKey: "key", want: http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/logs", nil)
			req.Header.Set("X-API-Key", tc.apiKey)
			rec := httptest.NewRecorder()
			TenantAuthMiddleware(tc.resolver, next).ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("expected %d, got %d", tc.want, rec.Code)
			}
		})
	}
}
