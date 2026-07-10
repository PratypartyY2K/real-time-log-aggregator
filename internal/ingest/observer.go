package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync/atomic"
)

type AuthOutcome string

const (
	AuthOutcomeAuthorized           AuthOutcome = "authorized"
	AuthOutcomeMissingAPIKey        AuthOutcome = "missing_api_key"
	AuthOutcomeInvalidAPIKey        AuthOutcome = "invalid_api_key"
	AuthOutcomeForbiddenScope       AuthOutcome = "forbidden_scope"
	AuthOutcomeBackendError         AuthOutcome = "backend_error"
	AuthOutcomeAuthenticatorMissing AuthOutcome = "authenticator_unavailable"
)

type AuthObservation struct {
	Outcome   AuthOutcome
	Service   string
	Env       string
	TenantID  int64
	ServiceID int64
}

type Observer interface {
	ObserveAuth(context.Context, AuthObservation)
}

type MetricsSnapshot struct {
	Authorized           int64 `json:"authorized"`
	MissingAPIKey        int64 `json:"missing_api_key"`
	InvalidAPIKey        int64 `json:"invalid_api_key"`
	ForbiddenScope       int64 `json:"forbidden_scope"`
	BackendError         int64 `json:"backend_error"`
	AuthenticatorMissing int64 `json:"authenticator_unavailable"`
}

type MetricsObserver struct {
	logger               *slog.Logger
	authorized           atomic.Int64
	missingAPIKey        atomic.Int64
	invalidAPIKey        atomic.Int64
	forbiddenScope       atomic.Int64
	backendError         atomic.Int64
	authenticatorMissing atomic.Int64
}

func NewMetricsObserver(logger *slog.Logger) *MetricsObserver {
	return &MetricsObserver{logger: logger}
}

func (o *MetricsObserver) ObserveAuth(_ context.Context, obs AuthObservation) {
	switch obs.Outcome {
	case AuthOutcomeAuthorized:
		o.authorized.Add(1)
	case AuthOutcomeMissingAPIKey:
		o.missingAPIKey.Add(1)
	case AuthOutcomeInvalidAPIKey:
		o.invalidAPIKey.Add(1)
	case AuthOutcomeForbiddenScope:
		o.forbiddenScope.Add(1)
	case AuthOutcomeBackendError:
		o.backendError.Add(1)
	case AuthOutcomeAuthenticatorMissing:
		o.authenticatorMissing.Add(1)
	}

	if o.logger == nil {
		return
	}

	level := slog.LevelInfo
	if obs.Outcome != AuthOutcomeAuthorized {
		level = slog.LevelWarn
	}

	o.logger.Log(
		context.Background(),
		level,
		"ingest auth outcome",
		"outcome", string(obs.Outcome),
		"service", obs.Service,
		"env", obs.Env,
		"tenant_id", obs.TenantID,
		"service_id", obs.ServiceID,
	)
}

func (o *MetricsObserver) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		Authorized:           o.authorized.Load(),
		MissingAPIKey:        o.missingAPIKey.Load(),
		InvalidAPIKey:        o.invalidAPIKey.Load(),
		ForbiddenScope:       o.forbiddenScope.Load(),
		BackendError:         o.backendError.Load(),
		AuthenticatorMissing: o.authenticatorMissing.Load(),
	}
}

func (o *MetricsObserver) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(o.Snapshot())
}
