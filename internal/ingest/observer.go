package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	stdstrconv "strconv"
	"strings"
	"sync/atomic"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/logging"
	commonmetrics "github.com/PratypartyY2K/real-time-log-aggregator/internal/metrics"
)

type AuthOutcome string

const (
	AuthOutcomeAuthorized           AuthOutcome = "authorized"
	AuthOutcomeMissingAPIKey        AuthOutcome = "missing_api_key"
	AuthOutcomeInvalidAPIKey        AuthOutcome = "invalid_api_key"
	AuthOutcomeForbiddenScope       AuthOutcome = "forbidden_scope"
	AuthOutcomeBackendError         AuthOutcome = "backend_error"
	AuthOutcomeAuthenticatorMissing AuthOutcome = "authenticator_unavailable"
	AuthOutcomeRateLimited          AuthOutcome = "rate_limited"
	AuthOutcomeRequestBodyTooLarge  AuthOutcome = "request_body_too_large"
	AuthOutcomeInvalidRequestBody   AuthOutcome = "invalid_request_body"
	AuthOutcomeBatchTooLarge        AuthOutcome = "batch_too_large"
)

type AuthObservation struct {
	Outcome   AuthOutcome
	APIKeyID  int64
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
	RateLimited          int64 `json:"rate_limited"`
	RequestBodyTooLarge  int64 `json:"request_body_too_large"`
	InvalidRequestBody   int64 `json:"invalid_request_body"`
	BatchTooLarge        int64 `json:"batch_too_large"`
}

type MetricsObserver struct {
	logger               *slog.Logger
	authorized           atomic.Int64
	missingAPIKey        atomic.Int64
	invalidAPIKey        atomic.Int64
	forbiddenScope       atomic.Int64
	backendError         atomic.Int64
	authenticatorMissing atomic.Int64
	rateLimited          atomic.Int64
	requestBodyTooLarge  atomic.Int64
	invalidRequestBody   atomic.Int64
	batchTooLarge        atomic.Int64
}

func NewMetricsObserver(logger *slog.Logger) *MetricsObserver {
	return &MetricsObserver{logger: logger}
}

func (o *MetricsObserver) ObserveAuth(ctx context.Context, obs AuthObservation) {
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
	case AuthOutcomeRateLimited:
		o.rateLimited.Add(1)
	case AuthOutcomeRequestBodyTooLarge:
		o.requestBodyTooLarge.Add(1)
	case AuthOutcomeInvalidRequestBody:
		o.invalidRequestBody.Add(1)
	case AuthOutcomeBatchTooLarge:
		o.batchTooLarge.Add(1)
	}

	if o.logger == nil {
		return
	}

	level := slog.LevelInfo
	if obs.Outcome != AuthOutcomeAuthorized {
		level = slog.LevelWarn
	}

	o.logger.Log(
		ctx,
		level,
		"ingest auth outcome",
		"request_id", logging.RequestIDFromContext(ctx),
		"outcome", string(obs.Outcome),
		"api_key_id", obs.APIKeyID,
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
		RateLimited:          o.rateLimited.Load(),
		RequestBodyTooLarge:  o.requestBodyTooLarge.Load(),
		InvalidRequestBody:   o.invalidRequestBody.Load(),
		BatchTooLarge:        o.batchTooLarge.Load(),
	}
}

func (o *MetricsObserver) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(o.Snapshot())
}

func (o *MetricsObserver) WritePrometheus(body *strings.Builder) {
	if o == nil {
		return
	}

	commonmetrics.WriteMetricHelp(body, "logagg_ingest_auth_outcomes_total", "Total ingest auth and validation outcomes.", "counter")

	for _, sample := range []struct {
		outcome string
		value   int64
	}{
		{outcome: string(AuthOutcomeAuthorized), value: o.authorized.Load()},
		{outcome: string(AuthOutcomeMissingAPIKey), value: o.missingAPIKey.Load()},
		{outcome: string(AuthOutcomeInvalidAPIKey), value: o.invalidAPIKey.Load()},
		{outcome: string(AuthOutcomeForbiddenScope), value: o.forbiddenScope.Load()},
		{outcome: string(AuthOutcomeBackendError), value: o.backendError.Load()},
		{outcome: string(AuthOutcomeAuthenticatorMissing), value: o.authenticatorMissing.Load()},
		{outcome: string(AuthOutcomeRateLimited), value: o.rateLimited.Load()},
		{outcome: string(AuthOutcomeRequestBodyTooLarge), value: o.requestBodyTooLarge.Load()},
		{outcome: string(AuthOutcomeInvalidRequestBody), value: o.invalidRequestBody.Load()},
		{outcome: string(AuthOutcomeBatchTooLarge), value: o.batchTooLarge.Load()},
	} {
		commonmetrics.WriteMetricLine(body, "logagg_ingest_auth_outcomes_total", map[string]string{
			"outcome": sample.outcome,
		}, stdstrconv.FormatInt(sample.value, 10))
	}
}
