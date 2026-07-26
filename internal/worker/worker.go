package worker

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/logging"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/metrics"
)

type Runner func(context.Context, app.Logger) error

type Service struct {
	cfg            config.Config
	logger         *slog.Logger
	run            Runner
	metricsHandler http.Handler
}

func New(cfg config.Config, runner Runner, metricsHandler http.Handler) *Service {
	return &Service{
		cfg:            cfg,
		logger:         logging.New(cfg.LogLevel),
		run:            runner,
		metricsHandler: metricsHandler,
	}
}

func (s *Service) Run(ctx context.Context) error {
	s.logger.Info("worker service starting", "service", s.cfg.ServiceName)
	metrics.StartServer(ctx, s.cfg.MetricsAddr, logging.Middleware(s.logger, s.cfg.ServiceName, s.metricsHandler), func(err error) {
		s.logger.Error("worker metrics server failed", "service", s.cfg.ServiceName, "addr", s.cfg.MetricsAddr, "error", err)
	})
	return s.run(ctx, s.logger)
}
