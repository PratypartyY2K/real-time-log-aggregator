package worker

import (
	"context"
	"log/slog"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/app"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/logging"
)

type Runner func(context.Context, app.Logger) error

type Service struct {
	cfg    config.Config
	logger *slog.Logger
	run    Runner
}

func New(cfg config.Config, runner Runner) *Service {
	return &Service{
		cfg:    cfg,
		logger: logging.New(cfg.LogLevel),
		run:    runner,
	}
}

func (s *Service) Run(ctx context.Context) error {
	s.logger.Info("worker service starting", "service", s.cfg.ServiceName)
	return s.run(ctx, s.logger)
}
