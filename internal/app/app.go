package app

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/logging"
)

type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

type Service interface {
	Run(context.Context) error
}

type HTTPService struct {
	cfg    config.Config
	logger *slog.Logger
	server *http.Server
}

func NewHTTPService(cfg config.Config, handler http.Handler) *HTTPService {
	logger := logging.New(cfg.LogLevel)
	return &HTTPService{
		cfg:    cfg,
		logger: logger,
		server: &http.Server{
			Addr:              cfg.HTTPAddr,
			Handler:           logging.Middleware(logger, cfg.ServiceName, handler),
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
}

func (s *HTTPService) Run(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		s.logger.Info("http service starting", "service", s.cfg.ServiceName, "addr", s.cfg.HTTPAddr)
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.logger.Info("http service shutting down", "service", s.cfg.ServiceName)
		return s.server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func Run(service Service) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := service.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
