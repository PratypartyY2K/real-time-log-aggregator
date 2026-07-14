package readiness

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const defaultTimeout = 2 * time.Second

type Checker interface {
	Name() string
	Check(context.Context) error
}

type Handler struct {
	checkers []Checker
	timeout  time.Duration
}

type checkResult struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type response struct {
	Status string        `json:"status"`
	Checks []checkResult `json:"checks"`
}

func NewHandler(checkers ...Checker) *Handler {
	return &Handler{
		checkers: checkers,
		timeout:  defaultTimeout,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if h.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.timeout)
		defer cancel()
	}

	resp := response{
		Status: "ready",
		Checks: make([]checkResult, 0, len(h.checkers)),
	}

	httpStatus := http.StatusOK
	for _, checker := range h.checkers {
		if checker == nil {
			continue
		}
		result := checkResult{
			Name:   checker.Name(),
			Status: "ok",
		}
		if err := checker.Check(ctx); err != nil {
			result.Status = "error"
			result.Error = err.Error()
			resp.Status = "not_ready"
			httpStatus = http.StatusServiceUnavailable
		}
		resp.Checks = append(resp.Checks, result)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(resp)
}

type funcChecker struct {
	name string
	fn   func(context.Context) error
}

func Func(name string, fn func(context.Context) error) Checker {
	return funcChecker{name: name, fn: fn}
}

func (c funcChecker) Name() string {
	return c.name
}

func (c funcChecker) Check(ctx context.Context) error {
	if c.fn == nil {
		return fmt.Errorf("checker is not configured")
	}
	return c.fn(ctx)
}

func PostgresChecker(name string, db *sql.DB) Checker {
	return Func(name, func(ctx context.Context) error {
		if db == nil {
			return fmt.Errorf("postgres db is nil")
		}
		if err := db.PingContext(ctx); err != nil {
			return fmt.Errorf("ping postgres: %w", err)
		}
		return nil
	})
}
