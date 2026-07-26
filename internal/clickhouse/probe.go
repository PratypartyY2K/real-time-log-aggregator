package clickhouse

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/logging"
)

func Do(ctx context.Context, client *http.Client, url, query string) (io.ReadCloser, error) {
	url = strings.TrimSpace(url)
	if url == "" {
		return nil, fmt.Errorf("clickhouse url is not configured")
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(query))
	if err != nil {
		return nil, fmt.Errorf("build clickhouse request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	logging.PropagateContext(ctx, req.Header)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("run clickhouse query: %w", err)
	}
	if resp.StatusCode >= http.StatusBadRequest {
		defer resp.Body.Close()
		payload, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return nil, fmt.Errorf("clickhouse returned %s", resp.Status)
		}
		return nil, fmt.Errorf("clickhouse returned %s: %s", resp.Status, strings.TrimSpace(string(payload)))
	}
	return resp.Body, nil
}

func Probe(ctx context.Context, url string, client *http.Client) error {
	body, err := Do(ctx, client, url, "SELECT 1 FORMAT JSON")
	if err != nil {
		return fmt.Errorf("probe clickhouse: %w", err)
	}
	return body.Close()
}
