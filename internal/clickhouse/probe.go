package clickhouse

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func Probe(ctx context.Context, url string, client *http.Client) error {
	url = strings.TrimSpace(url)
	if url == "" {
		return fmt.Errorf("clickhouse url is not configured")
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader("SELECT 1 FORMAT JSON"))
	if err != nil {
		return fmt.Errorf("build clickhouse readiness request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("probe clickhouse: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		payload, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return fmt.Errorf("clickhouse probe failed with status %s", resp.Status)
		}
		return fmt.Errorf("clickhouse probe failed with status %s: %s", resp.Status, strings.TrimSpace(string(payload)))
	}

	return nil
}
