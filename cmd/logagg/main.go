package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, http.DefaultClient); err != nil {
		fmt.Fprintln(os.Stderr, "logagg:", err)
		os.Exit(1)
	}
}

func run(args []string, stdin io.Reader, stdout io.Writer, client *http.Client) error {
	if len(args) == 0 {
		return errors.New("usage: logagg <ingest|query> [flags]")
	}
	switch args[0] {
	case "ingest":
		return runIngest(args[1:], stdin, stdout, client)
	case "query":
		return runQuery(args[1:], stdout, client)
	case "help", "-h", "--help":
		_, _ = fmt.Fprintln(stdout, "usage: logagg <ingest|query> [flags]")
		return nil
	default:
		return fmt.Errorf("unknown command %q; expected ingest or query", args[0])
	}
}

func runIngest(args []string, stdin io.Reader, stdout io.Writer, client *http.Client) error {
	flags := flag.NewFlagSet("ingest", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	endpoint := flags.String("url", "http://localhost:8080/v1/logs", "ingest endpoint")
	apiKey := flags.String("api-key", envOrDefault("LOGAGG_API_KEY", "local-dev-key"), "API key")
	file := flags.String("file", "-", "JSON batch file, or - for stdin")
	if err := flags.Parse(args); err != nil {
		return err
	}

	var body []byte
	var err error
	if *file == "-" {
		body, err = io.ReadAll(stdin)
	} else {
		body, err = os.ReadFile(*file)
	}
	if err != nil {
		return fmt.Errorf("read ingest payload: %w", err)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return errors.New("ingest payload is empty")
	}
	return request(client, http.MethodPost, *endpoint, *apiKey, bytes.NewReader(body), stdout)
}

func runQuery(args []string, stdout io.Writer, client *http.Client) error {
	flags := flag.NewFlagSet("query", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	endpoint := flags.String("url", "http://localhost:8081/v1/logs", "query endpoint")
	apiKey := flags.String("api-key", envOrDefault("LOGAGG_API_KEY", "local-dev-key"), "API key")
	start := flags.String("start", time.Now().UTC().Add(-time.Hour).Format(time.RFC3339), "RFC3339 range start")
	end := flags.String("end", time.Now().UTC().Format(time.RFC3339), "RFC3339 range end")
	service := flags.String("service", "", "service filter")
	level := flags.String("level", "", "level filter")
	traceID := flags.String("trace-id", "", "trace ID filter")
	limit := flags.Int("limit", 100, "maximum records")
	stream := flags.Bool("stream", false, "return newline-delimited JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *limit <= 0 {
		return errors.New("limit must be positive")
	}

	parsed, err := url.Parse(*endpoint)
	if err != nil {
		return fmt.Errorf("parse query URL: %w", err)
	}
	values := parsed.Query()
	values.Set("start", *start)
	values.Set("end", *end)
	values.Set("limit", fmt.Sprint(*limit))
	addQueryValue(values, "service", *service)
	addQueryValue(values, "level", *level)
	addQueryValue(values, "trace_id", *traceID)
	if *stream {
		values.Set("stream", "true")
	}
	parsed.RawQuery = values.Encode()
	return request(client, http.MethodGet, parsed.String(), *apiKey, nil, stdout)
}

func request(client *http.Client, method, endpoint, apiKey string, body io.Reader, stdout io.Writer) error {
	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("X-API-Key", strings.TrimSpace(apiKey))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(payload)))
	}
	if _, err := stdout.Write(payload); err != nil {
		return fmt.Errorf("write response: %w", err)
	}
	return nil
}

func addQueryValue(values url.Values, key, value string) {
	if value = strings.TrimSpace(value); value != "" {
		values.Set(key, value)
	}
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
