package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type requestPayload struct {
	Service string           `json:"service"`
	Env     string           `json:"env"`
	Source  string           `json:"source"`
	Logs    []requestLogItem `json:"logs"`
}

type requestLogItem struct {
	Timestamp string         `json:"timestamp"`
	Level     string         `json:"level"`
	Message   string         `json:"message"`
	Fields    map[string]any `json:"fields,omitempty"`
}

type result struct {
	statusCode int
	duration   time.Duration
	err        error
}

func main() {
	var (
		targetURL   = flag.String("url", "http://localhost:8080/v1/logs", "ingest endpoint URL")
		apiKey      = flag.String("api-key", "local-dev-key", "ingest API key")
		service     = flag.String("service", "checkout", "service name in the payload")
		environment = flag.String("env", "prod", "environment name in the payload")
		source      = flag.String("source", "app", "source tag in the payload")
		bursts      = flag.Int("bursts", 5, "number of bursts to send")
		burstSize   = flag.Int("burst-size", 100, "requests per burst")
		pause       = flag.Duration("pause", 2*time.Second, "pause between bursts")
		concurrency = flag.Int("concurrency", 20, "maximum in-flight requests per burst")
		logsPerReq  = flag.Int("logs-per-request", 25, "number of log records per request")
		timeout     = flag.Duration("timeout", 10*time.Second, "per-request timeout")
	)
	flag.Parse()

	if *bursts <= 0 || *burstSize <= 0 || *concurrency <= 0 || *logsPerReq <= 0 {
		log.Fatal("bursts, burst-size, concurrency, and logs-per-request must be positive")
	}

	client := &http.Client{Timeout: *timeout}
	results := make([]result, 0, (*bursts)*(*burstSize))

	for burst := 0; burst < *bursts; burst++ {
		burstResults, err := runBurst(client, *targetURL, *apiKey, requestPayload{
			Service: *service,
			Env:     *environment,
			Source:  *source,
			Logs:    buildLogs(*logsPerReq, burst),
		}, *burstSize, *concurrency)
		if err != nil {
			log.Fatal(err)
		}
		results = append(results, burstResults...)

		fmt.Printf("burst=%d sent=%d\n", burst+1, len(burstResults))
		if burst < *bursts-1 {
			time.Sleep(*pause)
		}
	}

	printSummary(results)
}

func runBurst(client *http.Client, targetURL, apiKey string, payload requestPayload, total, concurrency int) ([]result, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	results := make([]result, total)
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for i := 0; i < total; i++ {
		wg.Add(1)
		sem <- struct{}{}

		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			start := time.Now()
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, targetURL, bytes.NewReader(encoded))
			if err != nil {
				results[idx] = result{err: err}
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-API-Key", apiKey)

			resp, err := client.Do(req)
			if err != nil {
				results[idx] = result{duration: time.Since(start), err: err}
				return
			}
			_ = resp.Body.Close()
			results[idx] = result{statusCode: resp.StatusCode, duration: time.Since(start)}
		}(i)
	}

	wg.Wait()
	return results, nil
}

func buildLogs(count, burst int) []requestLogItem {
	logs := make([]requestLogItem, 0, count)
	base := time.Now().UTC().Add(time.Duration(burst) * time.Second)
	for i := 0; i < count; i++ {
		logs = append(logs, requestLogItem{
			Timestamp: base.Add(time.Duration(i) * time.Millisecond).Format(time.RFC3339Nano),
			Level:     "error",
			Message:   fmt.Sprintf("database timeout burst=%d item=%d", burst+1, i+1),
			Fields: map[string]any{
				"host":     "loadtest-1",
				"trace_id": fmt.Sprintf("burst-%d-item-%d", burst+1, i+1),
				"region":   "us-west-2",
			},
		})
	}
	return logs
}

func printSummary(results []result) {
	statuses := map[int]*atomic.Int64{}
	var failures atomic.Int64
	latencies := make([]time.Duration, 0, len(results))

	for _, item := range results {
		if item.err != nil {
			failures.Add(1)
			continue
		}
		latencies = append(latencies, item.duration)
		counter := statuses[item.statusCode]
		if counter == nil {
			counter = &atomic.Int64{}
			statuses[item.statusCode] = counter
		}
		counter.Add(1)
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	fmt.Printf("requests=%d transport_failures=%d\n", len(results), failures.Load())
	statusCodes := make([]int, 0, len(statuses))
	for code := range statuses {
		statusCodes = append(statusCodes, code)
	}
	sort.Ints(statusCodes)
	for _, code := range statusCodes {
		fmt.Printf("status_%d=%d\n", code, statuses[code].Load())
	}
	if len(latencies) == 0 {
		return
	}
	fmt.Printf("latency_p50=%s\n", percentile(latencies, 0.50))
	fmt.Printf("latency_p95=%s\n", percentile(latencies, 0.95))
	fmt.Printf("latency_p99=%s\n", percentile(latencies, 0.99))
	fmt.Printf("latency_max=%s\n", latencies[len(latencies)-1])
}

func percentile(values []time.Duration, pct float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1) * pct)
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}
