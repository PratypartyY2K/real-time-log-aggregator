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
	"strings"
	"sync"
	"time"
)

type requestPayload struct {
	SchemaVersion string           `json:"schema_version"`
	Service       string           `json:"service"`
	Env           string           `json:"env"`
	Source        string           `json:"source"`
	Logs          []requestLogItem `json:"logs"`
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

type summary struct {
	requests   int
	successes  int
	failures   int
	duration   time.Duration
	latencyP50 time.Duration
	latencyP95 time.Duration
	latencyP99 time.Duration
	latencyMax time.Duration
}

func main() {
	var (
		targetURL   = flag.String("url", "http://localhost:8080/v1/logs", "ingest endpoint URL")
		apiKey      = flag.String("api-key", "local-dev-key", "ingest API key")
		service     = flag.String("service", "checkout", "service name in the payload")
		environment = flag.String("env", "prod", "environment name in the payload")
		source      = flag.String("source", "app", "source tag in the payload")
		mode        = flag.String("mode", "burst", "load profile: burst or sustained")
		bursts      = flag.Int("bursts", 5, "number of bursts to send")
		burstSize   = flag.Int("burst-size", 100, "requests per burst")
		pause       = flag.Duration("pause", 2*time.Second, "pause between bursts")
		duration    = flag.Duration("duration", time.Minute, "sustained test duration")
		rate        = flag.Int("rate", 100, "target requests per second in sustained mode")
		concurrency = flag.Int("concurrency", 20, "maximum in-flight requests per burst")
		logsPerReq  = flag.Int("logs-per-request", 25, "number of log records per request")
		timeout     = flag.Duration("timeout", 10*time.Second, "per-request timeout")
		maxError    = flag.Float64("max-error-rate", 0.01, "fail when the non-2xx/transport error ratio exceeds this value")
		maxP95      = flag.Duration("max-p95", 0, "optional p95 latency failure threshold (zero disables)")
	)
	flag.Parse()

	if *concurrency <= 0 || *logsPerReq <= 0 || *maxError < 0 || *maxError > 1 {
		log.Fatal("concurrency and logs-per-request must be positive; max-error-rate must be between 0 and 1")
	}

	client := &http.Client{Timeout: *timeout}
	payload := requestPayload{
		SchemaVersion: "logs.ingest.v1",
		Service:       *service,
		Env:           *environment,
		Source:        *source,
	}
	started := time.Now()
	var results []result
	var err error
	switch strings.ToLower(strings.TrimSpace(*mode)) {
	case "burst":
		if *bursts <= 0 || *burstSize <= 0 {
			log.Fatal("bursts and burst-size must be positive")
		}
		results, err = runBursts(client, *targetURL, *apiKey, payload, *logsPerReq, *bursts, *burstSize, *concurrency, *pause)
	case "sustained":
		if *duration <= 0 || *rate <= 0 {
			log.Fatal("duration and rate must be positive")
		}
		results, err = runSustained(client, *targetURL, *apiKey, payload, *logsPerReq, *duration, *rate, *concurrency)
	default:
		log.Fatal("mode must be burst or sustained")
	}
	if err != nil {
		log.Fatal(err)
	}

	stats := summarize(results, time.Since(started))
	printSummary(stats)
	if stats.duration > 0 {
		totalLogs := int64(stats.requests) * int64(*logsPerReq)
		fmt.Printf("log_rate=%.2f/s\n", float64(totalLogs)/stats.duration.Seconds())
	}
	if errorRate(stats) > *maxError {
		log.Fatalf("error rate %.4f exceeded threshold %.4f", errorRate(stats), *maxError)
	}
	if *maxP95 > 0 && stats.latencyP95 > *maxP95 {
		log.Fatalf("p95 latency %s exceeded threshold %s", stats.latencyP95, *maxP95)
	}
}

func runBursts(client *http.Client, targetURL, apiKey string, payload requestPayload, logsPerRequest, bursts, burstSize, concurrency int, pause time.Duration) ([]result, error) {
	results := make([]result, 0, bursts*burstSize)
	for burst := 0; burst < bursts; burst++ {
		burstResults, err := runBurst(client, targetURL, apiKey, payload, logsPerRequest, burst*burstSize, burstSize, concurrency)
		if err != nil {
			return nil, err
		}
		results = append(results, burstResults...)
		fmt.Printf("burst=%d sent=%d\n", burst+1, len(burstResults))
		if burst < bursts-1 {
			time.Sleep(pause)
		}
	}
	return results, nil
}

func runBurst(client *http.Client, targetURL, apiKey string, payload requestPayload, logsPerRequest, sequenceStart, total, concurrency int) ([]result, error) {
	results := make([]result, total)
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for i := 0; i < total; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			requestPayload := payload
			requestPayload.Logs = buildLogs(logsPerRequest, sequenceStart+idx)
			results[idx] = sendRequest(client, targetURL, apiKey, requestPayload)
		}(i)
	}
	wg.Wait()
	return results, nil
}

func runSustained(client *http.Client, targetURL, apiKey string, payload requestPayload, logsPerRequest int, duration time.Duration, rate, concurrency int) ([]result, error) {
	interval := time.Second / time.Duration(rate)
	if interval <= 0 {
		return nil, fmt.Errorf("rate is too high")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	timer := time.NewTimer(duration)
	defer timer.Stop()

	sem := make(chan struct{}, concurrency)
	results := make([]result, 0, int(duration.Seconds()*float64(rate)))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sequence := 0
	for {
		select {
		case <-timer.C:
			wg.Wait()
			return results, nil
		case <-ticker.C:
			sem <- struct{}{}
			wg.Add(1)
			current := sequence
			sequence++
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				requestPayload := payload
				requestPayload.Logs = buildLogs(logsPerRequest, current)
				item := sendRequest(client, targetURL, apiKey, requestPayload)
				mu.Lock()
				results = append(results, item)
				mu.Unlock()
			}()
		}
	}
}

func sendRequest(client *http.Client, targetURL, apiKey string, payload requestPayload) result {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return result{err: fmt.Errorf("marshal payload: %w", err)}
	}
	start := time.Now()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, targetURL, bytes.NewReader(encoded))
	if err != nil {
		return result{err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", apiKey)
	resp, err := client.Do(req)
	if err != nil {
		return result{duration: time.Since(start), err: err}
	}
	_ = resp.Body.Close()
	return result{statusCode: resp.StatusCode, duration: time.Since(start)}
}

func buildLogs(count, sequence int) []requestLogItem {
	logs := make([]requestLogItem, 0, count)
	base := time.Now().UTC()
	for i := 0; i < count; i++ {
		logs = append(logs, requestLogItem{
			Timestamp: base.Add(time.Duration(i) * time.Millisecond).Format(time.RFC3339Nano),
			Level:     "error",
			Message:   fmt.Sprintf("database timeout request=%d item=%d", sequence+1, i+1),
			Fields: map[string]any{
				"host":     "loadtest-1",
				"trace_id": fmt.Sprintf("request-%d-item-%d", sequence+1, i+1),
				"region":   "us-west-2",
			},
		})
	}
	return logs
}

func summarize(results []result, elapsed time.Duration) summary {
	stats := summary{requests: len(results), duration: elapsed}
	latencies := make([]time.Duration, 0, len(results))
	for _, item := range results {
		if item.err != nil || item.statusCode < 200 || item.statusCode >= 300 {
			stats.failures++
		} else {
			stats.successes++
		}
		if item.err == nil {
			latencies = append(latencies, item.duration)
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if len(latencies) > 0 {
		stats.latencyP50 = percentile(latencies, 0.50)
		stats.latencyP95 = percentile(latencies, 0.95)
		stats.latencyP99 = percentile(latencies, 0.99)
		stats.latencyMax = latencies[len(latencies)-1]
	}
	return stats
}

func errorRate(stats summary) float64 {
	if stats.requests == 0 {
		return 1
	}
	return float64(stats.failures) / float64(stats.requests)
}

func printSummary(stats summary) {
	fmt.Printf("requests=%d successes=%d failures=%d error_rate=%.4f\n", stats.requests, stats.successes, stats.failures, errorRate(stats))
	if stats.duration > 0 {
		fmt.Printf("request_rate=%.2f/s\n", float64(stats.requests)/stats.duration.Seconds())
	}
	if stats.requests == 0 {
		return
	}
	fmt.Printf("latency_p50=%s\n", stats.latencyP50)
	fmt.Printf("latency_p95=%s\n", stats.latencyP95)
	fmt.Printf("latency_p99=%s\n", stats.latencyP99)
	fmt.Printf("latency_max=%s\n", stats.latencyMax)
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
