package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceName               string
	HTTPAddr                  string
	MetricsAddr               string
	LogLevel                  string
	NATSURL                   string
	NATSStream                string
	NATSSubject               string
	NATSDLQSubject            string
	NATSDurable               string
	NATSMaxDeliver            int
	NATSDupeWindow            time.Duration
	NATSReplayMode            string
	NATSReplaySeq             uint64
	NATSReplayTime            string
	NATSBackpressureStrategy  string
	NATSQueueLagHighWatermark uint64
	NATSBackpressureDelay     time.Duration
	PostgresDSN               string
	ClickHouseDSN             string
}

func Load(defaultServiceName, defaultHTTPAddr string) Config {
	return Config{
		ServiceName:               envOrDefault("SERVICE_NAME", defaultServiceName),
		HTTPAddr:                  envOrDefault("HTTP_ADDR", defaultHTTPAddr),
		MetricsAddr:               envOrDefault("METRICS_ADDR", defaultMetricsAddr(defaultServiceName)),
		LogLevel:                  envOrDefault("LOG_LEVEL", "info"),
		NATSURL:                   envOrDefault("NATS_URL", "nats://localhost:4222"),
		NATSStream:                envOrDefault("NATS_STREAM", "LOGS"),
		NATSSubject:               envOrDefault("NATS_SUBJECT", "logs.raw"),
		NATSDLQSubject:            envOrDefault("NATS_DLQ_SUBJECT", "logs.raw.dlq"),
		NATSDurable:               envOrDefault("NATS_DURABLE", defaultServiceName),
		NATSMaxDeliver:            envOrDefaultInt("NATS_MAX_DELIVER", 5),
		NATSDupeWindow:            envOrDefaultDuration("NATS_DEDUPE_WINDOW", 2*time.Minute),
		NATSReplayMode:            strings.ToLower(envOrDefault("NATS_REPLAY_MODE", "live")),
		NATSReplaySeq:             envOrDefaultUint64("NATS_REPLAY_SEQUENCE", 0),
		NATSReplayTime:            envOrDefault("NATS_REPLAY_TIME", ""),
		NATSBackpressureStrategy:  strings.ToLower(envOrDefault("NATS_BACKPRESSURE_STRATEGY", "off")),
		NATSQueueLagHighWatermark: envOrDefaultUint64("NATS_QUEUE_LAG_HIGH_WATERMARK", 10000),
		NATSBackpressureDelay:     envOrDefaultDuration("NATS_BACKPRESSURE_DELAY", 250*time.Millisecond),
		PostgresDSN:               envOrDefault("POSTGRES_DSN", "postgres://logagg:logagg@localhost:55432/logagg?sslmode=disable"),
		ClickHouseDSN:             envOrDefault("CLICKHOUSE_DSN", "http://localhost:8123"),
	}
}

func defaultMetricsAddr(serviceName string) string {
	switch serviceName {
	case "processor":
		return ":9092"
	default:
		return ""
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envOrDefaultInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}

func envOrDefaultUint64(key string, fallback uint64) uint64 {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
			return parsed
		}
	}
	return fallback
}

func envOrDefaultDuration(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}
