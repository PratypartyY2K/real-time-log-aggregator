package config

import (
	"os"
	"strconv"
)

type Config struct {
	ServiceName    string
	HTTPAddr       string
	LogLevel       string
	NATSURL        string
	NATSStream     string
	NATSSubject    string
	NATSDLQSubject string
	NATSDurable    string
	NATSMaxDeliver int
	PostgresDSN    string
	ClickHouseDSN  string
}

func Load(defaultServiceName, defaultHTTPAddr string) Config {
	return Config{
		ServiceName:    envOrDefault("SERVICE_NAME", defaultServiceName),
		HTTPAddr:       envOrDefault("HTTP_ADDR", defaultHTTPAddr),
		LogLevel:       envOrDefault("LOG_LEVEL", "info"),
		NATSURL:        envOrDefault("NATS_URL", "nats://localhost:4222"),
		NATSStream:     envOrDefault("NATS_STREAM", "LOGS"),
		NATSSubject:    envOrDefault("NATS_SUBJECT", "logs.raw"),
		NATSDLQSubject: envOrDefault("NATS_DLQ_SUBJECT", "logs.raw.dlq"),
		NATSDurable:    envOrDefault("NATS_DURABLE", defaultServiceName),
		NATSMaxDeliver: envOrDefaultInt("NATS_MAX_DELIVER", 5),
		PostgresDSN:    envOrDefault("POSTGRES_DSN", "postgres://logagg:logagg@localhost:55432/logagg?sslmode=disable"),
		ClickHouseDSN:  envOrDefault("CLICKHOUSE_DSN", "http://localhost:8123"),
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
