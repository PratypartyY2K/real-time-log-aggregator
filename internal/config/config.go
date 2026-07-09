package config

import "os"

type Config struct {
	ServiceName   string
	HTTPAddr      string
	LogLevel      string
	NATSURL       string
	NATSStream    string
	NATSSubject   string
	PostgresDSN   string
	ClickHouseDSN string
}

func Load(defaultServiceName, defaultHTTPAddr string) Config {
	return Config{
		ServiceName:   envOrDefault("SERVICE_NAME", defaultServiceName),
		HTTPAddr:      envOrDefault("HTTP_ADDR", defaultHTTPAddr),
		LogLevel:      envOrDefault("LOG_LEVEL", "info"),
		NATSURL:       envOrDefault("NATS_URL", "nats://localhost:4222"),
		NATSStream:    envOrDefault("NATS_STREAM", "LOGS"),
		NATSSubject:   envOrDefault("NATS_SUBJECT", "logs.raw"),
		PostgresDSN:   envOrDefault("POSTGRES_DSN", "postgres://logagg:logagg@localhost:5432/logagg?sslmode=disable"),
		ClickHouseDSN: envOrDefault("CLICKHOUSE_DSN", "http://localhost:8123"),
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
