# Real-time Log Aggregator

A portfolio project focused on reliability engineering and distributed systems fundamentals:

- high-throughput log ingestion
- stream-based processing
- alert evaluation and notification flow
- operational telemetry
- local containerized infrastructure

## Architecture

- `ingest-api`: receives logs over HTTP and publishes batches to the stream
- `processor`: consumes streamed logs, logs receipt now, and will prepare records for storage and alerting
- `query-api`: serves health endpoints now and will become the read path for logs, alerts, and metadata
- `NATS JetStream`: durable buffering, replay, and backpressure boundary
- `ClickHouse`: analytical store for logs
- `Postgres`: control-plane metadata and alert state

## Repository layout

```text
cmd/                    service entrypoints
internal/app/           reusable service bootstrap
internal/config/        env-driven configuration
internal/ingest/        ingestion request model and handler
internal/logging/       structured logger
internal/worker/        background worker scaffold
db/                     bootstrap SQL for Postgres and ClickHouse
deployments/local/      Docker Compose and Prometheus
docs/                   architecture and milestone notes
```

## Running locally

1. Start infrastructure:

```bash
docker compose -f deployments/local/docker-compose.yml up -d
```

2. Run a service:

```bash
go run ./cmd/nats-setup
go run ./cmd/ingest-api
go run ./cmd/query-api
go run ./cmd/processor
```

## Environment variables

- `SERVICE_NAME`
- `HTTP_ADDR`
- `LOG_LEVEL`
- `NATS_URL`
- `POSTGRES_DSN`
- `CLICKHOUSE_DSN`

Each service has sane local defaults; see `internal/config/config.go`.

## JetStream contract

The `logs.raw` stream contract is defined explicitly in [docs/jetstream.md](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/docs/jetstream.md). Runtime services validate and bind to pre-provisioned JetStream state; they do not create the stream implicitly.

## MVP next steps

1. Implement `/v1/logs` ingestion with API key validation and request limits.
2. Add normalized-log persistence and alert evaluation in `processor`.
3. Define Postgres schema for tenants, API keys, and alert rules.
4. Define ClickHouse schema for normalized logs.
5. Add Prometheus metrics and OpenTelemetry once the base flow is in place.
