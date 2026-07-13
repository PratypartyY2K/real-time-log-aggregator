# Real-time Log Aggregator

A portfolio project focused on reliability engineering and distributed systems fundamentals:

- high-throughput log ingestion
- stream-based processing
- alert evaluation and notification flow
- operational telemetry
- local containerized infrastructure

## Architecture

- `ingest-api`: receives logs over HTTP and publishes batches to the stream
- `processor`: consumes streamed logs, normalizes them, and writes processed records to ClickHouse
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

Before `ingest-api` can authorize requests, insert at least one active API key row in Postgres and a matching service record. The `api_keys.key_hash` column stores a SHA-256 hex digest of the plaintext key.

Example local bootstrap:

```sql
INSERT INTO tenants (name) VALUES ('local') ON CONFLICT (name) DO NOTHING;

INSERT INTO services (tenant_id, name, environment)
SELECT id, 'checkout', 'prod'
FROM tenants
WHERE name = 'local'
ON CONFLICT (tenant_id, name, environment) DO NOTHING;

INSERT INTO api_keys (tenant_id, key_hash, status)
SELECT id, '<sha256-of-local-dev-key>', 'active'
FROM tenants
WHERE name = 'local'
ON CONFLICT (key_hash) DO NOTHING;
```

Leave `api_keys.service_id` as `NULL` for a tenant-wide key, or set it to a `services.id` value to restrict the key to one service/environment pair.

## JetStream contract

The `logs.raw` stream contract is defined explicitly in [docs/jetstream.md](/docs/jetstream.md). Runtime services validate and bind to pre-provisioned JetStream state; they do not create the stream implicitly.

## MVP next steps

1. Add tenant/service-aware authorization checks in `ingest-api`.
2. Add alert evaluation in `processor`.
3. Extend Postgres schema and workflows for alert rules and control-plane state.
4. Add Prometheus metrics and OpenTelemetry once the base flow is in place.
