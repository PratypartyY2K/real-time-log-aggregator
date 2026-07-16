# Real-time Log Aggregator

A portfolio project focused on reliability engineering and distributed systems fundamentals:

- high-throughput log ingestion
- stream-based processing
- alert evaluation and notification flow
- operational telemetry
- local containerized infrastructure

## Architecture

- `ingest-api`: receives logs over HTTP and publishes batches to the stream
- `processor`: consumes streamed logs, normalizes them, writes processed records to ClickHouse, evaluates alert rules, and dispatches notifications
- `query-api`: serves health/status endpoints and ClickHouse-backed log reads
- `NATS JetStream`: durable buffering, replay, dedupe window, and backpressure boundary
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
db/                     versioned SQL migrations and bootstrap schema
deployments/local/      Docker Compose and Prometheus
docs/                   architecture and milestone notes
```

## Running locally

1. Start infrastructure:

```bash
docker compose -f deployments/local/docker-compose.yml up -d
```

2. Apply Postgres migrations:

```bash
go run ./cmd/postgres-migrate
```

3. Run a service:

```bash
go run ./cmd/nats-setup
go run ./cmd/ingest-api
go run ./cmd/query-api
go run ./cmd/processor
```

`make migrate-postgres` runs the same migration command through the project `Makefile`.

## Environment variables

- `SERVICE_NAME`
- `HTTP_ADDR`
- `LOG_LEVEL`
- `METRICS_ADDR`
- `NATS_URL`
- `NATS_STREAM`
- `NATS_SUBJECT`
- `NATS_DURABLE`
- `NATS_DLQ_SUBJECT`
- `NATS_MAX_DELIVER`
- `NATS_DEDUPE_WINDOW`
- `NATS_REPLAY_MODE`
- `NATS_REPLAY_SEQUENCE`
- `NATS_REPLAY_TIME`
- `POSTGRES_DSN`
- `CLICKHOUSE_DSN`

Each service has sane local defaults; see `internal/config/config.go`.

Runtime services expose Prometheus-compatible metrics on `/metrics`. `ingest-api` and `query-api` serve metrics on their main HTTP port. `processor` serves metrics on `METRICS_ADDR`, which defaults to `:9092`.

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

Current delivery model:

- ingestion into JetStream is idempotent within the configured duplicate window
- processing semantics are `at-least-once`
- processor replay can start from the full stream, a JetStream sequence, or an RFC3339 timestamp via `NATS_REPLAY_MODE`

## Documentation

- system architecture: [docs/architecture.md](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/docs/architecture.md)
- HTTP API surface: [docs/api.md](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/docs/api.md)
- JetStream topology and replay: [docs/jetstream.md](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/docs/jetstream.md)

## Current gaps

1. Add read-side authn/authz to `query-api`.
2. Replace the process-local rate limiter with a distributed implementation if multiple `ingest-api` replicas are introduced.
3. Add external notification sinks beyond the current log dispatcher.
4. Add richer query filters and tenant-scoped saved-query workflows.
