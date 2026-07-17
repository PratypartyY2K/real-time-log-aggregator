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

The local setup uses Docker Compose for infrastructure only. `ingest-api`, `processor`, `query-api`, and helper commands run on the host with `go run`.

## Docker bootstrap

1. Start local infrastructure:

```bash
docker compose -f deployments/local/docker-compose.yml up -d
```

2. Confirm the containers are up:

```bash
docker compose -f deployments/local/docker-compose.yml ps
```

3. Initialize the Postgres schema:

```bash
go run ./cmd/postgres-migrate
```

`make migrate-postgres` runs the same migration command through the project `Makefile`.

4. Initialize the ClickHouse schema:

```bash
cat db/clickhouse/001_logs.sql | docker compose -f deployments/local/docker-compose.yml exec -T clickhouse clickhouse-client --multiquery
```

5. Provision JetStream stream and consumer state:

```bash
go run ./cmd/nats-setup
```

6. Seed local auth and service metadata:

Generate a local API key hash:

```bash
printf 'local-dev-key' | shasum -a 256
```

Insert the tenant, service, and API key:

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

You can apply the SQL from the host with `psql` if it is installed locally, or inside the container:

```bash
docker compose -f deployments/local/docker-compose.yml exec -it postgres psql -U logagg -d logagg
```

Leave `api_keys.service_id` as `NULL` for a tenant-wide key, or set it to a `services.id` value to restrict the key to one service/environment pair.

## Service startup

After the Docker-backed infrastructure and schemas are ready, start the Go services on the host in separate terminals:

Terminal 1:

```bash
go run ./cmd/ingest-api
```

Terminal 2:

```bash
go run ./cmd/query-api
```

Terminal 3:

```bash
go run ./cmd/processor
```

Optional checks:

```bash
curl -sf http://localhost:8080/readyz
curl -sf http://localhost:8081/readyz
curl -sf http://localhost:9092/readyz
```

## Load testing

Use the built-in burst load generator against local `ingest-api`:

```bash
go run ./cmd/loadtest \
  -url http://localhost:8080/v1/logs \
  -api-key local-dev-key \
  -bursts 5 \
  -burst-size 100 \
  -concurrency 20 \
  -logs-per-request 25 \
  -pause 2s
```

What to watch during a run:

- `GET /metrics` on `ingest-api` for HTTP request rates and status codes
- `GET /metrics` on `processor` for `logagg_queue_consumer_pending`, `logagg_queue_consumer_ack_pending`, and processor throughput
- JetStream growth and drain rate through the new queue lag metrics
- `429` responses when `reject` backpressure is enabled
- increased request latency when `delay` backpressure is enabled

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
- `NATS_BACKPRESSURE_STRATEGY`
- `NATS_QUEUE_LAG_HIGH_WATERMARK`
- `NATS_BACKPRESSURE_DELAY`
- `POSTGRES_DSN`
- `CLICKHOUSE_DSN`

Each service has sane local defaults for the Docker Compose environment; see `internal/config/config.go`.

Runtime services expose Prometheus-compatible metrics on `/metrics`. `ingest-api` and `query-api` serve metrics on their main HTTP port. `processor` serves metrics on `METRICS_ADDR`, which defaults to `:9092`.

Queue lag metrics are exposed from both `ingest-api` and `processor`:

- `logagg_queue_monitor_up`
- `logagg_queue_stream_messages`
- `logagg_queue_stream_bytes`
- `logagg_queue_consumer_pending`
- `logagg_queue_consumer_ack_pending`
- `logagg_queue_consumer_waiting`
- `logagg_queue_consumer_redelivered`

Alert evaluation volume is exposed from `processor` as `logagg_alert_state_changes_total`, labeled by `event_type` (`triggered`, `suppressed`, `resolved`) and `status`.

Before `ingest-api` can authorize requests, Postgres must contain at least one active API key row and a matching service record. The `api_keys.key_hash` column stores a SHA-256 hex digest of the plaintext key.

## JetStream contract

The `logs.raw` stream contract is defined explicitly in [docs/jetstream.md](/docs/jetstream.md). Runtime services validate and bind to pre-provisioned JetStream state; they do not create the stream implicitly.

Current delivery model:

- ingestion into JetStream is idempotent within the configured duplicate window
- processing semantics are `at-least-once`
- processor replay can start from the full stream, a JetStream sequence, or an RFC3339 timestamp via `NATS_REPLAY_MODE`
- backpressure can be configured as `off`, `delay`, or `reject` via `NATS_BACKPRESSURE_STRATEGY`

## Backpressure

Current strategy:

- durable buffering happens in JetStream
- producers can be slowed with `NATS_BACKPRESSURE_STRATEGY=delay`
- producers can be rejected with `NATS_BACKPRESSURE_STRATEGY=reject`
- the decision is based on `logagg_queue_consumer_pending` crossing `NATS_QUEUE_LAG_HIGH_WATERMARK`

Recommended local settings for testing:

```bash
export NATS_BACKPRESSURE_STRATEGY=reject
export NATS_QUEUE_LAG_HIGH_WATERMARK=500
```

Or:

```bash
export NATS_BACKPRESSURE_STRATEGY=delay
export NATS_QUEUE_LAG_HIGH_WATERMARK=500
export NATS_BACKPRESSURE_DELAY=500ms
```

## Processor scaling

Current autoscaling posture is manual.

Operational guidance:

- use `logagg_queue_consumer_pending` as the primary lag signal
- use `logagg_processor_batches_total` and `logagg_processor_logs_total` to understand drain rate
- if lag grows continuously and ClickHouse/Postgres remain healthy, increase processor replicas or allocated CPU
- if lag is near zero and processor utilization is low for an extended period, scale back down

Initial manual policy:

- scale up when `logagg_queue_consumer_pending` remains above the chosen watermark for several minutes
- scale down only after lag returns near zero and stays there
- keep backpressure enabled so ingest does not outrun recovery indefinitely

## Documentation

- system architecture: [docs/architecture.md](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/docs/architecture.md)
- HTTP API surface: [docs/api.md](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/docs/api.md)
- JetStream topology and replay: [docs/jetstream.md](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/docs/jetstream.md)

## Current gaps

1. Add read-side authn/authz to `query-api`.
2. Replace the process-local rate limiter with a distributed implementation if multiple `ingest-api` replicas are introduced.
3. Add external notification sinks beyond the current log dispatcher.
4. Add richer query filters and tenant-scoped saved-query workflows.
