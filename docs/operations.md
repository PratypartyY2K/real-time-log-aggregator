# Operations Guide

This guide contains local bootstrap, configuration, migrations, load testing,
and operational runbooks. Architectural rationale belongs in
[architecture.md](./architecture.md); wire contracts belong in
[api.md](./api.md) and [jetstream.md](./jetstream.md).

## Table of contents

- [Local topology](#local-topology)
- [Bootstrap](#bootstrap)
- [Seed local access](#seed-local-access)
- [Run services](#run-services)
- [Configuration](#configuration)
- [Load testing](#load-testing)
- [Backpressure exercises](#backpressure-exercises)
- [Alert rule evaluation](#alert-rule-evaluation)
- [Observability](#observability)
- [Failure drills](#failure-drills)
- [Migration validation](#migration-validation)

## Local topology

Docker Compose runs infrastructure; Go services run on the host.

| Component | Address | Purpose |
|---|---:|---|
| NATS | `localhost:4222` | JetStream transport |
| NATS monitoring | `localhost:8222` | Stream/server diagnostics |
| Postgres | `localhost:55432` | Control-plane state |
| ClickHouse coordinator/shard 1 | `localhost:8123` | Distributed writes and reads |
| ClickHouse shard 2 | `localhost:8124` | Second logical shard |
| Prometheus | `localhost:9090` | Metric collection |
| Grafana | `localhost:3000` | Operational dashboards |

## Bootstrap

Start infrastructure and inspect its state:

```bash
docker compose -f deployments/local/docker-compose.yml up -d
docker compose -f deployments/local/docker-compose.yml ps
```

Initialize Postgres and JetStream:

```bash
go run ./cmd/migrate -target=postgres
go run ./cmd/nats-setup
```

Initialize ClickHouse on a new environment:

```bash
go run ./cmd/migrate -target=clickhouse
```

For a table created with the older unoptimized schema, pause processor writes,
run `go run ./cmd/migrate -target=clickhouse`, and validate the preserved
tables. ClickHouse migration versions are recorded in `schema_migrations`, but
ClickHouse DDL is not transactional; keep processor writes paused for the
one-time table rewrite migrations.

## Seed local access

Generate a SHA-256 hash for the development key:

```bash
printf 'local-dev-key' | shasum -a 256
```

Insert a tenant, local services, and active API key:

```sql
INSERT INTO tenants (name) VALUES ('local') ON CONFLICT (name) DO NOTHING;

INSERT INTO services (tenant_id, name, environment)
SELECT id, service, 'prod'
FROM tenants
CROSS JOIN unnest(ARRAY['catalog', 'gateway', 'checkout', 'payments', 'payments-db']) AS service
WHERE name = 'local'
ON CONFLICT (tenant_id, name, environment) DO NOTHING;

INSERT INTO api_keys (tenant_id, key_hash, status)
SELECT id, '<sha256-of-local-dev-key>', 'active'
FROM tenants WHERE name = 'local'
ON CONFLICT (key_hash) DO NOTHING;
```

Open `psql` inside the container if it is not installed on the host:

```bash
docker compose -f deployments/local/docker-compose.yml exec -it postgres psql -U logagg -d logagg
```

A `NULL` `api_keys.service_id` grants tenant-wide ingest access. Setting it to a
service ID restricts ingestion to that service/environment.

## Run services

Run each process in a separate terminal:

```bash
go run ./cmd/ingest-api
go run ./cmd/processor
go run ./cmd/query-api
```

Readiness checks:

```bash
curl -sf http://localhost:8080/health
curl -sf http://localhost:8080/ready
curl -sf http://localhost:8081/health
curl -sf http://localhost:8081/ready
curl -sf http://localhost:9092/health
curl -sf http://localhost:9092/ready
```

## Configuration

All services have local defaults in `internal/config/config.go`.

| Area | Variables |
|---|---|
| HTTP/logging | `SERVICE_NAME`, `HTTP_ADDR`, `METRICS_ADDR`, `LOG_LEVEL` |
| JetStream | `NATS_URL`, `NATS_STREAM`, `NATS_SUBJECT`, `NATS_DLQ_SUBJECT`, `NATS_DURABLE`, `NATS_MAX_DELIVER`, `NATS_DEDUPE_WINDOW` |
| Replay | `NATS_REPLAY_MODE`, `NATS_REPLAY_SEQUENCE`, `NATS_REPLAY_TIME` |
| Backpressure | `NATS_BACKPRESSURE_STRATEGY`, `NATS_QUEUE_LAG_HIGH_WATERMARK`, `NATS_BACKPRESSURE_DELAY` |
| Storage | `POSTGRES_DSN`, `CLICKHOUSE_DSN`, `CLICKHOUSE_SHARD_DSNS` |

`CLICKHOUSE_SHARD_DSNS` is comma-separated and defaults to the two local HTTP
endpoints.

## Load testing

Burst ingestion sends short, concurrent spikes. Every request has unique log
content so replay deduplication does not hide actual load:

```bash
go run ./cmd/loadtest \
  -url http://localhost:8080/v1/logs \
  -api-key local-dev-key \
  -mode burst \
  -bursts 5 \
  -burst-size 100 \
  -concurrency 20 \
  -logs-per-request 25 \
  -error-code PAYMENT_TIMEOUT \
  -pause 2s \
  -max-error-rate 0.01 \
  -max-p95 500ms
```

Generate a small cross-service incident corpus for RAG retrieval tests after
registering the `catalog`, `gateway`, `checkout`, `payments`, and `payments-db`
services for the local tenant:

```bash
go run ./cmd/rag-fixture
```

Use a fixed incident time for repeatable evaluation, or inspect the payloads
without sending them:

```bash
go run ./cmd/rag-fixture -at 2026-07-30T14:32:00Z
go run ./cmd/rag-fixture -at 2026-07-30T14:32:00Z -print
```

After applying migrations and setting `OPENAI_API_KEY`, embed unique log
templates without sending raw log messages:

```bash
go run ./cmd/embed-templates
```

Sustained mode holds a target request rate for a fixed duration:

```bash
go run ./cmd/loadtest \
  -url http://localhost:8080/v1/logs \
  -api-key local-dev-key \
  -mode sustained \
  -duration 10m \
  -rate 200 \
  -concurrency 50 \
  -logs-per-request 25 \
  -max-error-rate 0.01 \
  -max-p95 500ms
```

The command exits unsuccessfully when an error-rate or p95 threshold is
exceeded. Its summary reports achieved request rate and p50/p95/p99/max
latency. Multiply request rate by `logs-per-request` for offered log throughput.
Set `-error-code` when validating alert thresholds against a stable incident
field while keeping request payloads unique.

Run the query-path microbenchmarks with:

```bash
make query-benchmarks
```

The benchmarks separately measure filtered-query construction and the complete
HTTP query handler with a 100-record response. Record results alongside the
machine shape, dataset size, ClickHouse topology, and query window when doing
environment-level capacity tests.

Observe request rate, processor throughput, consumer lag, redelivery count, and
ClickHouse availability during the run.

## Backpressure exercises

Reject traffic when lag crosses a threshold:

```bash
export NATS_BACKPRESSURE_STRATEGY=reject
export NATS_QUEUE_LAG_HIGH_WATERMARK=500
```

Delay producers instead:

```bash
export NATS_BACKPRESSURE_STRATEGY=delay
export NATS_QUEUE_LAG_HIGH_WATERMARK=500
export NATS_BACKPRESSURE_DELAY=500ms
```

Use `logagg_queue_consumer_pending` as the primary pressure signal. Scale
processors when lag remains elevated and the storage dependencies are healthy;
scale down only after backlog remains near zero.

## Alert rule evaluation

The processor evaluates active rules over event-time sliding windows. New log
batches are evaluated inline, and the scheduled evaluator also periodically
queries ClickHouse so alerts can fire or resolve even when no new batch arrives
for a service. Count and pattern rules remain supported; metric rules use these
configurations:

- `rate_threshold` compares matching records per second with `threshold`. `window_seconds` must be positive.
- `percentile_threshold` compares a percentile with `threshold`. Set `filter_json.value_field` to `raw_size_bytes` or `field.<name>`, and set `filter_json.percentile` between 0 and 100.
- `group_by` creates independently evaluated groups for every rule type.
- filters such as `level`, `source`, `host`, `trace_id`, `message_contains`, and `field_equals` are applied before aggregation.

Example rate rule (at least 2 errors/second over five minutes):

```sql
INSERT INTO alert_rules
    (tenant_id, service_id, name, rule_type, severity, log_level, fingerprint, group_by, window_seconds, threshold)
VALUES
    (1, 10, 'payment-api error fingerprint spike', 'count_threshold', 'high',
     'ERROR', 'payment-db-timeout', '["service"]', 300, 20);
```

The service and environment scope comes from `service_id` through the
`services` table. `log_level`, `fingerprint`, `threshold`, and
`window_seconds` are first-class alert rule fields; existing `filter_json`
filters still work and can be combined with them.

Example latency rule (p95 duration at least 500 ms):

```sql
INSERT INTO alert_rules
    (tenant_id, name, rule_type, severity, filter_json, group_by, window_seconds, threshold)
VALUES
    (1, 'service latency p95', 'percentile_threshold', 'critical',
     '{"value_field":"field.duration_ms","percentile":95}',
     '["service"]', 300, 500);
```

Alert instance lifecycle states are:

- `inactive`: no current alert instance for the rule/group.
- `firing`: the threshold is currently exceeded.
- `resolved`: a previously firing alert no longer exceeds its threshold.

Notifications are deduplicated by rule and group key. A firing notification is
enqueued only when an incident first enters `firing`; repeated evaluations while
the same incident remains firing are recorded as suppressed state changes and do
not enqueue another notification. If a resolved incident reopens within
`cooldown_seconds`, the state moves back to `firing` but the reopening
notification is suppressed.

Triggered event payloads include `metric_value`, `threshold`, `window_seconds`, and, for percentile rules, `percentile` and `value_field`.
Alert history exposes when an incident triggered, when it resolved, the latest
triggering value, and the latest notification delivery result.

Scheduler configuration:

- `ALERT_EVALUATION_INTERVAL`, default `30s`.
- `ALERT_EVALUATION_MAX_RECORDS`, default `50000`, caps records loaded per rule evaluation window.

### Notification delivery worker

Alert transitions transactionally enqueue immutable notification payloads in
Postgres. The processor runs a dedicated in-process delivery worker. Workers
claim due rows with `FOR UPDATE SKIP LOCKED`,
release the database transaction before dispatch, and recover claims whose
lease expires.

Delivery failures use exponential backoff with stable jitter. Every attempt is
recorded in `notification_delivery_attempts`; the delivery row tracks current
status, attempt count, next retry, last error, and sent time.

Configuration:

- `NOTIFICATION_POLL_INTERVAL` (default `5s`)
- `NOTIFICATION_WEBHOOK_URL` optional. When set, the processor posts alert
  notifications to this endpoint as JSON and applies the existing retry policy
  to request failures and non-2xx responses.
- `NOTIFICATION_RETRY_BASE` (default `30s`)
- `NOTIFICATION_RETRY_MAX` (default `30m`)
- `NOTIFICATION_LEASE_DURATION` (default `2m`)
- `NOTIFICATION_MAX_ATTEMPTS` (default `5`)
- `NOTIFICATION_BATCH_SIZE` (default `50`, capped at `500`)

Run `go run ./cmd/migrate -target=postgres` before deploying this version to apply the
delivery audit migration. Delivery remains at-least-once: a process crash after
an external target accepts a message but before Postgres records success can
cause a duplicate send. Future channel implementations should therefore attach
the delivery ID as an idempotency key where the provider supports it.

## Observability

Prometheus-compatible metrics are exposed at `/metrics`. Grafana is provisioned
with `Ingest Throughput`, `Queue Lag`, `Processor Failures`, `Alert Volume`,
`Query Performance`, and `Service SLOs` dashboards. The SLO-style dashboard
shows 5-minute and rolling 1-hour HTTP availability, HTTP p95/p99 latency,
end-to-end processor p95/p99 latency, processor error ratio, and DLQ publication
failure ratio. Its thresholds are operational defaults and should be aligned
with the service's formal SLO policy before production use.

Important metric families:

- `logagg_http_requests_total`
- `logagg_http_request_duration_seconds` (histogram; existing `_sum` and
  `_count` series remain available)
- `logagg_ingest_batches_total`, `logagg_ingest_logs_total`, and
  `logagg_ingest_bytes_total`
- `logagg_queue_monitor_up`
- `logagg_queue_stream_messages` and `logagg_queue_stream_bytes`
- `logagg_queue_consumer_pending` and `logagg_queue_consumer_ack_pending`
- `logagg_queue_consumer_redelivered`
- `logagg_processor_batches_total` and `logagg_processor_logs_total`
- `logagg_processor_end_to_end_latency_seconds` (histogram measured from the
  ingest event's `received_at` through processor completion)
- `logagg_processor_retries_total`
- `logagg_clickhouse_write_duration_seconds` and
  `logagg_clickhouse_write_errors_total`
- `logagg_dlq_publications_total`
- `logagg_alert_state_changes_total`

Ingest throughput should be calculated with `rate()` over the accepted ingest
counters. Query latency percentiles use
`logagg_http_request_duration_seconds_bucket` filtered to `service="query-api"`
and `histogram_quantile()`. Query failures use `logagg_http_requests_total`
filtered to the same service and non-success HTTP status codes. Processor retry
rate uses `logagg_processor_retries_total`; ClickHouse write p95 and write
failure rate use `logagg_clickhouse_write_duration_seconds_bucket` and
`logagg_clickhouse_write_errors_total`. DLQ publication rate is available by
both `reason` and `outcome`, including failed DLQ writes.

## Failure drills

Run the deterministic lightweight recovery checks without external services:

```bash
make chaos-test
```

### Kill processor

Start the processor from a shell and retain its exact PID:

```bash
go run ./cmd/processor &
LOGAGG_PROCESSOR_PID=$!
kill "$LOGAGG_PROCESSOR_PID"
go run ./cmd/processor
```

Publish a batch before the kill and verify after restart that
`logagg_queue_consumer_redelivered` increases, consumer pending returns to zero,
and the ingest ID appears only once in ClickHouse. JetStream leaves an in-flight
unacknowledged message durable; the restarted processor receives it again and
the `(tenant_id, ingest_id)` check prevents a duplicate write.

### Drop NATS connection

```bash
docker compose -f deployments/local/docker-compose.yml stop nats
docker compose -f deployments/local/docker-compose.yml start nats
```

While NATS is unavailable, ingest publishing returns `503`, queue monitoring
reports unavailable, and the processor reports fetch errors without exiting.
After the NATS client reconnects, the processor resumes pulls from the durable
consumer. Verify pending messages drain and `logagg_queue_consumer_redelivered`
does not grow continuously. If the configured NATS reconnect window is
exhausted, restart the processor under its service supervisor.

### Stop shard 2

```bash
docker compose -f deployments/local/docker-compose.yml stop clickhouse-shard2
```

Expected behavior:

- distributed reads return best-effort data with `partial=true`
- streaming reads include `X-Logagg-Partial-Results: true`
- distributed writes fail synchronously
- the processor negatively acknowledges the message and JetStream redelivers it

Restart the shard and verify backlog recovery:

```bash
docker compose -f deployments/local/docker-compose.yml start clickhouse-shard2
```

### Replay

Use `NATS_REPLAY_MODE=all`, `sequence`, or `time` for recovery exercises. Replay
consumers are ephemeral; normal live processing uses the durable consumer.
Processor-side tenant-and-ingest-ID checks prevent already persisted batches
from being written again.

## Migration validation

Before deleting preserved pre-migration tables, compare total and per-tenant
counts:

```sql
SELECT count() FROM logs_single_node;
SELECT count() FROM logs;
SELECT tenant_id, count() FROM logs GROUP BY tenant_id ORDER BY tenant_id;
```

Also execute representative tenant/time-range queries and inspect
`system.clusters`, `system.parts`, and `system.distribution_queue`.
