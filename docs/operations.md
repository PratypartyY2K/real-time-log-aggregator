# Operations Guide

This guide contains local bootstrap, configuration, migrations, load testing,
and operational runbooks. Architectural rationale belongs in
[architecture.md](./architecture.md); wire contracts belong in
[api.md](./api.md) and [jetstream.md](./jetstream.md).

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
go run ./cmd/postgres-migrate
go run ./cmd/nats-setup
```

Initialize ClickHouse on a new environment:

```bash
cat db/clickhouse/001_logs.sql | docker compose -f deployments/local/docker-compose.yml exec -T clickhouse clickhouse-client --multiquery
cat db/clickhouse/003_distributed_logs.sql | docker compose -f deployments/local/docker-compose.yml exec -T clickhouse clickhouse-client --multiquery
```

For a table created with the older unoptimized schema, pause processor writes,
run `002_optimize_logs.sql`, validate it, and then run
`003_distributed_logs.sql`. Both migrations preserve the previous table for
manual validation and rollback.

## Seed local access

Generate a SHA-256 hash for the development key:

```bash
printf 'local-dev-key' | shasum -a 256
```

Insert a tenant, service, and active API key:

```sql
INSERT INTO tenants (name) VALUES ('local') ON CONFLICT (name) DO NOTHING;

INSERT INTO services (tenant_id, name, environment)
SELECT id, 'checkout', 'prod' FROM tenants WHERE name = 'local'
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
curl -sf http://localhost:8080/readyz
curl -sf http://localhost:8081/readyz
curl -sf http://localhost:9092/readyz
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

The processor evaluates active rules over event-time sliding windows. Windows are retained in each processor process and keyed by tenant and rule. Count and pattern rules remain supported; metric rules use these configurations:

- `rate_threshold` compares matching records per second with `threshold`. `window_seconds` must be positive.
- `percentile_threshold` compares a percentile with `threshold`. Set `filter_json.value_field` to `raw_size_bytes` or `field.<name>`, and set `filter_json.percentile` between 0 and 100.
- `group_by` creates independently evaluated groups for every rule type.
- filters such as `level`, `source`, `host`, `trace_id`, `message_contains`, and `field_equals` are applied before aggregation.

Example rate rule (at least 2 errors/second over five minutes):

```sql
INSERT INTO alert_rules
    (tenant_id, name, rule_type, severity, filter_json, group_by, window_seconds, threshold)
VALUES
    (1, 'service error rate', 'rate_threshold', 'high',
     '{"level":"error"}', '["service"]', 300, 2);
```

Example latency rule (p95 duration at least 500 ms):

```sql
INSERT INTO alert_rules
    (tenant_id, name, rule_type, severity, filter_json, group_by, window_seconds, threshold)
VALUES
    (1, 'service latency p95', 'percentile_threshold', 'critical',
     '{"value_field":"field.duration_ms","percentile":95}',
     '["service"]', 300, 500);
```

Triggered event payloads include `metric_value`, `threshold`, `window_seconds`, and, for percentile rules, `percentile` and `value_field`. Sliding-window samples are currently process-local; restarting or independently scaling processors resets or partitions evaluation history.

## Observability

Prometheus-compatible metrics are exposed at `/metrics`. Grafana is provisioned
with `Ingest Throughput`, `Queue Lag`, `Processor Failures`, and `Alert Volume`
dashboards.

Important metric families:

- `logagg_http_requests_total`
- `logagg_queue_monitor_up`
- `logagg_queue_stream_messages` and `logagg_queue_stream_bytes`
- `logagg_queue_consumer_pending` and `logagg_queue_consumer_ack_pending`
- `logagg_queue_consumer_redelivered`
- `logagg_processor_batches_total` and `logagg_processor_logs_total`
- `logagg_alert_state_changes_total`

## Failure drills

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
