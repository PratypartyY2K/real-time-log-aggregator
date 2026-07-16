# Real-time Log Aggregator Architecture

## Overview

This system is a multi-tenant backend for ingesting application logs, buffering them durably, normalizing them into a queryable analytical shape, and evaluating alert rules against each ingested batch.

The implementation is intentionally split across three concerns:

- `ingest-api` handles write traffic, authentication, request validation, and publication to JetStream.
- `processor` handles asynchronous normalization, persistence to ClickHouse, alert evaluation, alert state transitions, and notification dispatch.
- `query-api` handles read traffic against normalized data in ClickHouse.

Supporting infrastructure is also explicit:

- `NATS JetStream` is the durable ingest buffer and replay boundary.
- `ClickHouse` is the analytical store for normalized logs.
- `Postgres` is the control-plane store for tenants, services, API keys, alert rules, alert state, notification deliveries, and saved queries.

This document describes the system as implemented on July 16, 2026.

## Goals

- Accept high-throughput batched log ingestion over HTTP.
- Keep ingestion decoupled from downstream storage and alerting latency.
- Preserve replayability through JetStream.
- Support safe redelivery and replay with idempotent batch handling.
- Store normalized logs in a shape optimized for time-range reads.
- Maintain alert lifecycle state separately from raw log storage.
- Expose health, readiness, and Prometheus-style metrics for operations.

## High-level Topology

```text
producers
  -> ingest-api
  -> NATS JetStream stream LOGS (logs.raw, logs.raw.dlq)
  -> processor
  -> ClickHouse logs table
  -> query-api
  -> readers

processor
  -> Postgres alert_rules / alert_instances / alert_events / notification_deliveries
```

## Runtime Components

## `ingest-api`

Responsibilities:

- Accepts `POST /v1/logs`.
- Requires `X-API-Key`.
- Validates request shape, body size, batch size, and per-record timestamps.
- Resolves the API key against Postgres.
- Enforces service and environment authorization.
- Applies an in-memory per-key rate limiter.
- Computes a deterministic batch fingerprint.
- Publishes a versioned `logs.raw.v1` event to JetStream.

Behavioral notes:

- A structurally valid batch returns `202 Accepted` only after a successful JetStream publish.
- The deterministic batch fingerprint is used as both `request_id` and `fingerprint`.
- The fingerprint is also sent as JetStream `Nats-Msg-Id`, enabling server-side duplicate suppression inside the configured duplicate window.

## `processor`

Responsibilities:

- Pulls from JetStream subject `logs.raw`.
- Validates the raw event contract.
- Normalizes records into the ClickHouse storage shape.
- Checks whether the batch `ingest_id` already exists in ClickHouse.
- Skips already-processed batches to make replay and redelivery safe.
- Writes normalized records to ClickHouse.
- Loads alert rules from Postgres for the batch tenant/service/environment.
- Evaluates rules against the normalized records.
- Synchronizes alert instance state in Postgres.
- Enqueues notification deliveries in Postgres.
- Dispatches due notifications through the configured dispatcher.

Current notification behavior:

- The default dispatcher is a log-based dispatcher.
- Notification state and retry bookkeeping are persisted in Postgres.
- Deliveries move through `pending`, `retrying`, `sent`, and `failed`.

## `query-api`

Responsibilities:

- Exposes read-side HTTP endpoints over normalized ClickHouse data.
- Supports time-range log queries with optional `service` and `level` filters.
- Exposes status, health, readiness, and Prometheus metrics endpoints.

Current read API:

- `GET /v1/logs`
- `GET /v1/status`
- `GET /healthz`
- `GET /readyz`
- `GET /metrics`

## Data Flow

## 1. Ingestion flow

1. A producer sends a JSON batch to `POST /v1/logs`.
2. `ingest-api` validates the batch and authorizes the API key against Postgres.
3. `ingest-api` computes a deterministic batch fingerprint from tenant, service, env, source, and log content.
4. `ingest-api` publishes a `logs.raw.v1` event to JetStream subject `logs.raw`.
5. JetStream stores the batch durably in stream `LOGS`.

## 2. Processing flow

1. `processor` fetches batches from JetStream using a pull consumer.
2. The event is decoded and validated.
3. Each raw log record is normalized:
   - timestamps are converted to UTC
   - service, environment, source, and level are normalized
   - host and trace ID are extracted from fields
   - remaining fields are serialized into stable JSON
   - a record fingerprint is computed for grouping and alert targeting
4. `processor` checks ClickHouse for an existing row with the same `ingest_id`.
5. If already processed, the batch is skipped and acknowledged.
6. Otherwise, normalized rows are inserted into ClickHouse.
7. Alert rules are loaded from Postgres and evaluated against the normalized batch.
8. Alert state is reconciled in Postgres.
9. Notification deliveries are enqueued and due notifications are dispatched.
10. The JetStream message is acknowledged on success.

## 3. Query flow

1. A client calls `GET /v1/logs` with `start`, `end`, and optional filters.
2. `query-api` validates the filter set.
3. `query-api` generates a ClickHouse query.
4. ClickHouse returns normalized rows.
5. `query-api` decodes `fields_json` back into JSON and returns the response.

## Message Contract

The primary transport contract is `logs.raw.v1`.

Fields:

- `schema_version`: currently `logs.raw.v1`
- `request_id`: deterministic batch identifier
- `fingerprint`: same deterministic batch fingerprint used for JetStream dedupe
- `received_at`: UTC ingest timestamp generated by `ingest-api`
- `tenant_id`: tenant derived from API key authorization
- `service`
- `env`
- `source`
- `logs[]`: raw records containing `timestamp`, `level`, `message`, and optional `fields`

Contract ownership:

- `ingest-api` produces the event.
- `processor` validates and consumes the event.
- `docs/jetstream.md` is the transport-specific reference.

## Delivery Semantics

The system does not implement exactly-once processing.

Current guarantees:

- JetStream publication is idempotent within `NATS_DEDUPE_WINDOW` because the batch fingerprint is sent as `Nats-Msg-Id`.
- Processing semantics are `at-least-once`.
- Replay and consumer redelivery are made safe by checking for an existing `ingest_id` in ClickHouse before writing.

Implications:

- Producers may safely retry the same batch within the dedupe window without creating multiple JetStream copies.
- The processor may receive a batch more than once.
- A batch can still be reintroduced after the dedupe window expires, but processor-side `ingest_id` checks prevent duplicate persistence and duplicate alert transitions for already-written batches.

## Replay Model

Replay is intentionally anchored to JetStream rather than ClickHouse or Postgres.

Supported processor replay modes:

- `NATS_REPLAY_MODE=live`
  - bind to the durable consumer and process new traffic
- `NATS_REPLAY_MODE=all`
  - replay the full stream from the beginning through an ephemeral pull subscription
- `NATS_REPLAY_MODE=sequence`
  - replay from `NATS_REPLAY_SEQUENCE`
- `NATS_REPLAY_MODE=time`
  - replay from `NATS_REPLAY_TIME` parsed as RFC3339/RFC3339Nano

Operational intent:

- Use `live` for the default deployment.
- Use `all`, `sequence`, or `time` for backfills, reprocessing, or recovery exercises.
- Rely on batch idempotency in the processor so replay does not multiply persisted rows or reopen alert state unnecessarily.

## Persistence Model

## ClickHouse

The analytical table is `logs`.

Stored fields:

- `timestamp`
- `tenant_id`
- `service`
- `environment`
- `source`
- `host`
- `level`
- `trace_id`
- `fingerprint`
- `message`
- `fields_json`
- `ingest_id`
- `raw_size_bytes`

Engine and layout:

- `MergeTree`
- partitioned by `toDate(timestamp)`
- ordered by `(tenant_id, service, environment, timestamp, level)`
- 30-day TTL on `timestamp`

Design intent:

- Time-range scans are the primary read pattern.
- `ingest_id` is stored for replay-safe idempotency checks.
- `fingerprint` is stored separately from `ingest_id` because it represents record similarity, not batch identity.

## Postgres

Postgres holds control-plane and alerting state.

Current tables:

- `tenants`
- `services`
- `api_keys`
- `alert_rules`
- `alert_instances`
- `alert_events`
- `notification_deliveries`
- `saved_queries`

Responsibilities:

- tenant and service scoping
- API key authorization and rate-limit metadata
- alert rule definitions
- alert lifecycle state
- notification retry bookkeeping
- saved query storage for future UX expansion

## Alerting Architecture

Alert evaluation currently happens inline during batch processing.

Supported rule types:

- `count_threshold`
- `pattern_match`

Filter capabilities:

- `level`
- `source`
- `host`
- `trace_id`
- `message_contains`
- `pattern`
- `target`
- `field_equals`

Grouping:

- Rules may group by named fields such as `service`, `environment`, `source`, `host`, `level`, `trace_id`, `fingerprint`, or `field.<name>`.
- When no explicit group is present, the system uses `scope=all`.

State transitions:

- New matching group creates an `active` alert instance and a `triggered` event.
- Repeated matches within cooldown produce a `suppressed` state change.
- Repeated matches after cooldown update the active instance and create a `triggered` event.
- Missing groups for previously active instances produce a `resolved` event and mark the instance resolved.

Notifications:

- Triggered and resolved state changes enqueue notification deliveries.
- Deliveries are dispatched from Postgres-backed pending work.
- Failed deliveries are retried with bounded attempts and retry delay.

## Failure Handling

## Ingestion failures

- Invalid API keys return `401`.
- Unauthorized service/environment scope returns `403`.
- Rate-limited keys return `429`.
- Invalid payloads return `400`.
- Oversized request bodies return `413`.
- JetStream publish failures return `503`.

## Processing failures

- Malformed JetStream payloads are terminated and published to `logs.raw.dlq`.
- Contract validation failures are treated as poison batches and moved to the DLQ.
- Normalization failures are treated as poison batches and moved to the DLQ.
- Retryable downstream errors are negatively acknowledged with delay until `NATS_MAX_DELIVER` is reached.
- Retry exhaustion sends the batch to `logs.raw.dlq` and terminates it.

## Read path failures

- ClickHouse query failures return `503` from `query-api`.
- Invalid query parameters return `400`.

## Observability

All services expose Prometheus-compatible metrics on `/metrics`.

Current operational endpoints:

- `ingest-api`
  - `/healthz`
  - `/readyz`
  - `/metrics`
  - `/metricsz`
- `processor`
  - `/healthz`
  - `/readyz`
  - `/metrics`
- `query-api`
  - `/healthz`
  - `/readyz`
  - `/metrics`
  - `/v1/status`

Current metric families include:

- HTTP request metrics
- ingest auth and validation outcomes
- queue lag and consumer backlog metrics
- processor batch counts
- processor processed log counts
- processor cumulative batch duration

Readiness dependencies:

- `ingest-api`
  - Postgres
  - NATS
- `processor`
  - Postgres
  - NATS
  - ClickHouse
- `query-api`
  - ClickHouse

## Security and Multi-tenancy

Current tenant isolation model:

- Every API key belongs to a tenant.
- An API key may optionally be bound to a single service/environment pair through `service_id`.
- The tenant ID from authorization is carried into the raw event and persisted into normalized rows.

Current limitations:

- `query-api` does not yet enforce authn/authz on reads.
- `GET /v1/logs` supports service and level filtering but not explicit tenant-scoped access control at the HTTP layer.
- The in-memory rate limiter is process-local and not shared across replicas.

## Configuration

Shared configuration comes from environment variables in `internal/config/config.go`.

Important variables:

- `SERVICE_NAME`
- `HTTP_ADDR`
- `METRICS_ADDR`
- `LOG_LEVEL`
- `NATS_URL`
- `NATS_STREAM`
- `NATS_SUBJECT`
- `NATS_DLQ_SUBJECT`
- `NATS_DURABLE`
- `NATS_MAX_DELIVER`
- `NATS_DEDUPE_WINDOW`
- `NATS_REPLAY_MODE`
- `NATS_REPLAY_SEQUENCE`
- `NATS_REPLAY_TIME`
- `POSTGRES_DSN`
- `CLICKHOUSE_DSN`

Defaults are local-development-friendly and are used by the Docker Compose environment.

## Scaling Characteristics

`ingest-api`:

- horizontally scalable for HTTP write traffic
- limited by process-local rate limiting if multiple replicas are used
- depends on JetStream for buffering under downstream pressure
- can slow or reject producers when consumer lag crosses the configured watermark

`processor`:

- currently modeled around one logical durable consumer for ordered batch handling
- replay modes use ephemeral pull subscriptions
- idempotent batch checks reduce the risk of duplicate writes during restarts or replays
- should scale from queue lag rather than CPU alone because backlog is the primary user-visible pressure signal

Manual autoscaling strategy:

- treat `logagg_queue_consumer_pending` as the primary scale signal
- corroborate it with `logagg_processor_batches_total` and `logagg_processor_logs_total` drain rate
- scale up when backlog remains above the chosen watermark for a sustained interval and downstream stores are healthy
- scale down only after backlog returns near zero and remains stable

`query-api`:

- horizontally scalable and stateless
- read throughput is primarily bounded by ClickHouse

## Tradeoffs and Current Gaps

Intentional tradeoffs:

- `at-least-once` semantics were chosen over exactly-once complexity.
- ClickHouse is used as the analytical store, not the source of truth for control-plane state.
- Alert evaluation runs inline in the processor to keep the architecture small.

Known gaps:

- Read-side authorization is not yet implemented.
- Per-key rate limiting is not distributed.
- Batch idempotency is implemented at batch scope, not individual record scope.
- Notification delivery currently logs instead of integrating with external sinks.
- The processor uses a simple existence check on `ingest_id`; there is no dedicated processed-batches table.

## Source Map

Useful implementation entrypoints:

- [cmd/ingest-api/main.go](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/cmd/ingest-api/main.go)
- [cmd/processor/main.go](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/cmd/processor/main.go)
- [cmd/query-api/main.go](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/cmd/query-api/main.go)
- [internal/ingest/handler.go](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/internal/ingest/handler.go)
- [internal/stream/nats.go](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/internal/stream/nats.go)
- [internal/processor/consumer.go](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/internal/processor/consumer.go)
- [internal/processor/normalize.go](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/internal/processor/normalize.go)
- [internal/queryapi/clickhouse.go](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/internal/queryapi/clickhouse.go)
- [db/postgres/001_init.sql](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/db/postgres/001_init.sql)
- [db/clickhouse/001_logs.sql](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/db/clickhouse/001_logs.sql)
