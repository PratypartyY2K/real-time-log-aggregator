# API Reference

This document describes the HTTP surfaces implemented today. Transport details for the JetStream contract live in [jetstream.md](./jetstream.md). System-level behavior and tradeoffs live in [architecture.md](./architecture.md).

The machine-readable OpenAPI 3.1 contract is served by `query-api` at
`GET /openapi.yaml` and stored at
[`internal/queryapi/playground/openapi.yaml`](../internal/queryapi/playground/openapi.yaml).

## Correlation headers

Every HTTP endpoint accepts `X-Request-Id` and the W3C `traceparent` header. If either context is absent or the trace context is invalid, the service generates it. Responses include `X-Request-Id` and `X-Trace-Id`, and downstream HTTP calls preserve both identifiers. The ingest pipeline also carries them across JetStream so processor activity and stored records remain connected to the originating request.

## `POST /v1/logs`

Current behavior:

- requires `X-API-Key`
- requires `schema_version: "logs.ingest.v1"` in the request body
- validates the API key against Postgres `api_keys.key_hash`
- requires the key to be authorized for the request `service` and `env`
- enforces per-key request rate limiting from `api_keys.rate_limit_per_sec`
- accepts one JSON batch
- enforces a 1 MiB request-body limit
- enforces a default maximum of 1000 log records per batch
- rejects unknown top-level request fields
- validates and canonicalizes `service`, `env`, and `source`
- validates RFC3339 timestamps and canonicalizes them to UTC
- canonicalizes supported level aliases such as `warning -> warn` and `err -> error`
- rejects unsupported log levels and unsupported nested object field values
- publishes a versioned `logs.raw.v1` event to JetStream
- computes a deterministic batch fingerprint and uses it as the request identifier
- returns the same deterministic identifier in both `request_id` and `fingerprint` on the wire contract
- returns `202 Accepted` when the batch is structurally valid and successfully published

Example request:

```json
{
  "schema_version": "logs.ingest.v1",
  "service": "checkout",
  "env": "prod",
  "source": "app",
  "logs": [
    {
      "timestamp": "2026-07-07T16:00:00Z",
      "level": "error",
      "message": "database timeout",
      "fields": {
        "region": "us-west-2",
        "pod": "checkout-7d9c5b4c9b-abc12"
      }
    }
  ]
}
```

Wire contract:

- subject: `logs.raw`
- schema: `logs.raw.v1`
- topology/bootstrap: see [jetstream.md](./jetstream.md)

Response shape:

```json
{
  "request_id": "3fbc3e706209ca620d4d0fdd2627fb76",
  "accepted": 1,
  "status": "queued"
}
```

Validation rules:

- `schema_version` is required and must currently be `logs.ingest.v1`
- `service`, `env`, and `source` are required and normalized to canonical tag form
- log timestamps must be valid RFC3339 values
- log levels must be from the supported set after canonicalization
- nested object field values are rejected at ingest
- unsupported top-level request fields are rejected

For local testing, hash a plaintext key with SHA-256 and insert it into Postgres:

```bash
printf 'local-dev-key' | shasum -a 256
```

Use the resulting hex digest as `key_hash` in `api_keys`, and make sure a matching row exists in `services` for the request `service` and `env`.

## `GET /metricsz`

Current behavior:

- returns JSON counters for ingest auth outcomes
- includes `authorized`, `missing_api_key`, `invalid_api_key`, `forbidden_scope`, `backend_error`, `authenticator_unavailable`, `rate_limited`, `request_body_too_large`, `invalid_request_body`, and `batch_too_large`
- is specific to `ingest-api`

## `GET /metrics`

Current behavior:

- exposes Prometheus-compatible metrics for the current service

## `GET /healthz`

Current behavior:

- returns `200 OK` with `ok` when the process is alive

## `GET /readyz`

Current behavior:

- `ingest-api` checks Postgres and NATS
- `query-api` checks Postgres and ClickHouse

## `GET /v1/logs`

Current behavior:

- requires `X-API-Key`
- derives the tenant scope from the active API key and always filters ClickHouse by that tenant
- queries normalized logs from ClickHouse
- requires `start` and `end` RFC3339 timestamps
- supports optional exact-match `service`, `level`, and `trace_id` filters
- supports optional `limit` or `page_size`, default `100`, max `1000`
- supports optional `offset`, max `10000`
- supports `stream=true` for newline-delimited JSON streaming
- returns logs ordered by newest `timestamp` first
- returns `partial` and `unavailable_shards` metadata when a distributed read skips an unavailable shard
- returns normalized records including `tenant_id`, `environment`, `source`, `host`, `trace_id`, `fingerprint`, `ingest_id`, and `raw_size_bytes`

Example request:

```text
/v1/logs?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&trace_id=trace-123&limit=100
```

Example response:

```json
{
  "logs": [
    {
      "timestamp": "2026-07-13T18:00:00Z",
      "tenant_id": 1,
      "service": "checkout",
      "environment": "prod",
      "source": "app",
      "host": "api-1",
      "level": "error",
      "trace_id": "trace-123",
      "fingerprint": "abc123",
      "message": "database timeout",
      "fields": {
        "region": "us-west-2"
      },
      "ingest_id": "req-123",
      "raw_size_bytes": 128
    }
  ],
  "count": 1
}
```

Validation rules:

- `start` is required
- `end` is required
- `start` must be before `end`
- the requested time range cannot exceed `7` days
- `service`, `level`, and `trace_id` accept only safe tag characters
- `limit` and `page_size` must be positive integers and are capped at `1000`
- `offset` must be a non-negative integer and cannot exceed `10000`

Streaming response:

```text
/v1/logs?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&stream=true&page_size=1000
```

When `stream=true`, the response content type is `application/x-ndjson` and each line is one normalized log record.
Completeness is reported through `X-Logagg-Partial-Results`; unavailable shard
names are listed in `X-Logagg-Unavailable-Shards`.

## `GET /v1/graph`

Builds a tenant-scoped service dependency graph and correlated flow summaries from normalized logs.

- requires `start` and `end`; the window is capped at 24 hours
- supports optional `trace_id`, `session_id`, and `user_id` filters
- supports up to 10,000 source records per request; `truncated` reports when the limit was reached
- creates explicit edges from `upstream_service`, `caller_service`, `downstream_service`, `peer_service`, and `target_service` fields
- infers additional edges from ordered service transitions in the same session, request, or trace
- groups flows by `session_id`, then `request_id`/`correlation_id`, then `trace_id`, and finally `user_id`
- reports per-node log, error, and flow counts
- reports per-edge affected-flow and propagated-error counts; a propagated error means consecutive services in a flow both emitted error-level records

Example:

```text
/v1/graph?start=2026-07-22T10:00:00Z&end=2026-07-22T11:00:00Z&session_id=session-123
```

The response contains `nodes`, `edges`, and `sessions`. Each group includes its correlation kind, user ID when available, trace IDs, participating services, time bounds, log count, and error count. User-only groups are lower-confidence flows because multiple requests by the same user inside the query window may be combined. Records without any supported correlation identifier contribute to service-node totals but cannot form an inferred flow.

## Alert history and delivery tracking

All alert delivery read APIs require `X-API-Key`, derive tenant scope from the
key, require `start` and `end` RFC3339 timestamps, and cap the requested window
at 90 days. They support `rule_id`, `limit`, and `offset`.

### `GET /v1/alerts/history`

Returns alert instances with rule metadata, dedupe key, current status, first
and latest firing times, and resolution time. The optional `status` filter
selects active or resolved instances.

### `GET /v1/alerts/deliveries`

Returns the current state of each notification delivery, including channel,
target, `pending`/`processing`/`retrying`/`sent`/`failed` status, attempt count,
maximum attempts, next retry time, last error, and sent time. The optional
`status` filter selects a delivery state.

### `GET /v1/alerts/audit`

Returns an append-only timeline combining alert lifecycle events,
`notification_enqueued` events, and immutable `notification_attempt` entries.
Attempt entries include the delivery ID, attempt number, result, error, channel,
and target. The optional `event_type` filter narrows the timeline.

Example:

```text
/v1/alerts/audit?start=2026-07-22T00:00:00Z&end=2026-07-23T00:00:00Z&event_type=notification_attempt
```

## `GET /v1/analytics`

Current behavior:

- requires `X-API-Key` and applies its tenant scope to the ClickHouse query
- queries aggregated views over normalized logs from ClickHouse
- requires `start` and `end` RFC3339 timestamps
- requires `aggregation`, one of `count`, `rate`, or `percentile`
- supports exact-match filters for `service`, `env`, `level`, and `error_code`
- supports `group_by` over `service`, `env`, `level`, and `error_code`
- supports time bucketing with `bucket=minute|hour|day`
- supports `top_k` for grouped leaderboard-style queries without bucketing
- requires `percentile` and `value_field` when `aggregation=percentile`
- supports optional `limit`, default `100`, max `1000`
- returns `partial` and `unavailable_shards` metadata for best-effort distributed results

Example request:

```text
/v1/analytics?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&aggregation=count&group_by=service,level&bucket=minute
```

Top-K example:

```text
/v1/analytics?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&aggregation=count&group_by=error_code&top_k=5
```

Percentile example:

```text
/v1/analytics?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&aggregation=percentile&percentile=95&value_field=field.duration_ms&group_by=service
```

Example response:

```json
{
  "aggregation": "count",
  "bucket": "minute",
  "group_by": ["service", "level"],
  "results": [
    {
      "bucket": "2026-07-13T18:00:00Z",
      "group": {
        "service": "checkout",
        "level": "error"
      },
      "value": 12
    }
  ],
  "count": 1
}
```

Validation rules:

- `start` is required
- `end` is required
- `start` must be before `end`
- the requested time range cannot exceed `31` days
- `aggregation` must be `count`, `rate`, or `percentile`
- `group_by` is restricted to `service`, `env`, `level`, and `error_code`
- `group_by` cannot contain more than `3` fields
- `bucket`, when set, must be `minute`, `hour`, or `day`
- bucketed queries cannot produce more than `5000` time buckets
- `rate` requires a `bucket`
- `top_k` requires at least one `group_by` field and does not support time bucketing
- `percentile` requires `percentile` between `0` and `100`
- `value_field` must be `raw_size_bytes` or `field.<name>`

## `POST /v1/query`

Current behavior:

- requires `X-API-Key` and applies its tenant scope to the dispatched query
- accepts a structured JSON query DSL
- dispatches to raw log queries when `type` is `logs`
- dispatches to analytical queries when `type` is `analytics`
- rejects unknown JSON fields
- applies the same validation, limits, pagination, and streaming behavior as `GET /v1/logs` and `GET /v1/analytics`

Raw log DSL example:

```json
{
  "type": "logs",
  "start": "2026-07-13T17:00:00Z",
  "end": "2026-07-13T19:00:00Z",
  "service": "checkout",
  "level": "error",
  "page_size": 100,
  "offset": 200
}
```

Analytics DSL example:

```json
{
  "type": "analytics",
  "start": "2026-07-13T17:00:00Z",
  "end": "2026-07-13T19:00:00Z",
  "aggregation": "count",
  "group_by": ["service", "level"],
  "bucket": "hour",
  "limit": 500
}
```

## Schema evolution

Current strategy:

- request validation is versioned separately from the internal JetStream event
- `POST /v1/logs` currently accepts `logs.ingest.v1`
- incompatible request-shape changes should be introduced as a new ingest schema version, not by silently loosening `v1`
- the queue event remains versioned as `logs.raw.v1`

## `GET /v1/status`

Current behavior:

- returns a bootstrap JSON payload from `query-api`
- includes `service`, `time`, and `status`
