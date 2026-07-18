# API Reference

This document describes the HTTP surfaces implemented today. Transport details for the JetStream contract live in [docs/jetstream.md](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/docs/jetstream.md). System-level behavior and tradeoffs live in [docs/architecture.md](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/docs/architecture.md).

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
- topology/bootstrap: see [docs/jetstream.md](/Users/pratyushkumar/Documents/Real-time%20Log%20Aggregator/docs/jetstream.md)

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
- `query-api` checks ClickHouse

## `GET /v1/logs`

Current behavior:

- queries normalized logs from ClickHouse
- requires `start` and `end` RFC3339 timestamps
- supports optional exact-match `service` and `level` filters
- supports optional `limit` or `page_size`, default `100`, max `1000`
- supports optional `offset`, max `10000`
- supports `stream=true` for newline-delimited JSON streaming
- returns logs ordered by newest `timestamp` first
- returns normalized records including `tenant_id`, `environment`, `source`, `host`, `trace_id`, `fingerprint`, `ingest_id`, and `raw_size_bytes`

Example request:

```text
/v1/logs?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&service=checkout&level=error&limit=100
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
- `service` and `level` accept only safe tag characters
- `limit` and `page_size` must be positive integers and are capped at `1000`
- `offset` must be a non-negative integer and cannot exceed `10000`

Streaming response:

```text
/v1/logs?start=2026-07-13T17:00:00Z&end=2026-07-13T19:00:00Z&stream=true&page_size=1000
```

When `stream=true`, the response content type is `application/x-ndjson` and each line is one normalized log record.

## `GET /v1/analytics`

Current behavior:

- queries aggregated views over normalized logs from ClickHouse
- requires `start` and `end` RFC3339 timestamps
- requires `aggregation`, one of `count`, `rate`, or `percentile`
- supports exact-match filters for `service`, `env`, `level`, and `error_code`
- supports `group_by` over `service`, `env`, `level`, and `error_code`
- supports time bucketing with `bucket=minute|hour|day`
- supports `top_k` for grouped leaderboard-style queries without bucketing
- requires `percentile` and `value_field` when `aggregation=percentile`
- supports optional `limit`, default `100`, max `1000`

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
