# API Bootstrap

## `POST /v1/logs`

Current behavior:

- requires `X-API-Key`
- validates the API key against Postgres `api_keys.key_hash`
- requires the key to be authorized for the request `service` and `env`
- enforces per-key request rate limiting from `api_keys.rate_limit_per_sec`
- accepts one JSON batch
- enforces a 1 MiB request-body limit
- enforces a default maximum of 1000 log records per batch
- validates RFC3339 timestamps
- publishes a versioned `logs.raw.v1` event to JetStream
- returns `202 Accepted` when the batch is structurally valid and successfully published

Example request:

```json
{
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

For local testing, hash a plaintext key with SHA-256 and insert it into Postgres:

```bash
printf 'local-dev-key' | shasum -a 256
```

Use the resulting hex digest as `key_hash` in `api_keys`, and make sure a matching row exists in `services` for the request `service` and `env`.

## `GET /metricsz`

Current behavior:

- returns JSON counters for ingest auth outcomes
- includes `authorized`, `missing_api_key`, `invalid_api_key`, `forbidden_scope`, `backend_error`, `authenticator_unavailable`, `rate_limited`, `request_body_too_large`, `invalid_request_body`, and `batch_too_large`
