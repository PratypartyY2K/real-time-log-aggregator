# API Bootstrap

## `POST /v1/logs`

Current behavior:

- requires `X-API-Key`
- accepts one JSON batch
- enforces a 1 MiB request-body limit
- validates RFC3339 timestamps
- returns `202 Accepted` when the batch is structurally valid

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

Planned next step:

- publish validated batches to `logs.raw` in NATS JetStream
- replace header-presence auth with real API-key lookup against Postgres
