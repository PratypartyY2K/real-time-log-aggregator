# API Bootstrap

## `POST /v1/logs`

Current behavior:

- requires `X-API-Key`
- accepts one JSON batch
- enforces a 1 MiB request-body limit
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

Planned next step:

- replace header-presence auth with real API-key lookup against Postgres
