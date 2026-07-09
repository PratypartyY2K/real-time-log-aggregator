# Service Boundaries

## Ingest API

- Accepts HTTP log batches.
- Will enforce API-key auth, payload sizing, and rate limits.
- Publishes validated batches to `logs.raw`.

## Processor

- Consumes `logs.raw`.
- Logs received batch metadata now.
- Will normalize timestamps, severity, tenant/service metadata, and event fingerprints.
- Writes to ClickHouse and emits alert evaluation input.

## Query API

- Serves health and readiness now.
- Will expose log search, alert history, rule management, and incident-oriented reads.

## Operational principles

- Keep transport, control state, and analytical storage separate.
- Start with explicit code and small dependencies.
- Add AI only after ingest, query, and alerting are reliable and observable.
