# Real-time Log Aggregator Architecture

This document is the architectural source of truth for the implemented system.
It covers the high-level design (HLD), low-level design (LLD), data model,
distributed-system behavior, guarantees, failure modes, and evolution path.

## 1. Problem statement

The platform accepts multi-tenant application logs, absorbs bursty write load,
normalizes and stores records for analytical access, evaluates alert rules, and
supports replay without multiplying persisted data or alert transitions.

The design optimizes for:

- durable acceptance independent of analytical-storage latency
- horizontal scaling of stateless services
- tenant isolation throughout ingestion, storage, and querying
- bounded and observable failure recovery
- efficient time-range and tenant-scoped analytical queries
- explicit delivery semantics rather than an unrealistic exactly-once claim

The current local deployment demonstrates logical sharding. It is not a
production HA topology because shards and Keeper are not replicated.

## 2. Requirements and constraints

### Functional requirements

- Authenticate producers and readers with API keys.
- Accept bounded batches of structured logs over HTTP.
- Preserve accepted batches in a durable replayable stream.
- Normalize timestamps, tags, trace IDs, hosts, fields, and fingerprints.
- Query raw logs and time-bucketed/grouped analytics.
- Evaluate threshold and pattern rules and persist alert lifecycle state.
- Retry transient failures and isolate poison messages in a DLQ.
- Expose health, readiness, metrics, and partial-result status.

### Non-functional requirements

| Quality | Design response |
|---|---|
| Availability | Stateless APIs, durable queue, best-effort reads, retrying writes |
| Scalability | Queue-based worker scaling and tenant-hashed ClickHouse shards |
| Durability | JetStream persistence, Postgres volumes, ClickHouse shard volumes |
| Consistency | At-least-once delivery plus application-level batch idempotency |
| Isolation | Tenant identity derived from API keys and enforced in every read/write |
| Operability | Readiness dependency checks, Prometheus metrics, Grafana dashboards |
| Evolvability | Versioned events, explicit interfaces, separate control/data planes |

### Explicit constraints

- Alert evaluation currently runs inline with processing.
- Rate limiting is process-local and therefore approximate across replicas.
- Idempotency is batch-scoped using `ingest_id`, not record-scoped.
- The local ClickHouse cluster has two shards with one replica each.

## 3. High-level design

### 3.1 System context

```mermaid
flowchart LR
    Producer["Application / log producer"]
    Operator["Operator"]
    Reader["Developer / dashboard"]
    Platform["Real-time Log Aggregator"]
    Notification["Notification destination"]

    Producer -->|"log batches"| Platform
    Reader -->|"raw and analytical queries"| Platform
    Operator -->|"configuration, replay, monitoring"| Platform
    Platform -->|"alert notifications"| Notification
```

### 3.2 Container view

```mermaid
flowchart TB
    subgraph Clients["Clients"]
        P["Producers"]
        R["Readers"]
        O["Operators"]
    end

    subgraph Stateless["Stateless Go services"]
        I["ingest-api"]
        W["processor workers"]
        Q["query-api / query coordinator"]
    end

    subgraph Messaging["Messaging plane"]
        JS["NATS JetStream\nLOGS stream"]
        DLQ["logs.raw.dlq"]
    end

    subgraph Data["Data plane"]
        C1["ClickHouse shard 1"]
        C2["ClickHouse shard 2"]
        K["ClickHouse Keeper"]
    end

    subgraph Control["Control plane"]
        PG["Postgres"]
    end

    subgraph Observe["Observability"]
        PR["Prometheus"]
        G["Grafana"]
    end

    P --> I
    I --> PG
    I --> JS
    JS --> W
    W --> C1 & C2
    W --> PG
    W --> DLQ
    R --> Q
    Q --> PG
    Q --> C1 & C2
    K --- C1
    K --- C2
    I & W & Q --> PR
    PR --> G
    O --> G
```

### 3.3 Control plane versus data plane

| Plane | Technology | Owns |
|---|---|---|
| Ingest transport | NATS JetStream | Durable batches, replay position, redelivery |
| Analytical data | ClickHouse | Normalized logs and aggregate execution |
| Control/state | Postgres | Tenants, keys, services, rules, alerts, deliveries |
| Coordination | ClickHouse Keeper | Cluster metadata and distributed DDL coordination |

The separation prevents high-volume immutable log data from competing with
transactional alert state and keeps stream retention independent of query-store
retention.

## 4. Core data flows

### 4.1 Ingestion sequence

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer
    participant I as ingest-api
    participant PG as Postgres
    participant J as JetStream

    P->>I: POST /v1/logs + X-API-Key
    I->>PG: Resolve active key and service scope
    PG-->>I: tenant_id, service_id, rate limit
    I->>I: Validate, normalize envelope, fingerprint batch
    I->>J: Publish logs.raw.v1 with Nats-Msg-Id
    alt durable publish succeeds
        J-->>I: PubAck
        I-->>P: 202 Accepted + request_id
    else stream unavailable
        J--xI: publish error
        I-->>P: 503 Service Unavailable
    end
```

Acceptance means the stream durably acknowledged the event; it does not mean
ClickHouse persistence or alert evaluation has completed.

### 4.2 Processing and retry sequence

```mermaid
sequenceDiagram
    autonumber
    participant J as JetStream
    participant W as processor
    participant CH as ClickHouse Distributed table
    participant PG as Postgres
    participant D as Dispatcher

    J->>W: Pull logs.raw.v1 batch
    W->>W: Validate contract and normalize records
    W->>CH: Check tenant_id + ingest_id
    alt batch already persisted
        CH-->>W: exists
        W->>J: ACK
    else new batch
        CH-->>W: absent
        W->>PG: Load active alert rules
        W->>W: Evaluate rules
        W->>CH: Synchronous distributed insert
        W->>PG: Reconcile alert state and enqueue deliveries
        W->>D: Dispatch due notifications
        alt all required work succeeds
            W->>J: ACK
        else transient dependency failure
            W->>J: NAK with delay
        else poison input or retry exhaustion
            W->>J: Publish logs.raw.dlq and terminate
        end
    end
```

Writes use `insert_distributed_sync=1`; an unavailable target shard therefore
fails the batch and lets JetStream drive recovery instead of acknowledging data
that only exists in a local distribution queue.

### 4.3 Distributed query sequence

```mermaid
sequenceDiagram
    autonumber
    participant R as Reader
    participant Q as query-api
    participant PG as Postgres
    participant S1 as Shard 1 / coordinator
    participant S2 as Shard 2

    R->>Q: Query + X-API-Key
    Q->>PG: Resolve tenant identity
    PG-->>Q: tenant_id
    par bounded health probes
        Q->>S1: SELECT 1
        Q->>S2: SELECT 1
    end
    Q->>S1: Tenant-scoped query on Distributed logs
    S1->>S1: Evaluate shard key and local partial
    S1->>S2: Fan out if required
    S2-->>S1: Rows or aggregate state
    S1->>S1: Merge, order, and limit
    S1-->>Q: Final result
    Q-->>R: Results + partial/unavailable_shards metadata
```

Every query includes the authenticated `tenant_id`. Because the distributed
table shards by `cityHash64(tenant_id)`, `optimize_skip_unused_shards=1` normally
reduces a tenant query to one shard. ClickHouse—not application code—performs
fan-out and aggregation-state merging.

## 5. Distributed storage design

### 5.1 Logical and physical layout

```mermaid
flowchart TB
    L["logs\nDistributed engine"]
    H["cityHash64(tenant_id)"]
    S1["Shard 1: logs_local\nMergeTree"]
    S2["Shard 2: logs_local\nMergeTree"]

    L --> H
    H -->|"hash mod shard count = 0"| S1
    H -->|"hash mod shard count = 1"| S2

    S1 --> P1["Daily partitions"]
    S2 --> P2["Daily partitions"]
    P1 --> O1["ORDER BY tenant_id, timestamp, service"]
    P2 --> O2["ORDER BY tenant_id, timestamp, service"]
```

The three independent layout decisions serve different purposes:

| Mechanism | Key | Purpose |
|---|---|---|
| Sharding | `cityHash64(tenant_id)` | Distribute tenants and route reads/writes |
| Partitioning | `toDate(timestamp)` | TTL cleanup and partition pruning |
| Sorting | `(tenant_id, timestamp, service)` | Tenant/time range locality |

Compression uses Delta codecs for numeric/time columns, LowCardinality for
bounded tag dimensions, and ZSTD for variable strings. Bloom filters accelerate
ingest-ID, trace-ID, and extracted error-code lookups where the primary sort key
cannot.

### 5.2 Query coordination and partial failure

The `query-api` is the external coordinator, while ClickHouse's `Distributed`
engine is the execution coordinator. Reads set:

- `optimize_skip_unused_shards=1`
- `skip_unavailable_shards=1`

The API probes configured shard HTTP endpoints with a one-second budget. If a
shard is unavailable, JSON responses expose `partial=true` and
`unavailable_shards`; streaming responses expose equivalent headers. This is a
conservative completeness signal: an unavailable shard may not contain the
authenticated tenant, but the API never labels uncertain data as complete.

Writes deliberately do not degrade to partial success.

### 5.3 Production deployment target

```mermaid
flowchart TB
    LB["Load balancer"]
    I["ingest-api replicas"]
    Q["query-api replicas"]
    W["processor replicas"]
    N["3-node NATS JetStream"]
    PG["HA Postgres"]

    subgraph CH["ClickHouse cluster"]
        S1R1["Shard 1 / replica 1"]
        S1R2["Shard 1 / replica 2"]
        S2R1["Shard 2 / replica 1"]
        S2R2["Shard 2 / replica 2"]
    end

    subgraph Keeper["Keeper quorum"]
        K1["K1"]
        K2["K2"]
        K3["K3"]
    end

    OBJ["Object storage backups"]

    LB --> I & Q
    I --> N & PG
    N --> W
    W --> CH & PG
    Q --> CH & PG
    K1 & K2 & K3 --- CH
    CH & PG & N --> OBJ
```

Persistent storage is required for JetStream, Postgres, ClickHouse replicas,
and Keeper. Compute may be disposable if these stateful layers use durable
volumes and tested backups.

## 6. Low-level design

### 6.1 Service component view

```mermaid
flowchart LR
    subgraph Ingest["ingest-api"]
        IH["HTTP handler"] --> AUTH["Authenticator"]
        IH --> RL["Rate limiter"]
        IH --> BP["Backpressure controller"]
        IH --> PUB["JetStream publisher"]
    end

    subgraph Processor["processor"]
        CON["Pull consumer"] --> NORM["Normalizer"]
        NORM --> WR["ClickHouse writer"]
        NORM --> EV["Alert evaluator"]
        EV --> ST["Alert state store"]
        ST --> ND["Notification dispatcher"]
        CON --> DQ["DLQ publisher"]
    end

    subgraph Query["query-api"]
        AM["Tenant auth middleware"] --> LH["Log handler"]
        AM --> AH["Analytics handler"]
        AM --> DSL["Query DSL handler"]
        LH & AH & DSL --> CS["ClickHouse store"]
        CS --> HP["Shard health probes"]
    end
```

Interfaces isolate transport and persistence concerns, allowing handler and
pipeline tests to use deterministic stubs without starting infrastructure.

### 6.2 Package responsibilities

| Package | Responsibility |
|---|---|
| `internal/ingest` | Validation, authorization contract, limits, rate limiting, backpressure |
| `internal/stream` | JetStream publish/consume, replay modes, lag monitoring, DLQ |
| `internal/processor` | Contract validation, normalization, idempotency, persistence pipeline |
| `internal/queryapi` | Tenant context, filters, SQL generation, response streaming |
| `internal/alerts` | Rule evaluation, state transitions, notification retry state |
| `internal/readiness` | Dependency-specific readiness aggregation |
| `internal/metrics` | Prometheus formatting and HTTP instrumentation |
| `internal/config` | Environment-driven runtime configuration |

### 6.3 Postgres domain model

```mermaid
erDiagram
    TENANTS ||--o{ SERVICES : owns
    TENANTS ||--o{ API_KEYS : issues
    SERVICES o|--o{ API_KEYS : restricts
    TENANTS ||--o{ ALERT_RULES : defines
    SERVICES o|--o{ ALERT_RULES : scopes
    ALERT_RULES ||--o{ ALERT_INSTANCES : creates
    ALERT_INSTANCES ||--o{ ALERT_EVENTS : records
    ALERT_INSTANCES ||--o{ NOTIFICATION_DELIVERIES : queues
    TENANTS ||--o{ SAVED_QUERIES : owns

    TENANTS {
        bigint id PK
        text name UK
    }
    SERVICES {
        bigint id PK
        bigint tenant_id FK
        text name
        text environment
    }
    API_KEYS {
        bigint id PK
        bigint tenant_id FK
        bigint service_id FK
        text key_hash UK
        text status
        int rate_limit_per_sec
    }
    ALERT_RULES {
        bigint id PK
        bigint tenant_id FK
        bigint service_id FK
        text rule_type
        jsonb filter_json
        int window_seconds
        numeric threshold
    }
    ALERT_INSTANCES {
        bigint id PK
        bigint rule_id FK
        text dedupe_key
        text status
    }
    ALERT_EVENTS {
        bigint id PK
        bigint alert_instance_id FK
        text event_type
        jsonb payload_json
    }
    NOTIFICATION_DELIVERIES {
        bigint id PK
        bigint alert_instance_id FK
        text status
        int attempt_count
    }
    SAVED_QUERIES {
        bigint id PK
        bigint tenant_id FK
        jsonb query_json
    }
```

### 6.4 Alert lifecycle

```mermaid
stateDiagram-v2
    [*] --> Active: first threshold/pattern match
    Active --> Active: match after cooldown / triggered
    Active --> Active: match inside cooldown / suppressed
    Active --> Resolved: group no longer matches
    Resolved --> Active: subsequent match creates active instance
    Resolved --> [*]
```

The dedupe key is derived from rule scope and configured grouping fields. Alert
events are append-oriented, while the instance row represents current state.

## 7. Contracts and delivery semantics

### 7.1 Versioned boundaries

- HTTP ingestion requires `logs.ingest.v1`.
- JetStream transport uses `logs.raw.v1`.
- DLQ envelopes use `logs.dlq.v1`.

Schema versions make incompatible changes explicit and prevent silent producer/
consumer drift.

### 7.2 At-least-once, not exactly-once

```mermaid
flowchart LR
    A["Producer retry"] --> B["JetStream dedupe window"]
    B --> C["At-least-once delivery"]
    C --> D["tenant_id + ingest_id check"]
    D --> E["Idempotent batch outcome"]
```

- The deterministic batch fingerprint is sent as `Nats-Msg-Id` for stream-side
  duplicate suppression within `NATS_DEDUPE_WINDOW`.
- JetStream may redeliver after timeouts or negative acknowledgements.
- The processor checks `(tenant_id, ingest_id)` before persistence.
- A crash between ClickHouse persistence and acknowledgement causes redelivery;
  the existence check then suppresses the duplicate batch.

There is still a distributed transaction boundary between ClickHouse and
Postgres alert state. The current ordering favors not acknowledging before log
persistence and state reconciliation complete, but it is not atomic across both
databases. A production evolution could use a dedicated processed-batch ledger
or transactional outbox pattern.

## 8. Failure model

| Failure | Observable behavior | Recovery |
|---|---|---|
| Invalid/missing API key | `401`/`403` | Correct credentials or scope |
| Ingest overload | Delay or `429` | Backpressure plus worker scaling |
| JetStream publish failure | `503`; batch not accepted | Producer retry |
| Poison event | Published to `logs.raw.dlq` | Inspect, correct, replay |
| Transient processor dependency failure | NAK and redelivery | Automatic bounded retry |
| Retry exhaustion | DLQ and terminate | Operator remediation |
| Target ClickHouse shard unavailable on write | Insert fails; message not ACKed | Shard recovery, then redelivery |
| Non-coordinator shard unavailable on read | `200` with partial metadata | Retry or accept degraded result |
| Coordinator unavailable | `503` | Fail over coordinator endpoint |
| Postgres unavailable | Auth/alert/readiness failure | Database recovery/failover |

## 9. Backpressure and scaling

JetStream absorbs short mismatches between producer rate and processing rate.
Let:

- `λ` = incoming logs/second
- `μ` = sustainable processed logs/second per worker
- `n` = active workers

Backlog grows when `λ > nμ`. Approximate drain time after load returns below
capacity is:

```text
drain_time ≈ backlog / (nμ - λ)
```

`logagg_queue_consumer_pending` is therefore the primary scaling signal; CPU is
supporting evidence. Scale out when lag remains elevated and dependencies are
healthy. Scale in only after lag remains near zero to avoid oscillation.

| Component | Horizontal strategy | Current limiter |
|---|---|---|
| `ingest-api` | Add stateless replicas behind a load balancer | Process-local rate limiter |
| `processor` | Add pull-consumer workers | Downstream write rate and alert contention |
| `query-api` | Add stateless replicas | ClickHouse query capacity |
| ClickHouse | Add tenant-hashed shards and replicas | Rebalancing existing tenants |

Adding shards changes hash placement. Production expansion therefore requires a
planned rebalancing strategy—such as weighted shards, explicit tenant placement,
or migration into a new distributed table—not an uncoordinated config edit.

## 10. Security and tenant isolation

```mermaid
flowchart LR
    K["X-API-Key"] --> H["SHA-256 lookup"]
    H --> T["Resolved tenant_id"]
    T --> E["Event tenant scope"]
    T --> Q["Mandatory query predicate"]
    E --> S["Shard routing"]
    Q --> S
```

- Plaintext API keys are not stored; Postgres stores their SHA-256 digests.
- Ingestion validates service/environment scope against control-plane records.
- Readers cannot submit or override `tenant_id`.
- Tenant identity is carried in the request context and required by ClickHouse
  store methods, which fail closed if it is absent.
- Parameter values are validated and SQL literals are escaped before query
  generation.

Future hardening includes secret rotation, TLS/mTLS, audit logging, distributed
rate limits, and service/environment authorization on the read path.

## 11. Observability model

```mermaid
flowchart LR
    S["Services"] -->|"/metrics"| P["Prometheus"]
    P --> G["Grafana dashboards"]
    S --> H["healthz: process liveness"]
    S --> R["readyz: dependency readiness"]
    J["JetStream state"] --> S
    C["ClickHouse shard probes"] --> S
```

The telemetry model follows the pipeline:

- edge: request rate, latency, status, authentication and validation outcomes
- queue: stream size, pending messages, acknowledgements, redeliveries
- worker: processed batches/logs, failures, processing duration
- domain: alert transitions by event type and status
- storage: readiness and per-request shard completeness

See [operations.md](./operations.md) for concrete metrics and failure drills.

## 12. Key design decisions and tradeoffs

| Decision | Why | Cost |
|---|---|---|
| JetStream between ingest and processing | Durable burst absorption and replay | Eventual visibility and operational queue management |
| ClickHouse for logs | Compression and analytical scans | Poor fit for transactional state |
| Postgres for control plane | Constraints, transactions, relational ownership | Cross-store consistency boundary |
| Tenant hash sharding | Stable tenant locality and shard pruning | Hot-tenant risk and rebalancing complexity |
| ClickHouse `Distributed` coordinator | Native fan-out and aggregate merging | Coordinator dependency and ClickHouse-specific behavior |
| Synchronous distributed inserts | Prevent acknowledged-but-not-remote writes | Higher write latency and lower availability during shard failure |
| Best-effort reads | Preserve diagnostic access during partial outage | Results can be incomplete and must be labeled |
| Inline alert evaluation | Simple, low-latency pipeline | Couples alert throughput to ingestion processing |

## 13. Evolution roadmap

1. Add replicated ClickHouse shards and a three-node Keeper quorum.
2. Introduce explicit tenant placement or consistent-hash virtual nodes before
   online shard expansion.
3. Replace process-local rate limiting with a distributed token bucket.
4. Move notifications behind a transactional outbox and dedicated workers.
5. Add a processed-batch ledger to tighten cross-store replay semantics.
6. Add service/environment authorization and audit events to read APIs.
7. Add archive/restore workflows once retention and cold-storage requirements
   are finalized.

## 14. Source map

- Service entrypoints: `cmd/ingest-api`, `cmd/processor`, `cmd/query-api`
- Ingestion pipeline: `internal/ingest`
- Stream and replay logic: `internal/stream`
- Processing pipeline: `internal/processor`
- Query coordination: `internal/queryapi`
- Alert domain: `internal/alerts`
- Postgres schema: `db/postgres/001_init.sql`
- ClickHouse schemas: `db/clickhouse/*.sql`
- Local topology: `deployments/local/docker-compose.yml`

Related references:

- [HTTP API](./api.md)
- [JetStream contract](./jetstream.md)
- [Distributed ClickHouse](./distributed-clickhouse.md)
- [Operations guide](./operations.md)
