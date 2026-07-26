# Distributed ClickHouse

The local deployment models production-style logical sharding with two
ClickHouse nodes and one ClickHouse Keeper node. It is intended for functional
testing, not production high availability: each shard has one replica and the
Keeper service is a single node.

## Table of contents

- [Data layout](#data-layout)
- [Partial failures](#partial-failures)
- [Migration](#migration)

## Data layout

- `logs_local` is a `MergeTree` table stored independently on each shard.
- `logs` is a `Distributed` table on the coordinator at `localhost:8123`.
- Daily partitions remain the retention and cleanup boundary.
- Local rows are ordered by `(tenant_id, timestamp, service)`.
- `cityHash64(tenant_id)` deterministically assigns all rows for a tenant to one
  shard.

The processor inserts into `logs` with `insert_distributed_sync=1`. ClickHouse
routes the complete batch to the appropriate shard and acknowledges it only
after the remote write completes. Replay checks include `tenant_id`, allowing
`optimize_skip_unused_shards` to contact only that tenant's shard.

The query API uses the coordinator's `logs` table. ClickHouse performs fan-out,
partial aggregation, and final result merging. Because every query includes the
authenticated `tenant_id`, shard pruning normally reduces a tenant query to a
single shard.

## Partial failures

Read queries enable `skip_unavailable_shards=1`. Before each read, the query API
probes every configured shard for up to one second. JSON responses include:

```json
{
  "partial": true,
  "unavailable_shards": ["shard-2"]
}
```

Streaming responses use the headers `X-Logagg-Partial-Results` and
`X-Logagg-Unavailable-Shards`. Writes do not skip unavailable shards: a failed
synchronous distributed insert is retried through the existing JetStream
consumer behavior.

Shard health probes are configured with `CLICKHOUSE_SHARD_DSNS`, a comma-
separated list. The local default is:

```text
http://localhost:8123,http://localhost:8124
```

## Migration

Start both shards and Keeper, pause the processor, and run the migration on the
coordinator:

```bash
go run ./cmd/migrate -target=clickhouse
```

The migration retains the previous table as `logs_single_node`. Validate totals
and per-tenant counts before removing it manually:

```sql
SELECT count() FROM logs_single_node;
SELECT count() FROM logs;
SELECT tenant_id, count() FROM logs GROUP BY tenant_id ORDER BY tenant_id;
```

For production, use at least two replicas per shard and a three-node Keeper
quorum. Each ClickHouse replica and Keeper node needs persistent storage; the
two local shard volumes demonstrate that requirement but do not provide host-
failure resilience.
