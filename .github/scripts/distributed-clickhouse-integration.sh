#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose -p logagg-distributed-ci -f .github/compose.distributed-clickhouse.yml)

cleanup() {
    status=$?
    if [[ $status -ne 0 ]]; then
        "${compose[@]}" ps || true
        "${compose[@]}" logs --no-color clickhouse-keeper clickhouse clickhouse-shard2 || true
    fi
    "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
    exit "$status"
}
trap cleanup EXIT

query() {
    local service=$1
    local sql=$2
    "${compose[@]}" exec -T "$service" clickhouse-client --query "$sql"
}

wait_for_clickhouse() {
    local service=$1
    for _ in $(seq 1 40); do
        if query "$service" "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "$service did not become ready" >&2
    return 1
}

"${compose[@]}" up -d clickhouse-keeper clickhouse clickhouse-shard2
wait_for_clickhouse clickhouse
wait_for_clickhouse clickhouse-shard2

"${compose[@]}" exec -T clickhouse clickhouse-client --multiquery < db/clickhouse/001_logs.sql
"${compose[@]}" exec -T clickhouse clickhouse-client --multiquery < db/clickhouse/003_distributed_logs.sql

cluster_nodes=$(query clickhouse "SELECT count() FROM system.clusters WHERE cluster = 'logs_cluster'")
[[ "$cluster_nodes" == "2" ]] || { echo "expected 2 cluster nodes, got $cluster_nodes" >&2; exit 1; }

tenant_shard1=$(query clickhouse "SELECT number + 1 FROM numbers(1000) WHERE cityHash64(number + 1) % 2 = 0 LIMIT 1 FORMAT TSV")
tenant_shard2=$(query clickhouse "SELECT number + 1 FROM numbers(1000) WHERE cityHash64(number + 1) % 2 = 1 LIMIT 1 FORMAT TSV")

query clickhouse "INSERT INTO logs SETTINGS insert_distributed_sync = 1 SELECT now64(3), toUInt64($tenant_shard1), 'checkout', 'prod', 'ci', 'host-1', 'info', '', 'fp-1', 'shard one', '{}', 'ci-1', toUInt32(100) UNION ALL SELECT now64(3), toUInt64($tenant_shard2), 'checkout', 'prod', 'ci', 'host-2', 'info', '', 'fp-2', 'shard two', '{}', 'ci-2', toUInt32(100)"

shard1_count=$(query clickhouse "SELECT count() FROM logs_local WHERE tenant_id = $tenant_shard1")
shard2_count=$(query clickhouse-shard2 "SELECT count() FROM logs_local WHERE tenant_id = $tenant_shard2")
[[ "$shard1_count" == "1" ]] || { echo "tenant was not routed to shard 1" >&2; exit 1; }
[[ "$shard2_count" == "1" ]] || { echo "tenant was not routed to shard 2" >&2; exit 1; }

distributed_count=$(query clickhouse "SELECT count() FROM logs")
[[ "$distributed_count" == "2" ]] || { echo "expected distributed count 2, got $distributed_count" >&2; exit 1; }

merged_count=$(query clickhouse "SELECT count() FROM logs GROUP BY service FORMAT TSV")
[[ "$merged_count" == "2" ]] || { echo "expected merged aggregate 2, got $merged_count" >&2; exit 1; }

"${compose[@]}" stop clickhouse-shard2
partial_count=$(query clickhouse "SELECT count() FROM logs SETTINGS skip_unavailable_shards = 1")
[[ "$partial_count" == "1" ]] || { echo "expected best-effort count 1, got $partial_count" >&2; exit 1; }

echo "distributed ClickHouse integration checks passed"
