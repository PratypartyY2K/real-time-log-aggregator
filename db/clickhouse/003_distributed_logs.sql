-- One-time migration from the single-node `logs` table to a two-shard
-- `logs_cluster`. Run this on the coordinator (`clickhouse`) after both shards
-- and ClickHouse Keeper are healthy. Pause processor writes during migration.

RENAME TABLE logs TO logs_single_node;

CREATE TABLE logs_local ON CLUSTER logs_cluster
(
    timestamp DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),
    tenant_id UInt64 CODEC(Delta(8), ZSTD(1)),
    service LowCardinality(String),
    environment LowCardinality(String),
    source LowCardinality(String),
    host String CODEC(ZSTD(1)),
    level LowCardinality(String),
    trace_id String CODEC(ZSTD(1)),
    fingerprint String CODEC(ZSTD(1)),
    message String CODEC(ZSTD(3)),
    fields_json String CODEC(ZSTD(3)),
    ingest_id String CODEC(ZSTD(1)),
    raw_size_bytes UInt32 CODEC(Delta(4), ZSTD(1)),
    INDEX ingest_id_bf ingest_id TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX trace_id_bf trace_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX error_code_bf JSONExtractString(fields_json, 'error_code') TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, timestamp, service)
TTL timestamp + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE logs
AS logs_local
ENGINE = Distributed(logs_cluster, currentDatabase(), logs_local, cityHash64(tenant_id));

INSERT INTO logs SETTINGS insert_distributed_sync = 1
SELECT * FROM logs_single_node;

-- Keep logs_single_node until row counts and tenant distribution are verified.
