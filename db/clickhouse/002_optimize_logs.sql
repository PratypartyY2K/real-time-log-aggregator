-- One-time migration for installations created with 001_logs.sql before the
-- time/service layout was introduced. Stop processor writes while this runs.
-- The old table is retained as logs_before_optimization for rollback/validation.

CREATE TABLE logs_optimized
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
ORDER BY (timestamp, service)
TTL timestamp + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

INSERT INTO logs_optimized SELECT * FROM logs;

RENAME TABLE logs TO logs_before_optimization, logs_optimized TO logs;
