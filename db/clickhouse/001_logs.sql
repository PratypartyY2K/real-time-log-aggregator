CREATE TABLE IF NOT EXISTS logs
(
    timestamp DateTime64(3, 'UTC'),
    tenant_id UInt64,
    service LowCardinality(String),
    environment LowCardinality(String),
    source LowCardinality(String),
    host String,
    level LowCardinality(String),
    trace_id String,
    fingerprint String,
    message String,
    fields_json String,
    ingest_id String,
    raw_size_bytes UInt32
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, service, environment, timestamp, level)
TTL timestamp + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;
