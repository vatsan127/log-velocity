CREATE TABLE logs
(
    thread_name     String,
    application     LowCardinality(String),
    instance_name   LowCardinality(String),
    log_level       LowCardinality(String),
    timestamp       DateTime64(3),
    sequence_number UInt64,
    trace_id        String,
    log_details     String
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (trace_id, timestamp, sequence_number)
TTL toDateTime(timestamp) + INTERVAL 3 DAY;

CREATE TABLE error_events
(
    thread_name     String,
    application     LowCardinality(String),
    instance_name   LowCardinality(String),
    log_level       LowCardinality(String),
    timestamp       DateTime64(3),
    sequence_number UInt64,
    trace_id        String,
    log_details     String
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (trace_id, timestamp, sequence_number)
TTL toDateTime(timestamp) + INTERVAL 14 DAY;
