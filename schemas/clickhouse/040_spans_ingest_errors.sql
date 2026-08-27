-- Dead-letter table for messages the Kafka engine could not parse.
--
-- The Kafka table is declared with kafka_handle_error_mode = 'stream', which
-- turns a parse failure into a row with the _error virtual column set instead
-- of an exception. Without somewhere to put those rows, the choice would be
-- between silently discarding bad messages and stalling the consumer group on
-- the first one -- and a stalled consumer means lag on every partition, which
-- looks identical to a broker problem while actually being a data problem.
--
-- In practice this should stay empty: JSONAsString accepts any byte sequence,
-- so only a truncated or non-UTF-8 message lands here. It exists because "this
-- should stay empty" is a claim worth being able to check.

CREATE TABLE IF NOT EXISTS telemetry.spans_ingest_errors
(
    ingested_at DateTime DEFAULT now() CODEC(Delta(4), ZSTD(1)),
    topic       LowCardinality(String),
    partition   UInt16,
    offset      UInt64,
    error       String CODEC(ZSTD(1)),
    -- Truncated: a dead-letter table should not become the largest table in the
    -- database during an incident.
    raw_message String CODEC(ZSTD(3))
)
ENGINE = MergeTree
ORDER BY (ingested_at, topic, partition)
TTL ingested_at + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192;
