-- Hourly rollup, built from the minute rollup rather than from raw spans.
--
-- This is the payoff of storing aggregate *states* instead of finished numbers:
-- the hourly view merges 60 minute-states per series, which costs a grouped
-- pass over a small table. Re-deriving hourly figures from spans_raw would mean
-- rescanning millions of rows, and would stop working the moment raw data hits
-- its 48-hour TTL.
--
-- Same dimensions as spans_1m, so the same cardinality bound applies -- with the
-- time bucket 60x coarser, this table is roughly 60x smaller per series. It is
-- what answers questions older than a week, and what the cold-tier export reads.

CREATE TABLE IF NOT EXISTS telemetry.spans_1h
(
    ts_hour         DateTime            CODEC(Delta(4), ZSTD(1)),

    tenant_id       LowCardinality(String),
    tenant_tier     LowCardinality(String),
    model           LowCardinality(String),
    operation       LowCardinality(String),
    route           LowCardinality(String),
    region          LowCardinality(String),
    status_class    LowCardinality(String),

    spans           AggregateFunction(count),
    traces          AggregateFunction(uniq, String),

    input_tokens    AggregateFunction(sum, UInt32),
    output_tokens   AggregateFunction(sum, UInt32),
    cached_tokens   AggregateFunction(sum, UInt32),

    ttft            AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), Float32),
    duration        AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), Float32),
    itl_avg         AggregateFunction(avg, Float32),
    queue_time_avg  AggregateFunction(avg, Float32),

    kv_cache_avg    AggregateFunction(avg, Float32),
    kv_cache_max    AggregateFunction(max, Float32),

    errors          AggregateFunction(countIf, UInt8)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(ts_hour)
ORDER BY (tenant_id, model, operation, status_class, route, region, tenant_tier, ts_hour)
-- 90 days of hourly history is cheap and covers quarter-over-quarter questions.
-- Anything older lives in Parquet.
TTL ts_hour + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;
