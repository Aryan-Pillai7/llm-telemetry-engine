-- One-minute rollup: the table dashboards actually read.
--
-- AggregatingMergeTree stores partial aggregate *states*, not finished numbers.
-- That is what makes the rollup composable: states merge, so the hourly rollup
-- can be built from this table instead of re-reading raw spans, and a query for
-- a 6-hour window merges 360 rows per series rather than scanning millions.
--
-- Cardinality is bounded by construction (ADR-005). Every dimension below is
-- registered in dimensions.yaml with an explicit budget, and the materialized
-- view rewrites unregistered values to `__other__`. The worst case is therefore
--
--     rows per minute <= product(budget + 1 for each dimension)
--
-- which is a number chosen in a reviewed file, not a property of whatever the
-- emitters happen to send. A test asserts the observed cardinality respects it.
--
-- Quantiles use tDigest states: mergeable, fixed memory, and accurate in the
-- tails where latency questions actually live. An exact quantile would require
-- keeping every observation, which defeats the purpose of a rollup.

CREATE TABLE IF NOT EXISTS telemetry.spans_1m
(
    ts_minute       DateTime            CODEC(Delta(4), ZSTD(1)),

    -- Registered dimensions only. Adding one here means adding it to
    -- dimensions.yaml and to the materialized view; the tests enforce parity.
    tenant_id       LowCardinality(String),
    tenant_tier     LowCardinality(String),
    model           LowCardinality(String),
    operation       LowCardinality(String),
    route           LowCardinality(String),
    region          LowCardinality(String),
    status_class    LowCardinality(String),

    -- Volume.
    spans           AggregateFunction(count),
    traces          AggregateFunction(uniq, String),

    -- Tokens: the cost signal.
    input_tokens    AggregateFunction(sum, UInt32),
    output_tokens   AggregateFunction(sum, UInt32),
    cached_tokens   AggregateFunction(sum, UInt32),

    -- Latency, as mergeable quantile states.
    ttft            AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), Float32),
    duration        AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), Float32),
    itl_avg         AggregateFunction(avg, Float32),
    queue_time_avg  AggregateFunction(avg, Float32),

    -- Serving pressure. max matters as much as avg: a minute that touched 0.99
    -- occupancy explains a latency spike that an average hides.
    kv_cache_avg    AggregateFunction(avg, Float32),
    kv_cache_max    AggregateFunction(max, Float32),

    -- Errors, kept as a state so error rate stays computable after merges.
    errors          AggregateFunction(countIf, UInt8)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(ts_minute)
-- Same leading key as spans_raw: tenant-scoped queries are the common case,
-- and the shared prefix keeps the mental model consistent across tiers.
ORDER BY (tenant_id, model, operation, status_class, route, region, tenant_tier, ts_minute)
-- Minute resolution is for recent, detailed views. Older questions are answered
-- by the hourly rollup and the Parquet cold tier.
TTL ts_minute + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192;
