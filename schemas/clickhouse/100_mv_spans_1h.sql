-- Roll the minute table up into the hour table.
--
-- The -MergeState combinator is the key: it consumes existing aggregate states
-- and produces a new state of the same type, so accuracy is preserved through
-- the cascade. Using -State here instead would try to aggregate the state
-- columns as if they were raw values, and using -Merge would produce finished
-- numbers that an AggregatingMergeTree cannot store.

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry.mv_spans_1h
TO telemetry.spans_1h
AS
SELECT
    toStartOfHour(ts_minute)        AS ts_hour,

    -- Already guarded on the way into spans_1m; no second check needed, and a
    -- second check would be a second place to keep in sync.
    tenant_id,
    tenant_tier,
    model,
    operation,
    route,
    region,
    status_class,

    countMergeState(spans)          AS spans,
    uniqMergeState(traces)          AS traces,

    sumMergeState(input_tokens)     AS input_tokens,
    sumMergeState(output_tokens)    AS output_tokens,
    sumMergeState(cached_tokens)    AS cached_tokens,

    quantilesTDigestMergeState(0.5, 0.95, 0.99)(ttft)     AS ttft,
    quantilesTDigestMergeState(0.5, 0.95, 0.99)(duration) AS duration,
    avgMergeState(itl_avg)          AS itl_avg,
    avgMergeState(queue_time_avg)   AS queue_time_avg,

    avgMergeState(kv_cache_avg)     AS kv_cache_avg,
    maxMergeState(kv_cache_max)     AS kv_cache_max,

    countIfMergeState(errors)       AS errors
FROM telemetry.spans_1m
GROUP BY
    ts_hour,
    tenant_id,
    tenant_tier,
    model,
    operation,
    route,
    region,
    status_class;
