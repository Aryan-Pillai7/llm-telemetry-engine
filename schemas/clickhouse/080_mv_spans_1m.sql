-- Rollup view: spans_raw -> spans_1m, applying the cardinality guard.
--
-- This is where ADR-005 is enforced. Every dimension is passed through
-- `dictHas` against the allowlist; anything not registered becomes `__other__`.
-- The guard runs per row inside ClickHouse because there is no Python process
-- on the ingest path (ADR-002) -- and because a guard that can be bypassed is
-- not a guard.
--
-- Chained materialized view: this fires on inserts into spans_raw, which are
-- themselves produced by mv_spans_raw consuming Kafka. Each insert block is
-- pre-aggregated here, so the rollup costs one grouped pass over a block
-- already in memory rather than a separate scan.
--
-- If the allowlist is empty, every value collapses to `__other__` and the
-- rollup is useless but still bounded -- a legible failure. Run
-- `telemetry-engine dimensions apply` after a fresh bootstrap.

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry.mv_spans_1m
TO telemetry.spans_1m
AS
SELECT
    toStartOfMinute(ts)                                                      AS ts_minute,

    -- The guard, applied uniformly. `dictHas` on a COMPLEX_KEY_HASHED
    -- dictionary is an in-memory probe, cheap enough to run on every row of
    -- every dimension.
    if(dictHas('telemetry.dim_allowlist_dict', ('tenant_id', tenant_id)),
       tenant_id, '__other__')                                               AS tenant_id,
    if(dictHas('telemetry.dim_allowlist_dict', ('tenant_tier', tenant_tier)),
       tenant_tier, '__other__')                                             AS tenant_tier,
    if(dictHas('telemetry.dim_allowlist_dict', ('model', model)),
       model, '__other__')                                                   AS model,
    if(dictHas('telemetry.dim_allowlist_dict', ('operation', operation)),
       operation, '__other__')                                               AS operation,
    if(dictHas('telemetry.dim_allowlist_dict', ('route', route)),
       route, '__other__')                                                   AS route,
    if(dictHas('telemetry.dim_allowlist_dict', ('region', region)),
       region, '__other__')                                                  AS region,
    if(dictHas('telemetry.dim_allowlist_dict', ('status_class', status_class)),
       status_class, '__other__')                                            AS status_class,

    countState()                                                             AS spans,
    uniqState(trace_id)                                                      AS traces,

    sumState(input_tokens)                                                   AS input_tokens,
    sumState(output_tokens)                                                  AS output_tokens,
    sumState(cached_prompt_tokens)                                           AS cached_tokens,

    -- Only spans that actually measured a latency contribute. Including the
    -- zeros from tool and agent spans would drag every percentile toward zero
    -- and make the dashboards quietly wrong.
    quantilesTDigestStateIf(0.5, 0.95, 0.99)(ttft_ms, ttft_ms > 0)           AS ttft,
    quantilesTDigestStateIf(0.5, 0.95, 0.99)(
        toFloat32(duration_ms), duration_ms > 0)                             AS duration,
    avgStateIf(itl_ms, itl_ms > 0)                                           AS itl_avg,
    avgStateIf(queue_time_ms, queue_time_ms > 0)                             AS queue_time_avg,

    avgStateIf(kv_cache_utilization, kv_cache_utilization > 0)               AS kv_cache_avg,
    maxState(kv_cache_utilization)                                           AS kv_cache_max,

    countIfState(toUInt8(status_class != 'ok'), status_class != 'ok')        AS errors
FROM telemetry.spans_raw
GROUP BY
    ts_minute,
    tenant_id,
    tenant_tier,
    model,
    operation,
    route,
    region,
    status_class;
