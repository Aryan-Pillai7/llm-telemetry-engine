-- Rebuild mv_spans_1m to distinguish "absent" from "unregistered".
--
-- Supersedes 080_mv_spans_1m.sql, which is left untouched: editing an applied
-- migration trips the runner's drift detection, and correctly so. Replacing a
-- view is a new migration.
--
-- WHY THIS CHANGED. The first version mapped every value that failed the
-- allowlist check to `__other__`, including values that were simply absent. In
-- testing that put 18,243 tool spans (which legitimately have no model) into
-- the same bucket as exactly 300 spans from a genuinely unregistered model.
-- The bucket that was supposed to shout "something unregistered is emitting"
-- was 98% routine, so the real finding was invisible.
--
-- Now:
--   ''            -> __none__   the span does not carry this dimension. Normal.
--   not allowed   -> __other__  a value nobody registered. Actionable.
--   allowed       -> the value itself
--
-- The cardinality bound becomes budget + 2 per dimension rather than budget + 1.
-- Both sentinels are single literals, so the bound stays finite and chosen.
--
-- Dropping a materialized view does not touch the target table, so spans_1m
-- keeps every row already aggregated. Rows written before this change retain
-- the old bucketing; that is visible as a step in the __other__ series rather
-- than silent corruption.

DROP VIEW IF EXISTS telemetry.mv_spans_1m;

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry.mv_spans_1m
TO telemetry.spans_1m
AS
SELECT
    toStartOfMinute(ts)                                                      AS ts_minute,

    multiIf(tenant_id = '', '__none__',
            dictHas('telemetry.dim_allowlist_dict', ('tenant_id', tenant_id)),
            tenant_id, '__other__')                                          AS tenant_id,
    multiIf(tenant_tier = '', '__none__',
            dictHas('telemetry.dim_allowlist_dict', ('tenant_tier', tenant_tier)),
            tenant_tier, '__other__')                                        AS tenant_tier,
    multiIf(model = '', '__none__',
            dictHas('telemetry.dim_allowlist_dict', ('model', model)),
            model, '__other__')                                              AS model,
    multiIf(operation = '', '__none__',
            dictHas('telemetry.dim_allowlist_dict', ('operation', operation)),
            operation, '__other__')                                          AS operation,
    multiIf(route = '', '__none__',
            dictHas('telemetry.dim_allowlist_dict', ('route', route)),
            route, '__other__')                                              AS route,
    multiIf(region = '', '__none__',
            dictHas('telemetry.dim_allowlist_dict', ('region', region)),
            region, '__other__')                                             AS region,
    multiIf(status_class = '', '__none__',
            dictHas('telemetry.dim_allowlist_dict', ('status_class', status_class)),
            status_class, '__other__')                                       AS status_class,

    countState()                                                             AS spans,
    uniqState(trace_id)                                                      AS traces,

    sumState(input_tokens)                                                   AS input_tokens,
    sumState(output_tokens)                                                  AS output_tokens,
    sumState(cached_prompt_tokens)                                           AS cached_tokens,

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
