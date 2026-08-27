-- Materialized view: Kafka -> parsed rows in spans_raw.
--
-- This is the only place OTLP's wire format is decoded, and it is the piece
-- most likely to need changing when the collector's encoder changes. Read it
-- inside out:
--
--   1. one Kafka message contains resourceSpans[]
--   2. each resourceSpan contains scopeSpans[]
--   3. each scopeSpan contains spans[]
--
-- so three nested arrayJoins flatten a message into one row per span. A single
-- message routinely carries several hundred spans, which is exactly why the
-- Kafka block size is expressed in messages, not rows.
--
-- OTLP attributes arrive as an array of {"key":..., "value":{<type>: ...}}.
-- They are converted to a Map once, then typed columns are pulled out of that
-- map. Doing it once matters: the naive alternative re-scans the attribute
-- array for every column extracted.
--
-- IMPORTANT: if this view raises, Kafka consumption stalls for the whole
-- consumer group. Every extraction below is therefore total -- the OrZero
-- variants, and Map lookups that yield an empty string on a miss -- so a
-- malformed or unexpected span produces a row with empty fields rather than
-- halting ingestion for everyone.

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry.mv_spans_raw
TO telemetry.spans_raw
AS
SELECT
    CAST(fromUnixTimestamp64Nano(toInt64OrZero(start_ns)), 'DateTime64(3)')  AS ts,

    JSONExtractString(span_json, 'traceId')                                  AS trace_id,
    JSONExtractString(span_json, 'spanId')                                   AS span_id,
    JSONExtractString(span_json, 'parentSpanId')                             AS parent_span_id,
    JSONExtractString(span_json, 'name')                                     AS span_name,

    -- OTLP encodes span kind as an enum ordinal; map it back to something a
    -- dashboard can display without a lookup table.
    multiIf(
        kind_ordinal = 1, 'internal',
        kind_ordinal = 2, 'server',
        kind_ordinal = 3, 'client',
        kind_ordinal = 4, 'producer',
        kind_ordinal = 5, 'consumer',
        'unspecified'
    )                                                                        AS span_kind,

    (toInt64OrZero(end_ns) - toInt64OrZero(start_ns)) / 1000000.0            AS duration_ms,

    -- Bounded dimensions.
    attrs['tenant.id']                                                       AS tenant_id,
    attrs['tenant.tier']                                                     AS tenant_tier,
    attrs['gen_ai.request.model']                                            AS model,
    attrs['gen_ai.operation.name']                                           AS operation,
    attrs['http.route']                                                      AS route,
    attrs['cloud.region']                                                    AS region,
    attrs['status.class']                                                    AS status_class,
    resource_attrs['service.name']                                           AS service_name,

    -- Measurements. Numeric OTLP attributes cross the wire as strings, so
    -- these are all total conversions.
    toUInt32OrZero(attrs['gen_ai.usage.input_tokens'])                       AS input_tokens,
    toUInt32OrZero(attrs['gen_ai.usage.output_tokens'])                      AS output_tokens,
    toUInt32OrZero(attrs['llm.cached_prompt_tokens'])                        AS cached_prompt_tokens,
    toFloat32OrZero(attrs['llm.kv_cache.utilization'])                       AS kv_cache_utilization,
    toFloat32OrZero(attrs['llm.time_to_first_token_ms'])                     AS ttft_ms,
    toFloat32OrZero(attrs['llm.inter_token_latency_ms'])                     AS itl_ms,
    toFloat32OrZero(attrs['llm.queue_time_ms'])                              AS queue_time_ms,
    toUInt16OrZero(attrs['llm.batch_size'])                                  AS batch_size,

    attrs['request.id']                                                      AS request_id,
    attrs['error.type']                                                      AS error_type,

    -- The full attribute map is kept so a newly emitted attribute is queryable
    -- immediately, before anyone decides it deserves a typed column.
    attrs                                                                    AS attributes,

    now()                                                                    AS ingested_at
FROM
(
    SELECT
        span_json,
        resource_attrs,
        JSONExtractString(span_json, 'startTimeUnixNano')                    AS start_ns,
        JSONExtractString(span_json, 'endTimeUnixNano')                      AS end_ns,
        JSONExtractUInt(span_json, 'kind')                                   AS kind_ordinal,
        CAST(
            (
                arrayMap(a -> JSONExtractString(a, 'key'), span_attr_array),
                arrayMap(a -> multiIf(
                    JSONHas(a, 'value', 'stringValue'), JSONExtractString(a, 'value', 'stringValue'),
                    JSONHas(a, 'value', 'intValue'),    JSONExtractString(a, 'value', 'intValue'),
                    JSONHas(a, 'value', 'doubleValue'), toString(JSONExtractFloat(a, 'value', 'doubleValue')),
                    JSONHas(a, 'value', 'boolValue'),   if(JSONExtractBool(a, 'value', 'boolValue'), 'true', 'false'),
                    ''
                ), span_attr_array)
            ),
            'Map(String, String)'
        )                                                                    AS attrs
    FROM
    (
        SELECT
            span_json,
            resource_attrs,
            JSONExtractArrayRaw(span_json, 'attributes')                     AS span_attr_array
        FROM
        (
            SELECT
                arrayJoin(JSONExtractArrayRaw(scope_json, 'spans'))          AS span_json,
                resource_attrs
            FROM
            (
                SELECT
                    arrayJoin(JSONExtractArrayRaw(rs_json, 'scopeSpans'))    AS scope_json,
                    resource_attrs
                FROM
                (
                    SELECT
                        rs_json,
                        CAST(
                            (
                                arrayMap(a -> JSONExtractString(a, 'key'), resource_attr_array),
                                arrayMap(
                                    a -> JSONExtractString(a, 'value', 'stringValue'),
                                    resource_attr_array
                                )
                            ),
                            'Map(String, String)'
                        )                                                    AS resource_attrs
                    FROM
                    (
                        SELECT
                            rs_json,
                            JSONExtractArrayRaw(
                                JSONExtractRaw(rs_json, 'resource'), 'attributes'
                            )                                                AS resource_attr_array
                        FROM
                        (
                            SELECT arrayJoin(JSONExtractArrayRaw(raw, 'resourceSpans')) AS rs_json
                            FROM telemetry.kafka_spans
                            WHERE _error = ''
                        )
                    )
                )
            )
        )
    )
);
