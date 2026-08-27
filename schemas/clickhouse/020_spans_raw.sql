-- Raw span landing table: the hot tier's high-cardinality layer.
--
-- This is where unbounded identifiers live (trace_id, span_id, request_id,
-- prompt hash). It is queried for debugging individual traces and exported to
-- the cold tier, but it is NEVER grouped by -- that is what the rollups in
-- Phase 4 are for (ADR-006).
--
-- Its TTL is short by design. Raw telemetry is expensive per byte and loses
-- value fast; the aggregates and the Parquet cold tier are what survive.

CREATE TABLE IF NOT EXISTS telemetry.spans_raw
(
    -- Event time from the span itself, not ingest time. Delta+ZSTD because
    -- timestamps within a part are near-sorted and compress extremely well.
    ts              DateTime64(3)   CODEC(Delta(8), ZSTD(1)),

    -- Identifiers: high cardinality, deliberately not LowCardinality.
    trace_id        String          CODEC(ZSTD(1)),
    span_id         String          CODEC(ZSTD(1)),
    parent_span_id  String          CODEC(ZSTD(1)),

    span_name       LowCardinality(String),
    span_kind       LowCardinality(String),
    duration_ms     Float32         CODEC(Gorilla, ZSTD(1)),

    -- Bounded dimensions. These are the only columns allowed to become rollup
    -- keys, and LowCardinality gives them dictionary encoding per part.
    tenant_id       LowCardinality(String),
    tenant_tier     LowCardinality(String),
    model           LowCardinality(String),
    operation       LowCardinality(String),
    route           LowCardinality(String),
    region          LowCardinality(String),
    status_class    LowCardinality(String),
    service_name    LowCardinality(String),

    -- LLM/agent measurements. Gorilla suits slowly-varying float series.
    input_tokens            UInt32  CODEC(T64, ZSTD(1)),
    output_tokens           UInt32  CODEC(T64, ZSTD(1)),
    cached_prompt_tokens    UInt32  CODEC(T64, ZSTD(1)),
    kv_cache_utilization    Float32 CODEC(Gorilla, ZSTD(1)),
    ttft_ms                 Float32 CODEC(Gorilla, ZSTD(1)),
    itl_ms                  Float32 CODEC(Gorilla, ZSTD(1)),
    queue_time_ms           Float32 CODEC(Gorilla, ZSTD(1)),
    batch_size              UInt16  CODEC(T64, ZSTD(1)),

    -- High-cardinality debugging context.
    request_id      String          CODEC(ZSTD(1)),
    error_type      LowCardinality(String),

    -- Everything not promoted to a typed column above. This is what makes a new
    -- signal type additive rather than a rewrite: emit a new attribute and it
    -- lands here immediately, then gets promoted to a real column only if it
    -- earns one.
    attributes      Map(LowCardinality(String), String),

    -- Ingest time, for measuring end-to-end pipeline latency against ts.
    ingested_at     DateTime        DEFAULT now() CODEC(Delta(4), ZSTD(1)),

    -- Trace lookup is a point query against a high-cardinality column that is
    -- not in the sort key. A bloom filter makes it skip most granules instead
    -- of scanning the partition.
    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_request_id request_id TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
-- Tenant first: nearly every query is tenant-scoped, and leading with it lets
-- ClickHouse skip whole granules for other tenants. Model second because
-- per-model breakdowns are the next most common filter (ADR-005).
ORDER BY (tenant_id, model, ts)
TTL toDateTime(ts) + INTERVAL 48 HOUR DELETE
SETTINGS index_granularity = 8192;
