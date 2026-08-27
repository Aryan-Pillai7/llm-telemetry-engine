-- Pipeline SLIs: consumer lag and collector drop counters over time.
--
-- Lag is a broker fact and drops are a collector fact, so neither is in
-- ClickHouse naturally. Grafana in this stack has one datasource, so rather
-- than running a Prometheus container to graph two numbers, a sampler
-- (`telemetry-engine monitor`) reads both and writes them here.
--
-- Counters are stored RAW and cumulative, not as rates. A rate computed at
-- write time bakes in the sampling interval and cannot be re-derived; a
-- dashboard can always difference a counter, but it cannot un-average a rate.
--
-- Phase 6's backpressure experiment reads this same table, so the numbers on
-- the dashboard and the numbers in the write-up cannot disagree.

CREATE TABLE IF NOT EXISTS telemetry.pipeline_health
(
    sampled_at              DateTime DEFAULT now() CODEC(Delta(4), ZSTD(1)),

    -- Redpanda consumer lag for the ClickHouse consumer group. The primary SLI:
    -- under the design's backpressure model, overload shows up here first.
    total_lag               UInt64 CODEC(T64, ZSTD(1)),
    max_partition_lag       UInt64 CODEC(T64, ZSTD(1)),
    partitions              UInt16,

    -- Collector counters, cumulative since its start.
    accepted_spans          UInt64 CODEC(T64, ZSTD(1)),
    refused_spans           UInt64 CODEC(T64, ZSTD(1)),
    sent_spans              UInt64 CODEC(T64, ZSTD(1)),
    -- Real, deliberate loss: retries exhausted, or the non-blocking queue
    -- rejecting a batch rather than pushing back on the endpoint (ADR-003).
    send_failed_spans       UInt64 CODEC(T64, ZSTD(1)),
    enqueue_failed_spans    UInt64 CODEC(T64, ZSTD(1)),

    queue_size              UInt32,
    queue_capacity          UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(sampled_at)
ORDER BY sampled_at
TTL sampled_at + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;
