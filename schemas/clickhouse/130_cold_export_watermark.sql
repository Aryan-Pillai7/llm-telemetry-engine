-- Cold-tier export watermark: how far the Parquet lake has caught up.
--
-- The exporter is the one job whose failure is permanent. The hot tier drops
-- raw spans after 48 hours, so a window that is never exported is data that
-- ceases to exist. The watermark is therefore advanced ONLY after an exported
-- file has been written, read back, and verified against the source.
--
-- ReplacingMergeTree keyed on the source name: one current value per source,
-- with older values collapsing away on merge. Queries use FINAL.

CREATE TABLE IF NOT EXISTS telemetry.cold_export_watermark
(
    source      LowCardinality(String),
    -- Exclusive upper bound: everything with ts < watermark is in the lake.
    watermark   DateTime,
    updated_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY source;
