-- Version the export watermark by the watermark itself, not by wall clock.
--
-- Supersedes the engine choice in 130_cold_export_watermark.sql.
--
-- The original used ReplacingMergeTree(updated_at) with a second-resolution
-- DateTime. An export run advances the watermark several times inside one
-- second, so the version column ties and FINAL returns an arbitrary row among
-- them. Observed directly: an export that advanced the watermark to 20:04:30
-- was reported by `cold status` as sitting at 14:34:30.
--
-- Reading a STALE watermark is merely wasteful -- windows get re-exported, and
-- deterministic filenames make that harmless. Reading a watermark from the
-- wrong side of a tie could skip a window entirely, and a skipped window is
-- data that ceases to exist when the hot tier's TTL fires.
--
-- Versioning by `watermark` removes the tie by construction: the watermark only
-- ever moves forward, so "highest version wins" and "furthest progress wins"
-- become the same statement.

CREATE TABLE IF NOT EXISTS telemetry.cold_export_watermark_v2
(
    source      LowCardinality(String),
    watermark   DateTime,
    updated_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(watermark)
ORDER BY source;

INSERT INTO telemetry.cold_export_watermark_v2 (source, watermark)
SELECT source, max(watermark) FROM telemetry.cold_export_watermark GROUP BY source;

DROP TABLE IF EXISTS telemetry.cold_export_watermark;

RENAME TABLE telemetry.cold_export_watermark_v2 TO telemetry.cold_export_watermark;
