-- Route unparseable Kafka messages into the dead-letter table.
--
-- Reads the same Kafka table as mv_spans_raw but selects the complement:
-- rows where _error is set. Both views attach to the same consumer group, so a
-- message is delivered to both and each keeps the half it cares about.

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry.mv_spans_ingest_errors
TO telemetry.spans_ingest_errors
AS
SELECT
    now()                       AS ingested_at,
    _topic                      AS topic,
    _partition                  AS partition,
    _offset                     AS offset,
    _error                      AS error,
    -- Cap the retained payload. Enough to identify the producer and the shape
    -- of the failure, not enough to make this table a storage problem.
    substring(_raw_message, 1, 4096) AS raw_message
FROM telemetry.kafka_spans
WHERE _error != '';
