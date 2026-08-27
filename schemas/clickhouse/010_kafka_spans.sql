-- Kafka table engine: ClickHouse consumes Redpanda directly (ADR-002).
--
-- Format is JSONAsString: the whole OTLP JSON message lands in one String
-- column and is parsed by the materialized view. The alternative -- declaring
-- the nested OTLP structure as ClickHouse columns -- means the schema breaks
-- whenever the collector's encoder changes shape, and OTLP's
-- resourceSpans/scopeSpans/spans nesting does not map cleanly onto columns
-- anyway. Parsing in the MV keeps the failure surface in SQL we control.
--
-- A Kafka engine table is a *consumer*, not storage. Selecting from it directly
-- consumes messages and moves the offset; always query spans_raw instead.

CREATE TABLE IF NOT EXISTS telemetry.kafka_spans
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'otel.spans',
    kafka_group_name = 'clickhouse-spans',
    kafka_format = 'JSONAsString',

    -- Consumers, bounded by two things: the topic has 6 partitions (more
    -- consumers than partitions just idle), and every consumer needs a thread
    -- from background_message_broker_schedule_pool_size (8, see
    -- deploy/clickhouse/config.d/limits.xml). Three leaves room for the merge
    -- work that ingestion creates.
    kafka_num_consumers = 3,
    kafka_thread_per_consumer = 1,

    -- Block sizing in MESSAGES, not spans. The collector batches up to 1024
    -- spans per message, so 2048 messages is on the order of 200k rows per
    -- insert -- large enough that MergeTree part counts stay sane, small enough
    -- that one block does not blow the memory budget.
    kafka_max_block_size = 2048,
    kafka_poll_max_batch_size = 1024,

    -- Upper bound on how long a partial block waits before being written.
    -- Trades ingest latency for part count; 3s keeps dashboards near-live
    -- without producing a part every few hundred milliseconds.
    kafka_flush_interval_ms = 3000,

    -- Malformed messages become rows with _error set instead of stalling the
    -- consumer. Without this, one bad message halts ingestion for the whole
    -- partition, which is a far worse failure than dropping it into a
    -- dead-letter table (see 040_spans_ingest_errors.sql).
    kafka_handle_error_mode = 'stream';
