"""Schema invariants, checked without a ClickHouse connection.

These read the shipped DDL as text. That is deliberately crude, but it catches
the failure mode that matters: the SQL and the Python settings drifting apart so
that the pipeline is tuned one way and configured another.
"""

from __future__ import annotations

import re

import pytest

from telemetry_engine.config import Settings
from telemetry_engine.emitters import attributes as A
from telemetry_engine.ingest.topics import load_specs
from telemetry_engine.storage.migrations import discover


@pytest.fixture(scope="module")
def schema_sql() -> dict[str, str]:
    """Every shipped migration, keyed by filename."""
    settings = Settings()
    return {m.name: m.sql for m in discover(settings.schemas_dir)}


def _sql_for(schema_sql: dict[str, str], fragment: str) -> str:
    for name, sql in schema_sql.items():
        if fragment in name:
            return sql
    raise AssertionError(f"no migration matching {fragment!r} in {sorted(schema_sql)}")


# --- Kafka consumer settings --------------------------------------------------


def test_kafka_consumers_do_not_exceed_partitions(schema_sql: dict[str, str]) -> None:
    """Consumers beyond the partition count sit idle forever.

    Not an error, just wasted threads from a pool that the Kafka engine shares
    with everything else -- and a misleading signal when debugging lag.
    """
    sql = _sql_for(schema_sql, "kafka_spans")
    match = re.search(r"kafka_num_consumers\s*=\s*(\d+)", sql)
    assert match, "kafka_num_consumers should be set explicitly, not left to default"
    consumers = int(match.group(1))

    settings = Settings()
    spec = next(s for s in load_specs() if s.name == settings.redpanda.spans_topic)
    assert consumers <= spec.partitions, (
        f"{consumers} consumers for {spec.partitions} partitions; the surplus will idle"
    )


def test_kafka_table_reads_the_configured_topic(schema_sql: dict[str, str]) -> None:
    sql = _sql_for(schema_sql, "kafka_spans")
    settings = Settings()
    assert settings.redpanda.spans_topic in sql


def test_kafka_error_mode_is_stream(schema_sql: dict[str, str]) -> None:
    """A parse failure must not stall the consumer group.

    With the default error mode, one malformed message halts ingestion for its
    whole partition -- which presents as broker-side lag while actually being a
    data problem, and is the worse of the two failures by a wide margin.
    """
    sql = _sql_for(schema_sql, "kafka_spans")
    assert "kafka_handle_error_mode = 'stream'" in sql


def test_kafka_uses_json_as_string(schema_sql: dict[str, str]) -> None:
    """Parsing happens in the MV, where the failure surface is SQL we control."""
    sql = _sql_for(schema_sql, "kafka_spans")
    assert "kafka_format = 'JSONAsString'" in sql


def test_dead_letter_view_exists_for_the_error_stream(schema_sql: dict[str, str]) -> None:
    """kafka_handle_error_mode='stream' is pointless without somewhere to route to."""
    sql = _sql_for(schema_sql, "mv_ingest_errors")
    assert "_error" in sql
    assert "spans_ingest_errors" in sql


# --- Raw table ----------------------------------------------------------------


def test_raw_table_orders_by_tenant_first(schema_sql: dict[str, str]) -> None:
    """ADR-005: nearly every query is tenant-scoped.

    Leading the sort key with tenant_id is what lets ClickHouse skip granules
    belonging to other tenants instead of scanning the partition.
    """
    sql = _sql_for(schema_sql, "spans_raw")
    match = re.search(r"ORDER BY \(([^)]+)\)", sql)
    assert match, "spans_raw must declare an explicit ORDER BY"
    first_key = match.group(1).split(",")[0].strip()
    assert first_key == "tenant_id"


def test_raw_ttl_matches_configured_retention(schema_sql: dict[str, str]) -> None:
    """The DDL and config.py must agree on how long raw spans live.

    They are edited by different people at different times; if they disagree,
    the cold-tier export window is wrong and data is dropped before it is
    exported.
    """
    sql = _sql_for(schema_sql, "spans_raw")
    match = re.search(r"TTL\s+.*?INTERVAL\s+(\d+)\s+HOUR", sql, re.IGNORECASE)
    assert match, "spans_raw must declare a TTL; raw telemetry cannot accumulate forever"
    assert int(match.group(1)) == Settings().clickhouse.raw_ttl_hours


def test_raw_table_is_partitioned_by_day(schema_sql: dict[str, str]) -> None:
    """TTL drops whole partitions cheaply; daily granularity keeps that true."""
    sql = _sql_for(schema_sql, "spans_raw")
    assert "PARTITION BY toYYYYMMDD(ts)" in sql


def test_high_cardinality_columns_are_not_low_cardinality(schema_sql: dict[str, str]) -> None:
    """LowCardinality on an unbounded column is a pathology.

    Its dictionary grows without bound per part, costing memory and making
    merges progressively slower -- the opposite of the intended optimization.
    """
    sql = _sql_for(schema_sql, "spans_raw")
    for column in ("trace_id", "span_id", "parent_span_id", "request_id"):
        pattern = rf"^\s*{column}\s+LowCardinality"
        assert not re.search(pattern, sql, re.MULTILINE), (
            f"{column} is unbounded and must not be LowCardinality"
        )


def test_bounded_dimensions_are_low_cardinality(schema_sql: dict[str, str]) -> None:
    """The converse: rollup dimensions should get dictionary encoding."""
    sql = _sql_for(schema_sql, "spans_raw")
    for column in ("tenant_id", "model", "operation", "route", "region", "status_class"):
        assert re.search(rf"^\s*{column}\s+LowCardinality", sql, re.MULTILINE), (
            f"{column} is a bounded dimension and should be LowCardinality"
        )


def test_trace_lookup_has_a_skip_index(schema_sql: dict[str, str]) -> None:
    """trace_id is not in the sort key, so a point lookup needs an index."""
    sql = _sql_for(schema_sql, "spans_raw")
    assert "INDEX idx_trace_id" in sql
    assert "bloom_filter" in sql


def test_raw_table_keeps_an_open_attribute_map(schema_sql: dict[str, str]) -> None:
    """A new signal type must be additive, not a schema rewrite (ADR-006)."""
    sql = _sql_for(schema_sql, "spans_raw")
    assert re.search(r"attributes\s+Map\(", sql)


# --- Materialized view --------------------------------------------------------


def test_mv_extracts_every_rollup_dimension(schema_sql: dict[str, str]) -> None:
    """Every attribute allowed to be a rollup key must survive parsing.

    A dimension the emitter sets but the MV drops would silently become an
    empty string in every aggregate -- the kind of bug that shows up as a
    dashboard panel that is merely wrong rather than broken.
    """
    sql = _sql_for(schema_sql, "mv_spans_raw")
    for key in A.ROLLUP_DIMENSION_KEYS:
        assert f"'{key}'" in sql, f"MV does not extract rollup dimension {key}"


def test_mv_extraction_is_total(schema_sql: dict[str, str]) -> None:
    """Numeric conversions must not raise: an exception stalls consumption.

    Every numeric attribute crosses the wire as a string, so each conversion
    has to be an OrZero variant.
    """
    sql = _sql_for(schema_sql, "mv_spans_raw")
    bare = re.findall(r"\btoUInt\d+\((?!.*OrZero)|\btoFloat\d+\((?!.*OrZero)", sql)
    assert not bare, f"non-total numeric conversions in the MV would stall Kafka: {bare}"


def test_mv_filters_out_error_rows(schema_sql: dict[str, str]) -> None:
    """Error-stream rows have no span payload and must not reach spans_raw."""
    sql = _sql_for(schema_sql, "mv_spans_raw")
    assert "_error" in sql
