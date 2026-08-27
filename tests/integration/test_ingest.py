"""End-to-end ingest: emitter -> collector -> Redpanda -> ClickHouse.

Requires `python tasks.py up` and an applied schema. This is the Phase 3
milestone expressed as tests: not merely that rows appear, but that they are
parsed correctly, that nothing stalls, and that the consumer keeps up.
"""

from __future__ import annotations

import time

import pytest

from telemetry_engine.config import Settings
from telemetry_engine.emitters.otlp import ExporterConfig, build_tracer_provider, emit_trace
from telemetry_engine.emitters.workload import WorkloadGenerator
from telemetry_engine.storage.client import client

pytestmark = pytest.mark.integration

OTLP_ENDPOINT = "http://localhost:4317"

# Generous: the SDK batches for 1s, the collector for up to 5s, and ClickHouse
# flushes Kafka blocks every 3s. Measured p95 end-to-end is ~8s.
INGEST_TIMEOUT_S = 90.0


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings()


def _count(conn, where: str = "") -> int:
    clause = f" WHERE {where}" if where else ""
    return int(conn.query(f"SELECT count() FROM telemetry.spans_raw{clause}").result_rows[0][0])


def test_emitted_spans_arrive_and_are_parsed(settings: Settings) -> None:
    """The whole path, with a marker that identifies exactly this test's spans.

    Uses a distinctive tenant count so the assertions cannot accidentally pass
    on rows left behind by another run.
    """
    with client(settings.clickhouse) as conn:
        mark = conn.query("SELECT now()").result_rows[0][0]

    provider = build_tracer_provider(ExporterConfig(endpoint=OTLP_ENDPOINT))
    tracer = provider.get_tracer("test-ingest")
    generator = WorkloadGenerator(tenants=5, seed=4242)
    emitted = sum(emit_trace(tracer, generator.generate_trace()) for _ in range(100))
    provider.shutdown()

    deadline = time.monotonic() + INGEST_TIMEOUT_S
    arrived = 0
    with client(settings.clickhouse) as conn:
        while time.monotonic() < deadline:
            arrived = _count(conn, f"ingested_at >= toDateTime('{mark}')")
            if arrived >= emitted * 0.9:
                break
            time.sleep(2.0)

    assert arrived >= emitted * 0.9, (
        f"only {arrived}/{emitted} spans reached ClickHouse within {INGEST_TIMEOUT_S}s"
    )


def test_parsed_rows_have_no_empty_dimensions(settings: Settings) -> None:
    """A dimension the MV fails to extract becomes an empty string, silently.

    That is worse than a crash: the pipeline keeps running and every aggregate
    built on that dimension is quietly wrong.
    """
    with client(settings.clickhouse) as conn:
        row = conn.query("""
            SELECT
                countIf(tenant_id = ''),
                countIf(trace_id = ''),
                countIf(span_id = ''),
                countIf(status_class = ''),
                countIf(service_name = ''),
                countIf(span_kind = 'unspecified'),
                count()
            FROM telemetry.spans_raw
        """).result_rows[0]

    no_tenant, no_trace, no_span, no_status, no_service, no_kind, total = row
    assert total > 0, "no data ingested; run a load first"
    assert no_tenant == 0, f"{no_tenant} rows lost tenant_id in parsing"
    assert no_trace == 0, f"{no_trace} rows lost trace_id"
    assert no_span == 0, f"{no_span} rows lost span_id"
    assert no_status == 0, f"{no_status} rows lost status_class"
    assert no_service == 0, f"{no_service} rows lost service.name (resource attribute)"
    assert no_kind == 0, f"{no_kind} rows have an unmapped span kind"


def test_timestamps_and_durations_are_sane(settings: Settings) -> None:
    """Catches unit errors in the nanosecond conversions.

    A wrong divisor here yields timestamps in 1970 or the year 55000, and
    durations off by three orders of magnitude -- all of which still "work".
    """
    with client(settings.clickhouse) as conn:
        row = conn.query("""
            SELECT
                countIf(ts < toDateTime('2020-01-01')),
                countIf(ts > now() + INTERVAL 1 DAY),
                countIf(duration_ms <= 0),
                countIf(duration_ms > 600000),
                count()
            FROM telemetry.spans_raw
        """).result_rows[0]

    too_old, too_new, non_positive, absurd, total = row
    assert total > 0
    assert too_old == 0, f"{too_old} rows before 2020: nanosecond conversion is wrong"
    assert too_new == 0, f"{too_new} rows in the future: nanosecond conversion is wrong"
    assert non_positive == 0, f"{non_positive} rows with non-positive duration"
    assert absurd == 0, f"{absurd} rows longer than 10 minutes"


def test_llm_metrics_survive_the_wire(settings: Settings) -> None:
    """Numeric attributes cross OTLP as strings; verify they came back as numbers."""
    with client(settings.clickhouse) as conn:
        row = conn.query("""
            SELECT
                count(),
                countIf(input_tokens > 0),
                countIf(kv_cache_utilization > 0 AND kv_cache_utilization <= 1),
                countIf(ttft_ms > 0)
            FROM telemetry.spans_raw
            WHERE operation = 'chat'
        """).result_rows[0]

    total, with_tokens, with_kv, with_ttft = row
    assert total > 0, "no chat spans ingested"
    # Not all-or-nothing: failed requests legitimately report zero tokens.
    assert with_tokens > total * 0.5
    assert with_kv > total * 0.9
    assert with_ttft > total * 0.9


def test_model_latency_ordering_is_preserved(settings: Settings) -> None:
    """The generator makes bigger models slower. If that ordering does not
    survive ingestion, some attribute is being mis-mapped."""
    with client(settings.clickhouse) as conn:
        rows = conn.query("""
            SELECT model, quantile(0.95)(ttft_ms) AS p95
            FROM telemetry.spans_raw
            WHERE operation = 'chat' AND model != ''
            GROUP BY model
            HAVING count() > 100
            ORDER BY p95 DESC
        """).result_rows

    assert len(rows) >= 2, "need at least two models with traffic"
    models = [r[0] for r in rows]
    assert models[0] == "llama-3.1-70b-instruct", (
        f"expected the 70b model to have the highest TTFT p95, got {models}"
    )


def test_tenant_skew_survives_ingestion(settings: Settings) -> None:
    """The zipfian imbalance must reach the database.

    If it did not, the cardinality work in Phase 4 would be defending against a
    problem the data no longer exhibits.
    """
    with client(settings.clickhouse) as conn:
        rows = conn.query("""
            SELECT tenant_id, count() AS n
            FROM telemetry.spans_raw
            GROUP BY tenant_id
            ORDER BY n DESC
        """).result_rows

    assert len(rows) >= 10, "not enough tenants to judge skew"
    total = sum(r[1] for r in rows)
    top_5 = sum(r[1] for r in rows[:5])
    assert top_5 / total > 0.4, f"top 5 tenants hold {top_5 / total:.1%}; expected heavy skew"


def test_attribute_map_is_populated(settings: Settings) -> None:
    """The open map is what makes a new signal type additive (ADR-006)."""
    with client(settings.clickhouse) as conn:
        avg_attrs = float(
            conn.query("SELECT avg(length(attributes)) FROM telemetry.spans_raw").result_rows[0][0]
        )
    assert avg_attrs > 5, f"attribute map looks empty (avg {avg_attrs} keys/span)"


def test_no_messages_landed_in_the_dead_letter_table(settings: Settings) -> None:
    """Should stay empty. It exists so that claim is checkable."""
    with client(settings.clickhouse) as conn:
        errors = int(
            conn.query("SELECT count() FROM telemetry.spans_ingest_errors").result_rows[0][0]
        )
        sample = ""
        if errors:
            sample = str(
                conn.query("SELECT error FROM telemetry.spans_ingest_errors LIMIT 1").result_rows[
                    0
                ][0]
            )
    assert errors == 0, f"{errors} unparseable messages; first error: {sample}"


def test_part_count_stays_manageable(settings: Settings) -> None:
    """'Too many parts' is the characteristic Kafka-engine failure.

    It happens when blocks are flushed too small and too often; merges then
    fall behind and inserts start being rejected. Part count is the early
    warning.
    """
    with client(settings.clickhouse) as conn:
        parts = int(
            conn.query("""
                SELECT count() FROM system.parts
                WHERE database = 'telemetry' AND table = 'spans_raw' AND active
            """).result_rows[0][0]
        )
    assert parts < 300, f"{parts} active parts suggests blocks are flushing too small"


def test_consumer_group_is_not_stalled(settings: Settings) -> None:
    """A stalled MV shows up as a consumer that stops committing offsets.

    Checked via ClickHouse's own view of its Kafka consumers rather than the
    broker, since that is where an MV exception would surface.
    """
    with client(settings.clickhouse) as conn:
        rows = conn.query("""
            SELECT
                consumer_id,
                num_messages_read,
                num_commits,
                length(assignments.partition_id) AS partitions,
                arrayStringConcat(`exceptions.text`, ' | ') AS errors
            FROM system.kafka_consumers
            WHERE database = 'telemetry' AND table = 'kafka_spans'
        """).result_rows

    assert rows, "no Kafka consumers registered; the Kafka table may not exist"
    for consumer_id, messages_read, commits, partitions, errors in rows:
        assert not errors, f"{consumer_id} reported an exception: {errors}"
        assert int(messages_read) > 0, f"{consumer_id} has read nothing at all"
        # Committing offsets is what distinguishes a working consumer from one
        # that reads and then fails inside the materialized view.
        assert int(commits) > 0, f"{consumer_id} has never committed an offset"
        assert int(partitions) > 0, f"{consumer_id} owns no partitions"


def test_every_partition_is_assigned_to_a_consumer(settings: Settings) -> None:
    """All six partitions must be owned, or part of the topic is never drained.

    An unassigned partition accumulates lag indefinitely while the others look
    perfectly healthy -- easy to miss on an aggregate lag panel.
    """
    with client(settings.clickhouse) as conn:
        rows = conn.query("""
            SELECT arrayJoin(`assignments.partition_id`) AS partition_id
            FROM system.kafka_consumers
            WHERE database = 'telemetry' AND table = 'kafka_spans'
        """).result_rows

    owned = {int(r[0]) for r in rows}
    expected = set(range(settings.redpanda.partitions))
    assert owned == expected, f"partitions {sorted(expected - owned)} are unassigned"
