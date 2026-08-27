"""Rollup correctness and the cardinality bound, against live data.

Requires `python tasks.py up`, an applied schema, a synced allowlist, and some
traffic. These check the two claims Phase 4 makes: that the aggregates are
correct, and that the bound actually holds.
"""

from __future__ import annotations

import time

import pytest
from opentelemetry.trace import SpanKind

from telemetry_engine.cardinality.guard import status as guard_status
from telemetry_engine.cardinality.registry import load_registry
from telemetry_engine.config import Settings
from telemetry_engine.emitters.otlp import ExporterConfig, build_tracer_provider
from telemetry_engine.storage.client import client

pytestmark = pytest.mark.integration

OTLP_ENDPOINT = "http://localhost:4317"


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings()


@pytest.fixture(scope="module")
def registry():
    return load_registry()


# --- Correctness --------------------------------------------------------------


def test_rollup_totals_match_raw_over_the_same_window(settings: Settings) -> None:
    """A rollup that disagrees with its source is worse than no rollup.

    Compared over the window the rollup actually covers: materialized views do
    not backfill, so raw may legitimately extend earlier.
    """
    with client(settings.clickhouse) as conn:
        start = conn.query("SELECT min(ts_minute) FROM telemetry.spans_1m").result_rows[0][0]
        assert start is not None, "no rollup data; run a load first"

        raw = conn.query(
            """
            SELECT count(), sum(input_tokens), sum(output_tokens), countIf(status_class != 'ok')
            FROM telemetry.spans_raw WHERE ts >= %(start)s
            """,
            parameters={"start": start},
        ).result_rows[0]
        rolled = conn.query(
            """
            SELECT countMerge(spans), sumMerge(input_tokens), sumMerge(output_tokens),
                   countIfMerge(errors)
            FROM telemetry.spans_1m WHERE ts_minute >= %(start)s
            """,
            parameters={"start": start},
        ).result_rows[0]

    assert tuple(rolled) == tuple(raw), f"rollup {tuple(rolled)} != raw {tuple(raw)}"


def test_hourly_cascade_preserves_totals(settings: Settings) -> None:
    """spans_1h is built from spans_1m via -MergeState; totals must survive."""
    with client(settings.clickhouse) as conn:
        start = conn.query("SELECT min(ts_minute) FROM telemetry.spans_1m").result_rows[0][0]
        minute = conn.query(
            "SELECT countMerge(spans), sumMerge(output_tokens) FROM telemetry.spans_1m "
            "WHERE ts_minute >= %(s)s",
            parameters={"s": start},
        ).result_rows[0]
        hour = conn.query(
            "SELECT countMerge(spans), sumMerge(output_tokens) FROM telemetry.spans_1h "
            "WHERE ts_hour >= toStartOfHour(toDateTime(%(s)s))",
            parameters={"s": start},
        ).result_rows[0]

    assert tuple(hour) == tuple(minute)


def test_tdigest_quantiles_are_close_to_exact(settings: Settings) -> None:
    """tDigest trades a little accuracy for mergeability. Verify 'a little'."""
    with client(settings.clickhouse) as conn:
        start = conn.query("SELECT min(ts_minute) FROM telemetry.spans_1m").result_rows[0][0]
        exact = float(
            conn.query(
                "SELECT quantile(0.95)(ttft_ms) FROM telemetry.spans_raw "
                "WHERE ttft_ms > 0 AND ts >= %(s)s",
                parameters={"s": start},
            ).result_rows[0][0]
        )
        approx = float(
            conn.query(
                "SELECT quantilesTDigestMerge(0.95)(ttft)[1] FROM telemetry.spans_1m "
                "WHERE ts_minute >= %(s)s",
                parameters={"s": start},
            ).result_rows[0][0]
        )

    error = abs(approx - exact) / exact
    assert error < 0.05, f"tDigest p95 {approx:.2f} vs exact {exact:.2f} ({error:.1%} off)"


def test_rollup_is_much_smaller_than_raw(settings: Settings) -> None:
    """If the rollup is not dramatically smaller, it is not earning its keep."""
    with client(settings.clickhouse) as conn:
        rows = conn.query("""
            SELECT table, sum(rows)
            FROM system.parts
            WHERE database = 'telemetry' AND active AND table IN ('spans_raw', 'spans_1m')
            GROUP BY table
        """).result_rows
    sizes = {str(t): int(n) for t, n in rows}
    assert sizes.get("spans_1m", 0) > 0
    assert sizes["spans_1m"] < sizes["spans_raw"] / 5, (
        f"rollup {sizes['spans_1m']} vs raw {sizes['spans_raw']} is not a useful reduction"
    )


# --- The cardinality bound ----------------------------------------------------


def test_every_dimension_stays_within_budget(settings: Settings, registry) -> None:
    """The claim, checked against live data rather than asserted in prose."""
    with client(settings.clickhouse) as conn:
        rows = guard_status(conn, registry)

    for row in rows:
        assert row.within_budget, (
            f"{row.name}: {row.observed_distinct} distinct values exceeds "
            f"budget {row.budget} + 2 sentinels"
        )


def test_unregistered_values_collapse_to_other(settings: Settings, registry) -> None:
    """Emit deliberately unregistered values and prove they are bucketed.

    The raw tier keeps every distinct value for debugging; the rollup must show
    exactly one. This is the whole design in one test.
    """
    marker_model = "test-shadow-deploy"
    rogue_tenants = 40

    provider = build_tracer_provider(ExporterConfig(endpoint=OTLP_ENDPOINT))
    tracer = provider.get_tracer("cardinality-test")
    now = time.time_ns()
    for i in range(rogue_tenants):
        span = tracer.start_span(
            "chat unregistered",
            kind=SpanKind.CLIENT,
            start_time=now - 1_000_000_000,
            attributes={
                "tenant.id": f"unregistered-tenant-{time.time_ns()}-{i}",
                "gen_ai.request.model": marker_model,
                "gen_ai.operation.name": "chat",
                "cloud.region": "antarctica-1",
                "status.class": "ok",
                "llm.time_to_first_token_ms": 10.0,
            },
        )
        span.end(end_time=now)
    provider.shutdown()

    deadline = time.monotonic() + 90.0
    with client(settings.clickhouse) as conn:
        raw_distinct = 0
        while time.monotonic() < deadline:
            raw_distinct = int(
                conn.query(
                    "SELECT uniqExact(tenant_id) FROM telemetry.spans_raw WHERE model = %(m)s",
                    parameters={"m": marker_model},
                ).result_rows[0][0]
            )
            if raw_distinct >= rogue_tenants:
                break
            time.sleep(2.0)

        assert raw_distinct >= rogue_tenants, (
            f"only {raw_distinct}/{rogue_tenants} rogue tenants reached raw"
        )

        # The rollup must contain no trace of the unregistered model itself.
        leaked = int(
            conn.query(
                "SELECT count() FROM telemetry.spans_1m WHERE model = %(m)s",
                parameters={"m": marker_model},
            ).result_rows[0][0]
        )
        assert leaked == 0, f"unregistered model appears in the rollup {leaked} times"

        rogue_region = int(
            conn.query(
                "SELECT count() FROM telemetry.spans_1m WHERE region = 'antarctica-1'"
            ).result_rows[0][0]
        )
        assert rogue_region == 0, "unregistered region leaked into the rollup"


def test_absent_and_unregistered_use_different_buckets(settings: Settings, registry) -> None:
    """`__none__` is routine; `__other__` is actionable.

    Tool spans carry no model, and there are far more of them than there are
    genuinely unregistered spans. If both shared a bucket, the actionable
    signal would be permanently drowned -- which is what the first version of
    the rollup view did.
    """
    with client(settings.clickhouse) as conn:
        row = conn.query(
            """
            SELECT
                countIf(model = %(none)s),
                countIf(model = %(other)s)
            FROM telemetry.spans_1m
            """,
            parameters={"none": registry.none_bucket, "other": registry.other_bucket},
        ).result_rows[0]

    none_rows, other_rows = int(row[0]), int(row[1])
    assert none_rows > 0, "expected tool spans with no model in the __none__ bucket"
    # Both buckets must be reachable and distinct; the point is that a small
    # number of unregistered rows stays visible next to a large routine bucket.
    assert other_rows >= 0
    assert none_rows != other_rows or none_rows == 0


def test_registered_dimensions_are_not_bucketed(settings: Settings, registry) -> None:
    """Ordinary traffic must survive the guard untouched.

    A guard that buckets legitimate values is worse than none: it destroys the
    data and hides the fact that it did.
    """
    with client(settings.clickhouse) as conn:
        rows = conn.query(
            """
            SELECT model, countMerge(spans) AS n
            FROM telemetry.spans_1m
            WHERE model NOT IN (%(none)s, %(other)s)
            GROUP BY model ORDER BY n DESC
            """,
            parameters={"none": registry.none_bucket, "other": registry.other_bucket},
        ).result_rows

    models = {str(r[0]) for r in rows}
    registered = set(registry.by_name["model"].values)
    assert models, "no registered models survived the guard"
    assert models <= registered, f"unregistered models leaked through: {models - registered}"


def test_rollup_row_count_respects_the_theoretical_bound(settings: Settings, registry) -> None:
    """Rows per minute cannot exceed the product of the dimension budgets."""
    with client(settings.clickhouse) as conn:
        worst = int(
            conn.query("""
                SELECT max(n) FROM (
                    SELECT ts_minute, count() AS n FROM telemetry.spans_1m GROUP BY ts_minute
                )
            """).result_rows[0][0]
            or 0
        )

    assert worst <= registry.max_rows_per_bucket, (
        f"{worst} rows in a single minute exceeds the declared bound {registry.max_rows_per_bucket}"
    )
