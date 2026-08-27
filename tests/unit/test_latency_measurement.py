"""Guards on how pipeline latency is measured.

Phase 6 reports lag and recovery as the project's headline result. Latency was
measured wrongly twice during Phase 3 -- once by including the shutdown flush,
once by filtering on ingest time while a backlog drained. These tests make the
corrections mechanical rather than remembered.
"""

from __future__ import annotations

import pytest

from telemetry_engine.ingest.latency import (
    DRAINED_LAG_THRESHOLD,
    BacklogNotDrainedError,
    assert_drained,
    latency_sql,
)


def test_latency_subtracts_the_span_duration() -> None:
    """Spans are backdated to trace start, so ingested_at - ts includes the
    simulated LLM call itself. Without subtracting duration_ms, a 10-second
    call reports 10 seconds of pipeline latency that never happened."""
    sql = latency_sql(since_expr="ts > now() - INTERVAL 5 MINUTE")
    assert "duration_ms" in sql
    assert "- duration_ms" in sql.replace("\n", " ")


def test_filtering_on_ingest_time_is_refused() -> None:
    """An ingested_at filter selects backlog rows specifically.

    During a drain, those rows have old event times and recent ingest times, so
    they report enormous latency. This is the exact mistake that produced a
    p95 of 24s against a true 8.5s.
    """
    with pytest.raises(ValueError, match="event time"):
        latency_sql(since_expr="ingested_at > now() - INTERVAL 5 MINUTE")


def test_event_time_filters_are_accepted() -> None:
    sql = latency_sql(since_expr="ts >= toDateTime('2026-01-01 00:00:00')")
    assert "spans_raw" in sql


def test_measuring_mid_drain_raises() -> None:
    """A wrong headline number is worse than a failed measurement.

    It still gets reported.
    """
    with pytest.raises(BacklogNotDrainedError, match="backlog recovery"):
        assert_drained(50_000)


def test_small_lag_is_acceptable() -> None:
    """A healthy pipeline at 5k spans/s always has some data in flight."""
    assert_drained(0)
    assert_drained(DRAINED_LAG_THRESHOLD)


def test_threshold_is_overridable_for_recovery_measurement() -> None:
    """Phase 6 deliberately measures during recovery; it must opt in explicitly."""
    assert_drained(1_000, threshold=10_000)
    with pytest.raises(BacklogNotDrainedError):
        assert_drained(20_000, threshold=10_000)
