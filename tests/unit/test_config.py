"""Configuration behaves as the deployment story assumes."""

from __future__ import annotations

import pytest

from telemetry_engine.config import Settings, get_settings


def test_defaults_point_at_the_local_compose_stack() -> None:
    """A fresh clone with no .env must work against `make up`."""
    s = Settings()
    assert s.redpanda.bootstrap_servers == "localhost:19092"
    assert s.clickhouse.host == "localhost"
    assert s.clickhouse.database == "telemetry"


def test_env_overrides_are_nested(monkeypatch: pytest.MonkeyPatch) -> None:
    """Nested overrides use the TE_SECTION__FIELD form."""
    monkeypatch.setenv("TE_CLICKHOUSE__HOST", "clickhouse")
    monkeypatch.setenv("TE_REDPANDA__PARTITIONS", "12")
    s = get_settings()
    assert s.clickhouse.host == "clickhouse"
    assert s.redpanda.partitions == 12


def test_scale_target_matches_adr_009() -> None:
    """Burst must exceed sustained, or the backpressure experiment is vacuous."""
    s = Settings()
    assert s.workload.burst_spans_per_sec > s.workload.sustained_spans_per_sec


def test_cold_tier_partitions_on_date_only() -> None:
    """ADR-007: `dt` is the only partition key; tenant is a sort key, not a directory.

    Partitioning by tenant would multiply file count by tenant cardinality --
    exactly the small-file problem the cold tier is designed to avoid.
    """
    s = Settings()
    assert s.coldtier.partition_keys == ("dt",)
    assert "tenant_id" in s.coldtier.sort_keys
    assert "tenant_id" not in s.coldtier.partition_keys


def test_raw_ttl_shorter_than_nothing_and_positive() -> None:
    """Raw spans are high-cardinality and must age out of the hot tier."""
    s = Settings()
    assert 0 < s.clickhouse.raw_ttl_hours <= 24 * 7
