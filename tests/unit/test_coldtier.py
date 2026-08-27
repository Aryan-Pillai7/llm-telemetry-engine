"""Cold tier layout and export planning.

Pure logic only. The failure mode this tier has that no other tier has is that
its mistakes are *permanent*: the hot tier deletes raw spans on a 48-hour TTL,
so a window the exporter skips is data that stops existing. These tests pin the
invariants that keep a skip from happening quietly.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from itertools import pairwise
from pathlib import Path

import pytest

from telemetry_engine.coldtier.export import (
    DEFAULT_LAG_MARGIN,
    ExportHealth,
    ExportResult,
    WindowResult,
    plan_windows,
)
from telemetry_engine.coldtier.layout import (
    PARTITION_KEYS,
    ROW_GROUP_SIZE,
    SORT_KEYS,
    ExportWindow,
    file_path,
    parquet_files,
    parse_partition,
    parse_window,
    partition_dir,
    temp_path,
)

# --- Layout -------------------------------------------------------------------


def test_partitions_on_date_only() -> None:
    """ADR-007. Partitioning on tenant would multiply file count by tenant
    cardinality; sorting achieves the pruning instead."""
    assert PARTITION_KEYS == ("dt",)
    assert "tenant_id" not in PARTITION_KEYS
    assert SORT_KEYS[0] == "tenant_id"


def test_partition_dir_uses_hive_convention() -> None:
    """DuckDB's hive_partitioning only recognises `key=value` directories."""
    assert partition_dir(Path("/lake"), date(2026, 8, 27)).name == "dt=2026-08-27"


def test_partition_roundtrip() -> None:
    day = date(2026, 8, 27)
    assert parse_partition(partition_dir(Path("/lake"), day)) == day


def test_non_partition_directories_are_ignored() -> None:
    assert parse_partition(Path("/lake/not-a-partition")) is None
    assert parse_partition(Path("/lake/dt=nonsense")) is None


def test_filenames_are_deterministic() -> None:
    """A retry must overwrite, not add a second copy of the same rows.

    Duplicates in a lake are far harder to notice than gaps: every query keeps
    working and every total is quietly too large.
    """
    window = ExportWindow(datetime(2026, 8, 27, 15), datetime(2026, 8, 27, 16))
    assert window.filename == ExportWindow(window.start, window.end).filename
    assert "20260827T150000" in window.filename


def test_window_roundtrip_through_filename() -> None:
    window = ExportWindow(datetime(2026, 8, 27, 15), datetime(2026, 8, 27, 16))
    parsed = parse_window(Path(window.filename))
    assert parsed == window


def test_windows_may_not_cross_midnight() -> None:
    """A straddling window would belong to two partitions at once."""
    with pytest.raises(ValueError, match="crosses a date boundary"):
        ExportWindow(datetime(2026, 8, 27, 23), datetime(2026, 8, 28, 1))


def test_a_window_ending_exactly_at_midnight_is_allowed() -> None:
    ExportWindow(datetime(2026, 8, 27, 23), datetime(2026, 8, 28, 0, 0, 0))


def test_empty_windows_are_rejected() -> None:
    with pytest.raises(ValueError, match="empty export window"):
        ExportWindow(datetime(2026, 8, 27, 15), datetime(2026, 8, 27, 15))


def test_staging_path_is_excluded_from_the_glob(tmp_path: Path) -> None:
    """A half-written file must never be visible to a reader."""
    window = ExportWindow(datetime(2026, 8, 27, 15), datetime(2026, 8, 27, 16))
    final = file_path(tmp_path, window)
    final.parent.mkdir(parents=True)
    temp_path(final).write_bytes(b"partial")
    assert parquet_files(tmp_path) == []

    final.write_bytes(b"committed")
    assert parquet_files(tmp_path) == [final]


def test_row_group_size_is_a_pruning_decision() -> None:
    """Row groups are the granularity at which a tenant filter can skip data."""
    assert 10_000 <= ROW_GROUP_SIZE <= 1_000_000


# --- Window planning -----------------------------------------------------------


def test_planning_covers_the_range_without_gaps_or_overlaps() -> None:
    """Gaps become permanent data loss; overlaps become duplicate rows."""
    windows = plan_windows(
        datetime(2026, 8, 27, 10),
        datetime(2026, 8, 27, 15),
        lag_margin=timedelta(0),
    )
    assert windows[0].start == datetime(2026, 8, 27, 10)
    for earlier, later in pairwise(windows):
        assert earlier.end == later.start, "windows must abut exactly"


def test_planning_stops_short_of_now() -> None:
    """Spans arrive seconds late; a window closed too eagerly loses them forever.

    The watermark advances past the window either way, so a row that lands after
    its window closed is never exported and is deleted by the TTL.
    """
    now = datetime(2026, 8, 27, 15, 30)
    windows = plan_windows(datetime(2026, 8, 27, 10), now)
    assert windows[-1].end <= now - DEFAULT_LAG_MARGIN


def test_planning_splits_at_midnight() -> None:
    windows = plan_windows(
        datetime(2026, 8, 27, 22),
        datetime(2026, 8, 28, 3),
        lag_margin=timedelta(0),
    )
    assert all(w.start.date() == w.partition_date for w in windows)
    boundary = [w for w in windows if w.end.time() == datetime.min.time()]
    assert boundary, "expected a window ending exactly at midnight"


def test_planning_is_empty_when_nothing_is_complete() -> None:
    now = datetime(2026, 8, 27, 15, 0)
    assert plan_windows(datetime(2026, 8, 27, 14, 58), now) == []


# --- Result semantics ----------------------------------------------------------


def test_a_window_is_only_ok_when_every_stage_agrees() -> None:
    """Row count alone is not verification: columns can misalign at the same count."""
    window = ExportWindow(datetime(2026, 8, 27, 15), datetime(2026, 8, 27, 16))
    base = {"window": window, "path": Path("x.parquet")}

    assert WindowResult(
        **base, source_rows=10, written_rows=10, verified_rows=10, fingerprint_matched=True
    ).ok
    # Counts line up but the values do not.
    assert not WindowResult(
        **base, source_rows=10, written_rows=10, verified_rows=10, fingerprint_matched=False
    ).ok
    # Read-back disagrees with what was written.
    assert not WindowResult(
        **base, source_rows=10, written_rows=10, verified_rows=9, fingerprint_matched=True
    ).ok


def test_an_empty_window_is_ok_but_not_a_file() -> None:
    window = ExportWindow(datetime(2026, 8, 27, 15), datetime(2026, 8, 27, 16))
    result = WindowResult(window=window, path=Path("x"), skipped=True, reason="no rows")
    assert result.ok
    assert ExportResult(windows=[result]).files == 0


def test_export_result_is_not_ok_if_any_window_failed() -> None:
    window = ExportWindow(datetime(2026, 8, 27, 15), datetime(2026, 8, 27, 16))
    good = WindowResult(
        window=window,
        path=Path("a"),
        source_rows=1,
        written_rows=1,
        verified_rows=1,
        fingerprint_matched=True,
    )
    bad = WindowResult(window=window, path=Path("b"), source_rows=5, written_rows=0)
    assert not ExportResult(windows=[good, bad]).ok


# --- Health --------------------------------------------------------------------


def _health(**kwargs) -> ExportHealth:
    defaults = {
        "watermark": datetime(2026, 8, 27, 15),
        "hot_tier_oldest": datetime(2026, 8, 27, 10),
        "ttl_hours": 48,
        "now": datetime(2026, 8, 27, 16),
        "lake_rows": 1_000,
    }
    return ExportHealth(**{**defaults, **kwargs})


def test_a_current_exporter_is_healthy() -> None:
    assert not _health().at_risk


def test_falling_behind_the_ttl_is_flagged() -> None:
    """Raw spans are deleted on schedule whether or not anyone copied them."""
    assert _health(now=datetime(2026, 8, 29, 15)).at_risk


def test_never_having_exported_is_flagged() -> None:
    assert _health(watermark=None).at_risk


def test_a_watermark_without_a_lake_is_flagged() -> None:
    """The silent-permanent-gap case.

    The lake is deleted or restored from an older backup while the watermark
    stays put. Both components are individually correct; together they skip
    those windows forever.
    """
    health = _health(lake_rows=0)
    assert health.watermark_without_data
    assert health.at_risk
