"""Cold tier against a live stack: export, verify, query.

Requires `python tasks.py up` and data in spans_raw.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from telemetry_engine.coldtier.export import (
    export_window,
    plan_windows,
    read_watermark,
)
from telemetry_engine.coldtier.layout import parquet_files
from telemetry_engine.coldtier.query import (
    ColdTierMismatchError,
    duplication,
    open_lake,
    stats,
    verify_sorted,
)
from telemetry_engine.config import Settings
from telemetry_engine.storage.client import client

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings()


def test_lake_exists_and_is_readable(settings: Settings) -> None:
    lake = stats(settings.coldtier.root)
    assert lake.files > 0, "run `telemetry-engine cold export` first"
    assert lake.rows > 0


def test_duckdb_sees_every_file_on_disk(settings: Settings) -> None:
    """A partial read returns a smaller answer with no error at all.

    open_lake raises rather than letting that through; this asserts the check
    itself is live.
    """
    on_disk = len(parquet_files(settings.coldtier.root))
    with open_lake(settings.coldtier.root) as conn:
        seen = conn.execute(
            "SELECT count(DISTINCT filename) FROM (SELECT filename FROM read_parquet(?, filename = true))",
            [str(settings.coldtier.root / "dt=*" / "*.parquet").replace("\\", "/")],
        ).fetchone()[0]
    assert seen == on_disk


def test_rows_are_sorted_for_pruning(settings: Settings) -> None:
    """Without the sort the layout still works and prunes nothing."""
    assert verify_sorted(settings.coldtier.root) == []


def test_timestamps_carry_no_client_timezone(settings: Settings) -> None:
    """An archive must not be stamped with whichever laptop exported it.

    Timezone-annotated timestamps shifted every value by the exporter's UTC
    offset, so a filter on a known window returned nothing.
    """
    with open_lake(settings.coldtier.root) as conn:
        earliest = conn.execute("SELECT min(ts) FROM spans").fetchone()[0]
    assert earliest.tzinfo is None, f"ts carries a timezone: {earliest.tzinfo}"


def test_lake_matches_clickhouse_over_the_same_window(settings: Settings) -> None:
    """The cold tier is only useful if it is faithful."""
    # Compare over whatever the lake actually covers, not a fixed hour. The
    # earlier version skipped when the lake held less than an hour, and a
    # skipped test is indistinguishable from a passing one in a summary line --
    # the exact failure mode the rest of this suite is built to avoid.
    with open_lake(settings.coldtier.root) as conn:
        bounds = conn.execute("SELECT min(ts), max(ts) FROM spans").fetchone()
        assert bounds and bounds[0], "lake is empty; run `telemetry-engine cold export`"
        start = bounds[0].replace(microsecond=0)
        end = bounds[1].replace(microsecond=0) + timedelta(seconds=1)
        lake_rows, lake_tokens = conn.execute(
            "SELECT count(*), sum(output_tokens) FROM spans WHERE ts >= ? AND ts < ?",
            [start, end],
        ).fetchone()

    with client(settings.clickhouse) as conn:
        ch_rows, ch_tokens = conn.query(
            """
            SELECT count(), sum(output_tokens) FROM telemetry.spans_raw
            WHERE ts >= %(s)s AND ts < %(e)s
            """,
            parameters={"s": start, "e": end},
        ).result_rows[0]

    assert lake_rows == ch_rows
    assert int(lake_tokens) == int(ch_tokens)


def test_reexporting_a_window_does_not_duplicate(settings: Settings, tmp_path) -> None:
    """Deterministic filenames make a retry idempotent.

    Exports the same window twice and asserts the lake gains one file, not two,
    and the same row count both times.

    Writes to a temporary root, never the real lake. An earlier version exported
    into the production lake using a window of its own choosing, which
    overlapped the window the exporter had already written and left duplicate
    rows behind -- so this test broke the fidelity test running after it. A test
    that corrupts the artifact other tests assert against is worse than no test.
    """
    root = tmp_path
    with client(settings.clickhouse) as conn:
        bounds = conn.query("SELECT min(ts), max(ts) FROM telemetry.spans_raw").result_rows[0]
        if not bounds[0]:
            pytest.skip("no data in spans_raw")
        window = plan_windows(bounds[0], bounds[1], lag_margin=timedelta(0))[0]

        first = export_window(conn, root, window)
        files_after_first = len(parquet_files(root))
        second = export_window(conn, root, window)
        files_after_second = len(parquet_files(root))

    assert first.written_rows == second.written_rows
    assert files_after_first == files_after_second, "a retry created a second file"


def test_duplication_is_measured_not_hidden(settings: Settings) -> None:
    """The pipeline is at-least-once; the size of that is a fact worth knowing."""
    report = duplication(settings.coldtier.root)
    assert report.rows >= report.distinct_spans
    # Both views must exist so a caller can choose deliberately.
    with open_lake(settings.coldtier.root) as conn:
        deduped = conn.execute("SELECT count(*) FROM spans_deduped").fetchone()[0]
    assert deduped == report.distinct_spans


def test_watermark_is_set_after_exporting(settings: Settings) -> None:
    with client(settings.clickhouse) as conn:
        assert read_watermark(conn) is not None


def test_mismatch_is_raised_not_swallowed(settings: Settings, tmp_path) -> None:
    """A lake DuckDB cannot fully read must fail loudly."""
    # An empty directory is a legitimately empty lake, not a mismatch.
    with open_lake(tmp_path) as conn:
        assert conn.execute("SELECT count(*) FROM spans").fetchone()[0] == 0

    # A file that is not valid Parquet must surface as an error, not as fewer rows.
    partition = tmp_path / "dt=2026-08-27"
    partition.mkdir()
    (partition / "spans-20260827T150000-20260827T160000.parquet").write_bytes(b"not parquet")
    with pytest.raises((ColdTierMismatchError, Exception)), open_lake(tmp_path) as conn:
        conn.execute("SELECT count(*) FROM spans").fetchone()
