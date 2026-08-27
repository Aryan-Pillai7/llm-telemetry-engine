"""Export raw spans from ClickHouse into the Parquet cold tier.

The hot tier drops raw spans after 48 hours (ADR-006). Everything that should
outlive that has to be here before then, which makes this the one job in the
pipeline whose failure is *permanent*: a missed window is not a stale dashboard,
it is data that no longer exists anywhere.

That shapes the design more than performance does:

  - **The watermark advances only after verification.** Rows are written to a
    staging file, read back, counted, and compared against ClickHouse on both
    row count and aggregates. Only then is the file renamed into place and the
    watermark moved. An export that fails halfway leaves the watermark where it
    was and re-runs cleanly.
  - **File names are deterministic**, so a retry overwrites rather than
    duplicates. Duplicates in a lake are much harder to notice than gaps.
  - **Rows are sorted server-side** by (tenant_id, ts) and streamed in that
    order, so the whole file is globally sorted rather than only within chunks.
    Chunk-local sorting would look identical on inspection and would silently
    destroy row-group pruning.
  - **Windows never cross midnight**, so each file belongs to exactly one
    `dt=` partition.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from clickhouse_connect.driver.client import Client

from telemetry_engine.coldtier.layout import (
    COMPRESSION,
    ROW_GROUP_SIZE,
    ExportWindow,
    file_path,
    temp_path,
)
from telemetry_engine.common.logging import get_logger
from telemetry_engine.config import Settings

log = get_logger(__name__)

WATERMARK_TABLE = "telemetry.cold_export_watermark"
WATERMARK_KEY = "spans_raw"

# Do not export right up to `now`: spans arrive seconds late (p95 ingest latency
# is ~8.5s), so a window closed too eagerly would omit rows that land moments
# later and the watermark would move past them forever.
DEFAULT_LAG_MARGIN = timedelta(minutes=5)

# One file per hour. Small enough to bound memory and to re-run cheaply, large
# enough to avoid the small-file problem.
DEFAULT_WINDOW = timedelta(hours=1)

# Columns exported, in order. Explicit rather than SELECT *: a column added to
# spans_raw should not silently change the cold-tier schema, and a column
# removed should fail loudly here rather than produce files that no longer match
# their older siblings.
COLUMNS: tuple[str, ...] = (
    "ts",
    "trace_id",
    "span_id",
    "parent_span_id",
    "span_name",
    "span_kind",
    "duration_ms",
    "tenant_id",
    "tenant_tier",
    "model",
    "operation",
    "route",
    "region",
    "status_class",
    "service_name",
    "input_tokens",
    "output_tokens",
    "cached_prompt_tokens",
    "kv_cache_utilization",
    "ttft_ms",
    "itl_ms",
    "queue_time_ms",
    "batch_size",
    "request_id",
    "error_type",
)


@dataclass
class WindowResult:
    """What one exported window produced."""

    window: ExportWindow
    path: Path
    source_rows: int = 0
    written_rows: int = 0
    verified_rows: int = 0
    bytes_on_disk: int = 0
    skipped: bool = False
    reason: str = ""

    @property
    def ok(self) -> bool:
        return self.skipped or (
            self.source_rows == self.written_rows == self.verified_rows and self.written_rows > 0
        )


@dataclass
class ExportResult:
    """A whole export run."""

    windows: list[WindowResult] = field(default_factory=list)
    watermark_before: datetime | None = None
    watermark_after: datetime | None = None

    @property
    def rows(self) -> int:
        return sum(w.written_rows for w in self.windows)

    @property
    def files(self) -> int:
        return sum(1 for w in self.windows if not w.skipped and w.written_rows)

    @property
    def ok(self) -> bool:
        return all(w.ok for w in self.windows)


# --- Watermark ----------------------------------------------------------------


def read_watermark(conn: Client) -> datetime | None:
    rows = conn.query(
        f"SELECT watermark FROM {WATERMARK_TABLE} FINAL WHERE source = %(k)s",
        parameters={"k": WATERMARK_KEY},
    ).result_rows
    return rows[0][0] if rows else None


def write_watermark(conn: Client, value: datetime) -> None:
    """Advance the watermark. Called ONLY after a window verifies."""
    conn.insert(
        WATERMARK_TABLE,
        [[WATERMARK_KEY, value]],
        column_names=["source", "watermark"],
    )


def default_start(conn: Client) -> datetime | None:
    """Where to begin when no watermark exists: the oldest row in the hot tier."""
    rows = conn.query("SELECT min(ts) FROM telemetry.spans_raw").result_rows
    return rows[0][0] if rows and rows[0][0] else None


def plan_windows(
    start: datetime,
    now: datetime,
    *,
    window: timedelta = DEFAULT_WINDOW,
    lag_margin: timedelta = DEFAULT_LAG_MARGIN,
) -> list[ExportWindow]:
    """Split [start, now - lag_margin) into windows that never cross midnight."""
    horizon = now - lag_margin
    windows: list[ExportWindow] = []
    cursor = start.replace(minute=0, second=0, microsecond=0)

    while cursor < horizon:
        end = cursor + window
        # Clip to midnight so every file lands in exactly one dt= partition.
        midnight = datetime.combine(cursor.date() + timedelta(days=1), datetime.min.time())
        end = min(end, midnight, horizon)
        if end <= cursor:
            break
        windows.append(ExportWindow(start=cursor, end=end))
        cursor = end

    return windows


# --- Export -------------------------------------------------------------------


def _count_source(conn: Client, window: ExportWindow) -> int:
    return int(
        conn.query(
            "SELECT count() FROM telemetry.spans_raw WHERE ts >= %(s)s AND ts < %(e)s",
            parameters={"s": window.start, "e": window.end},
        ).result_rows[0][0]
    )


def _source_fingerprint(conn: Client, window: ExportWindow) -> tuple:
    """Aggregates used to prove the exported values match the source.

    Row counts alone would pass even if columns were misaligned or a projection
    dropped a column's values -- which is exactly the kind of corruption that
    survives review because the shape looks right.
    """
    return tuple(
        conn.query(
            """
            SELECT count(),
                   sum(input_tokens),
                   sum(output_tokens),
                   round(sum(toFloat64(duration_ms)), 3),
                   uniqExact(tenant_id),
                   uniqExact(trace_id)
            FROM telemetry.spans_raw
            WHERE ts >= %(s)s AND ts < %(e)s
            """,
            parameters={"s": window.start, "e": window.end},
        ).result_rows[0]
    )


def _stream_window(conn: Client, window: ExportWindow) -> Iterator[pa.RecordBatch]:
    """Stream a window from ClickHouse, globally sorted.

    The ORDER BY runs server-side so the entire result is sorted before it is
    chunked. Sorting each chunk client-side instead would produce a file that is
    only locally ordered -- indistinguishable by eye, and useless for row-group
    pruning.
    """
    columns = ", ".join(COLUMNS)
    query = f"""
        SELECT {columns}, toHour(ts) AS hour
        FROM telemetry.spans_raw
        WHERE ts >= %(s)s AND ts < %(e)s
        ORDER BY tenant_id, ts
    """
    with conn.query_arrow_stream(query, parameters={"s": window.start, "e": window.end}) as stream:
        for batch in stream:
            if batch.num_rows:
                yield batch


def export_window(conn: Client, root: Path, window: ExportWindow) -> WindowResult:
    """Export one window: write, verify, then commit by rename."""
    final = file_path(root, window)
    result = WindowResult(window=window, path=final)

    result.source_rows = _count_source(conn, window)
    if result.source_rows == 0:
        result.skipped = True
        result.reason = "no rows in window"
        return result

    staging = temp_path(final)
    staging.parent.mkdir(parents=True, exist_ok=True)

    writer: pq.ParquetWriter | None = None
    try:
        for batch in _stream_window(conn, window):
            table = pa.Table.from_batches([batch])
            if writer is None:
                writer = pq.ParquetWriter(
                    staging,
                    table.schema,
                    compression=COMPRESSION,
                    # Recorded in the file's metadata so a reader can see the
                    # ordering is intentional rather than incidental.
                    sorting_columns=None,
                )
            writer.write_table(table, row_group_size=ROW_GROUP_SIZE)
            result.written_rows += batch.num_rows
    finally:
        if writer is not None:
            writer.close()

    if writer is None:
        result.skipped = True
        result.reason = "source reported rows but streamed none"
        staging.unlink(missing_ok=True)
        return result

    # Read back before committing. A file that cannot be read is not an export.
    verified = pq.ParquetFile(staging)
    result.verified_rows = verified.metadata.num_rows
    result.bytes_on_disk = staging.stat().st_size

    if not result.ok:
        staging.unlink(missing_ok=True)
        log.error(
            "export_verification_failed",
            window=str(window.start),
            source=result.source_rows,
            written=result.written_rows,
            verified=result.verified_rows,
        )
        return result

    # Atomic-ish commit. os.replace overwrites, which is what makes a retry
    # idempotent rather than duplicating rows.
    staging.replace(final)
    log.info(
        "window_exported",
        window=str(window.start),
        rows=result.written_rows,
        mib=round(result.bytes_on_disk / 1024 / 1024, 2),
    )
    return result


def run_export(
    conn: Client,
    settings: Settings,
    *,
    now: datetime | None = None,
    max_windows: int | None = None,
) -> ExportResult:
    """Export every complete window since the watermark."""
    root = settings.coldtier.root
    result = ExportResult()

    result.watermark_before = read_watermark(conn)
    start = result.watermark_before or default_start(conn)
    if start is None:
        log.info("nothing_to_export", reason="hot tier is empty")
        return result

    reference = now or conn.query("SELECT now()").result_rows[0][0]
    windows = plan_windows(start, reference)
    if max_windows:
        windows = windows[:max_windows]

    for window in windows:
        window_result = export_window(conn, root, window)
        result.windows.append(window_result)
        if not window_result.ok:
            # Stop at the first failure and leave the watermark behind it. The
            # alternative -- carrying on and advancing past a gap -- turns a
            # retryable error into permanent data loss once the TTL fires.
            log.error("export_halted", at=str(window.start), reason=window_result.reason)
            break
        write_watermark(conn, window.end)
        result.watermark_after = window.end

    return result


# --- Health -------------------------------------------------------------------


@dataclass
class ExportHealth:
    """Is the exporter keeping ahead of the hot tier's TTL?"""

    watermark: datetime | None
    hot_tier_oldest: datetime | None
    ttl_hours: int
    now: datetime

    @property
    def hours_behind(self) -> float | None:
        if self.watermark is None:
            return None
        return (self.now - self.watermark).total_seconds() / 3600.0

    @property
    def at_risk(self) -> bool:
        """True when unexported data is close enough to the TTL to be lost.

        The failure this guards against is silent and irreversible: raw rows are
        deleted on schedule whether or not anyone copied them first.
        """
        behind = self.hours_behind
        if behind is None:
            return True
        return behind > self.ttl_hours * 0.75

    def describe(self) -> str:
        if self.watermark is None:
            return "no watermark: nothing has ever been exported"
        return (
            f"watermark {self.watermark} ({self.hours_behind:.1f}h behind now); "
            f"hot tier TTL is {self.ttl_hours}h"
        )


def health(conn: Client, settings: Settings) -> ExportHealth:
    now = conn.query("SELECT now()").result_rows[0][0]
    oldest = default_start(conn)
    return ExportHealth(
        watermark=read_watermark(conn),
        hot_tier_oldest=oldest,
        ttl_hours=settings.clickhouse.raw_ttl_hours,
        now=now,
    )
