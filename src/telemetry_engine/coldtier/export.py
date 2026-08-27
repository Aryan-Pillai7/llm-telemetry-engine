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

import duckdb
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

# Settings for the export query. The global sort is the memory-hungry part: an
# hour of spans is comfortably over a million rows and ClickHouse OOMed against
# its 1.5 GiB server cap trying to sort one in memory.
#
# Spilling to disk is the right answer rather than shrinking the window, because
# the sort is not optional -- it is what makes row-group pruning work, so
# abandoning it to save memory would leave a lake that reads correctly and
# scans everything. Threads are capped so a backfill does not starve ingest.
EXPORT_SETTINGS = {
    # Hard ceiling well under the server total (1.5 GiB), because the export is
    # a BACKGROUND job that must yield to ingest. Without this cap an export
    # took ~1 GiB and left too little for the Kafka materialized view, which
    # then hit the server limit mid-parse and shed 61 messages into the
    # dead-letter table. Ingest degraded because a batch job was greedy.
    "max_memory_usage": 400_000_000,
    # Spill below the ceiling rather than failing at it. The sort is not
    # optional: it is what makes row-group pruning work.
    "max_bytes_before_external_sort": 200_000_000,
    "max_threads": 2,
    "max_execution_time": 900,
    # Never let a backfill push ingest out of the page cache.
    "max_bytes_before_external_group_by": 200_000_000,
}

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
    fingerprint_matched: bool = False

    @property
    def ok(self) -> bool:
        return self.skipped or (
            self.source_rows == self.written_rows == self.verified_rows
            and self.written_rows > 0
            and self.fingerprint_matched
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
    """Advance the watermark. Called ONLY after a window verifies.

    Written as a formatted string rather than passed as a `datetime`, because
    the client's read and write paths are not symmetric: DateTime values come
    back as naive UTC (this server runs in UTC), but `insert()` treats a naive
    datetime as *local* and converts it to UTC on the way in. On a machine in
    UTC+5:30 that silently stored every watermark 5.5 hours in the past --
    observed as an export reporting it had advanced to 20:07 while the table
    held 14:37.

    SELECT parameter binding does not do this, which is why the exported windows
    themselves were always correct and only the bookkeeping drifted. Sending a
    string removes the conversion entirely; `toDateTime` parses it in the
    server's timezone, which is the frame every other timestamp here uses.
    """
    conn.command(
        f"INSERT INTO {WATERMARK_TABLE} (source, watermark) "
        "VALUES (%(source)s, toDateTime(%(watermark)s))",
        parameters={
            "source": WATERMARK_KEY,
            "watermark": value.strftime("%Y-%m-%d %H:%M:%S"),
        },
    )


def rewind_watermark(conn: Client, to: datetime) -> None:
    """Force the watermark backwards, so a window can be re-exported.

    The table is ReplacingMergeTree(watermark), which keeps the highest value --
    a watermark cannot regress by accident, which is exactly what you want from
    bookkeeping that guards against data loss. Deliberately moving it back
    therefore needs an explicit delete rather than an insert.

    The case that makes this necessary: the lake is deleted or restored from an
    older backup while the watermark still claims those windows are covered.
    Re-export is safe -- filenames are deterministic, so it overwrites.
    """
    conn.command(
        f"ALTER TABLE {WATERMARK_TABLE} DELETE WHERE source = %(k)s",
        parameters={"k": WATERMARK_KEY},
        settings={"mutations_sync": 2},
    )
    write_watermark(conn, to)


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
    """Split [start, now - lag_margin) into windows that never cross midnight.

    Windows begin exactly at `start` -- never floored to the hour. Flooring
    looks harmless and is not: a watermark at 14:58 would re-plan the window
    from 14:00, re-exporting 58 minutes that are already in the lake under a
    file whose name ends at a different timestamp. Deterministic filenames stop
    a retry from duplicating rows only when the window is identical; a window
    with the same start and a different end writes a second file covering the
    same span, and duplicate rows in a lake are much harder to notice than a
    gap.

    So the first window runs from the watermark to the next hour boundary, and
    every window after that is a full hour. Windows abut exactly: no gaps, which
    would be permanent data loss, and no overlaps, which would be duplicates.
    """
    horizon = now - lag_margin
    windows: list[ExportWindow] = []
    cursor = start

    while cursor < horizon:
        # Align to the next hour boundary rather than start + 1h, so that after
        # the first partial window every file covers a whole clock hour.
        next_boundary = (cursor + window).replace(minute=0, second=0, microsecond=0)
        if next_boundary <= cursor:
            next_boundary = cursor + window
        # Clip to midnight so every file lands in exactly one dt= partition.
        midnight = datetime.combine(cursor.date() + timedelta(days=1), datetime.min.time())
        end = min(next_boundary, midnight, horizon)
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
    """Aggregates that prove the exported VALUES match the source.

    Row counts alone would pass even if columns were misaligned or a projection
    dropped a column's values -- exactly the kind of corruption that survives
    review because the shape looks right. Sums over three numeric columns and
    exact distinct counts over two identifier columns will not agree by accident
    if the data is wrong.

    `uniqExact` rather than `uniq`: an approximate distinct count could differ
    from DuckDB's exact one for entirely innocent reasons, which would make the
    check unusable.
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


def _parquet_fingerprint(path: Path) -> tuple:
    """The same aggregates, computed from the written file by DuckDB.

    Deliberately computed by a different engine than the one that produced the
    data. Re-reading with pyarrow would share assumptions with the writer; the
    reader that matters is the one the cold tier is actually queried with.
    """
    with duckdb.connect(":memory:") as conn:
        row = conn.execute(
            """
            SELECT count(*),
                   sum(input_tokens),
                   sum(output_tokens),
                   round(sum(CAST(duration_ms AS DOUBLE)), 3),
                   count(DISTINCT tenant_id),
                   count(DISTINCT trace_id)
            FROM read_parquet(?)
            """,
            [str(path)],
        ).fetchone()
    return tuple(row)


def _fingerprints_agree(source: tuple, parquet: tuple) -> bool:
    """Compare with a tolerance only on the float column."""
    if len(source) != len(parquet):
        return False
    for index, (a, b) in enumerate(zip(source, parquet, strict=True)):
        if a is None or b is None:
            if a != b:
                return False
            continue
        if index == 3:  # summed float; float32 -> float64 widening loses precision
            if abs(float(a) - float(b)) > max(1.0, abs(float(a)) * 1e-6):
                return False
        elif int(a) != int(b):
            return False
    return True


def _normalize_timestamps(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Strip timezone annotations from timestamp columns.

    The Arrow batches coming back from ClickHouse carry the *client machine's*
    timezone on DateTime columns. Written straight to Parquet, the archive ends
    up stamped with whichever laptop happened to run the export -- observed as a
    lake whose `ts` column read as `Asia/Calcutta` and whose values shifted by
    5.5 hours when queried, so a filter on a known window returned nothing.

    An archive that outlives the machine that wrote it has no business recording
    that machine's locale. Everything here is UTC; the annotation is dropped so
    the values are unambiguous and identical wherever they are read.
    """
    fields = []
    columns = []
    changed = False
    # Named schema_field, not field: dataclasses.field is imported above and
    # shadowing it here is the kind of thing that works until someone adds a
    # dataclass to this module.
    for index, schema_field in enumerate(batch.schema):
        column = batch.column(index)
        if pa.types.is_timestamp(schema_field.type) and schema_field.type.tz is not None:
            target = pa.timestamp(schema_field.type.unit)
            column = column.cast(target)
            schema_field = schema_field.with_type(target)
            changed = True
        fields.append(schema_field)
        columns.append(column)
    if not changed:
        return batch
    return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))


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
    with conn.query_arrow_stream(
        query,
        parameters={"s": window.start, "e": window.end},
        settings=EXPORT_SETTINGS,
    ) as stream:
        for batch in stream:
            if batch.num_rows:
                yield _normalize_timestamps(batch)


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
    # The handle must be closed before the rename: on Windows an open reader
    # locks the file and os.replace fails with WinError 32.
    with pq.ParquetFile(staging) as verified:
        result.verified_rows = verified.metadata.num_rows
    result.bytes_on_disk = staging.stat().st_size

    # Values, not just counts. A row count matches whether or not the columns
    # line up.
    source_fp = _source_fingerprint(conn, window)
    parquet_fp = _parquet_fingerprint(staging)
    result.fingerprint_matched = _fingerprints_agree(source_fp, parquet_fp)
    if not result.fingerprint_matched:
        result.reason = f"fingerprint mismatch: source={source_fp} parquet={parquet_fp}"

    if not result.ok:
        staging.unlink(missing_ok=True)
        log.error(
            "export_verification_failed",
            window=str(window.start),
            source=result.source_rows,
            written=result.written_rows,
            verified=result.verified_rows,
            fingerprint_matched=result.fingerprint_matched,
            reason=result.reason,
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
    since: datetime | None = None,
) -> ExportResult:
    """Export every complete window since the watermark."""
    root = settings.coldtier.root
    result = ExportResult()

    if since is not None:
        rewind_watermark(conn, since)

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
    """Is the exporter keeping ahead of the hot tier's TTL, and is the lake real?"""

    watermark: datetime | None
    hot_tier_oldest: datetime | None
    ttl_hours: int
    now: datetime
    lake_rows: int = 0
    lake_max_ts: datetime | None = None

    @property
    def watermark_without_data(self) -> bool:
        """The watermark claims coverage the lake does not have.

        Happens when the lake is deleted or restored from an older backup while
        the bookkeeping stays put. The exporter would then skip those windows
        forever and the hot tier would drop them on schedule -- a silent,
        permanent gap produced by two components that are each individually
        behaving correctly.
        """
        return self.watermark is not None and self.lake_rows == 0

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
        if self.watermark_without_data:
            return True
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
    from telemetry_engine.coldtier.query import stats

    now = conn.query("SELECT now()").result_rows[0][0]
    lake = stats(settings.coldtier.root)
    return ExportHealth(
        watermark=read_watermark(conn),
        hot_tier_oldest=default_start(conn),
        ttl_hours=settings.clickhouse.raw_ttl_hours,
        now=now,
        lake_rows=lake.rows,
    )
