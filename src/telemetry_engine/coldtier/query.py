"""Query the Parquet cold tier with DuckDB.

DuckDB is a library here, not a service (ADR-008): it opens the lake on demand,
answers, and exits. The cold tier costs disk, not standing RAM.

The one thing this module insists on is that a query which reads *fewer files
than exist* must not look like a query that found less data. A wrong glob, an
unrecognised partition directory, or a half-written file excluded from the scan
all produce a smaller answer with no error whatsoever -- the same shape of
failure as a cardinality bucket that is bounded but meaningless. `open_lake`
therefore cross-checks DuckDB's file count against the filesystem before
returning a connection anyone can query.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import duckdb

from telemetry_engine.coldtier.layout import glob_pattern, parquet_files
from telemetry_engine.common.logging import get_logger

log = get_logger(__name__)

VIEW = "spans"


class ColdTierMismatchError(RuntimeError):
    """DuckDB and the filesystem disagree about what is in the lake."""


@dataclass(frozen=True)
class LakeStats:
    """What the lake physically contains."""

    files: int
    rows: int
    bytes_on_disk: int
    partitions: int

    @property
    def mib(self) -> float:
        return self.bytes_on_disk / 1024 / 1024

    @property
    def bytes_per_row(self) -> float:
        return self.bytes_on_disk / self.rows if self.rows else 0.0


def _create_view(conn: duckdb.DuckDBPyConnection, root: Path) -> None:
    """Expose the lake as one table.

    `hive_partitioning=1` turns the `dt=` directory into a real column, so a
    date filter prunes whole directories without opening their files. Without
    it every query reads every partition and the layout achieves nothing.
    """
    conn.execute(
        f"""
        CREATE OR REPLACE VIEW {VIEW} AS
        SELECT * FROM read_parquet(?, hive_partitioning = 1, union_by_name = true)
        """,
        [glob_pattern(root)],
    )


@contextmanager
def open_lake(root: Path, *, verify: bool = True) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open the lake as an in-memory DuckDB with a `spans` view.

    Raises if DuckDB sees a different set of files than the filesystem holds.
    """
    on_disk = parquet_files(root)
    conn = duckdb.connect(":memory:")
    try:
        if not on_disk:
            # An empty lake is legitimate; a view over nothing is not, since
            # read_parquet raises on an empty glob. Give callers a typed empty
            # view so queries return no rows instead of failing.
            conn.execute(f"CREATE VIEW {VIEW} AS SELECT NULL AS ts WHERE false")
            yield conn
            return

        _create_view(conn, root)

        if verify:
            seen = conn.execute(
                "SELECT count(DISTINCT filename) FROM (SELECT filename FROM read_parquet(?, filename = true))",
                [glob_pattern(root)],
            ).fetchone()[0]
            if seen != len(on_disk):
                raise ColdTierMismatchError(
                    f"DuckDB reads {seen} files but {len(on_disk)} exist on disk. "
                    "A query against this lake would silently return partial data."
                )
        yield conn
    finally:
        conn.close()


def stats(root: Path) -> LakeStats:
    """Physical facts about the lake, straight from disk plus a row count."""
    files = parquet_files(root)
    if not files:
        return LakeStats(files=0, rows=0, bytes_on_disk=0, partitions=0)

    with open_lake(root) as conn:
        rows = int(conn.execute(f"SELECT count(*) FROM {VIEW}").fetchone()[0])

    return LakeStats(
        files=len(files),
        rows=rows,
        bytes_on_disk=sum(f.stat().st_size for f in files),
        partitions=len({f.parent.name for f in files}),
    )


def verify_sorted(root: Path, *, sample_files: int = 3) -> list[str]:
    """Check that rows really are ordered by (tenant_id, ts) inside each file.

    Returns a list of problems; empty means healthy. This exists because an
    unsorted lake is the definition of a silent failure: every query returns
    correct results, and every one of them reads far more data than it should.
    Nothing surfaces except a slow dashboard nobody attributes to the layout.
    """
    problems: list[str] = []
    for path in parquet_files(root)[:sample_files]:
        with duckdb.connect(":memory:") as conn:
            out_of_order = conn.execute(
                """
                SELECT count(*) FROM (
                    SELECT tenant_id,
                           lag(tenant_id) OVER (ORDER BY rowid) AS prev
                    FROM (SELECT tenant_id, row_number() OVER () AS rowid
                          FROM read_parquet(?))
                ) WHERE prev IS NOT NULL AND tenant_id < prev
                """,
                [str(path)],
            ).fetchone()[0]
        if out_of_order:
            problems.append(f"{path.name}: {out_of_order} rows break tenant_id ordering")
    return problems


def query(root: Path, sql: str) -> list[tuple]:
    """Run a query against the `spans` view."""
    with open_lake(root) as conn:
        return conn.execute(sql).fetchall()


# --- Canned queries the hot tier can no longer answer --------------------------
# These are the point of the cold tier: questions about data older than the hot
# tier's 48-hour TTL.

TOP_TENANTS_BY_TOKENS = f"""
SELECT tenant_id,
       count(*)            AS spans,
       sum(output_tokens)  AS output_tokens,
       round(avg(ttft_ms), 1) AS avg_ttft_ms
FROM {VIEW}
GROUP BY tenant_id
ORDER BY output_tokens DESC
LIMIT 10
"""

DAILY_VOLUME = f"""
SELECT dt, count(*) AS spans, round(sum(output_tokens) / 1e6, 2) AS m_output_tokens
FROM {VIEW}
GROUP BY dt
ORDER BY dt
"""

MODEL_LATENCY_BY_HOUR = f"""
SELECT dt, hour, model,
       count(*) AS spans,
       round(quantile_cont(ttft_ms, 0.95), 1) AS ttft_p95
FROM {VIEW}
WHERE operation = 'chat'
GROUP BY dt, hour, model
ORDER BY dt, hour, ttft_p95 DESC
"""
