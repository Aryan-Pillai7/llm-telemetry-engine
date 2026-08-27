"""Enforcement of the dimension registry against live data.

The registry says what *may* be a rollup key. This module makes that true of the
running system:

  - it syncs the allowlist into a ClickHouse table backing a dictionary, which
    the rollup materialized views consult per row;
  - it recomputes the `top_k` allowlists (tenants) from recent traffic;
  - it reports actual cardinality against budget, so the bound can be audited
    rather than believed.

Why the enforcement lives in ClickHouse rather than in Python: ClickHouse
consumes Redpanda directly (ADR-002), so there is no Python process on the
ingest path to intercept. Putting the guard in the materialized view means it
runs on every row with no extra service, and the rollup tables are structurally
incapable of exceeding their budget.
"""

from __future__ import annotations

from dataclasses import dataclass

from clickhouse_connect.driver.client import Client

from telemetry_engine.cardinality.registry import Dimension, Registry, load_registry
from telemetry_engine.common.logging import get_logger

log = get_logger(__name__)

ALLOWLIST_TABLE = "telemetry.dim_allowlist"
ALLOWLIST_DICT = "telemetry.dim_allowlist_dict"


@dataclass(frozen=True)
class DimensionStatus:
    """Observed cardinality of one dimension against its budget."""

    name: str
    allowlisted: int
    budget: int
    observed_distinct: int
    other_bucket_rows: int
    none_bucket_rows: int
    total_rows: int

    @property
    def headroom(self) -> int:
        return self.budget - self.allowlisted

    @property
    def other_share(self) -> float:
        """Fraction of rows carrying an UNREGISTERED value.

        The actionable number. A non-zero share means something is emitting a
        dimension value nobody registered -- a shadow deployment, a new region,
        or an allowlist that has gone stale. Kept strictly separate from
        `none_share`, which is normal.
        """
        return self.other_bucket_rows / self.total_rows if self.total_rows else 0.0

    @property
    def none_share(self) -> float:
        """Fraction of rows where the dimension does not apply at all.

        Expected and benign: a tool span has no model. Reported so it cannot be
        mistaken for unregistered traffic.
        """
        return self.none_bucket_rows / self.total_rows if self.total_rows else 0.0

    @property
    def within_budget(self) -> bool:
        # +2 for the two sentinel buckets.
        return self.observed_distinct <= self.budget + 2


@dataclass
class SyncResult:
    """What a sync changed."""

    static_values: int = 0
    top_k_values: dict[str, int] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.top_k_values is None:
            self.top_k_values = {}


def sync_static(conn: Client, registry: Registry) -> int:
    """Write every statically-enumerated dimension value into the allowlist."""
    rows = [
        [dimension.name, value]
        for dimension in registry.static_dimensions
        for value in dimension.values
    ]
    if rows:
        conn.insert(ALLOWLIST_TABLE, rows, column_names=["dimension", "value"])
    return len(rows)


def compute_top_k(conn: Client, dimension: Dimension) -> list[str]:
    """Return the top-K values of a dimension by recent span volume.

    This is the mechanism that makes an unbounded dimension safe without
    discarding the information anyone actually wants: the loud values stay
    individually attributed, the long tail aggregates. With zipfian traffic the
    top 200 tenants cover essentially everything, and the tail that collapses
    into `__other__` was never going to be read per-tenant anyway.
    """
    query = f"""
        SELECT {dimension.column} AS value
        FROM telemetry.spans_raw
        WHERE ts > now() - INTERVAL {dimension.top_k_window_hours} HOUR
          AND {dimension.column} != ''
        GROUP BY value
        ORDER BY count() DESC
        LIMIT {dimension.budget}
    """
    return [str(row[0]) for row in conn.query(query).result_rows]


def sync_top_k(conn: Client, registry: Registry) -> dict[str, int]:
    """Recompute and store every top-K allowlist."""
    counts: dict[str, int] = {}
    for dimension in registry.top_k_dimensions:
        values = compute_top_k(conn, dimension)
        if values:
            conn.insert(
                ALLOWLIST_TABLE,
                [[dimension.name, value] for value in values],
                column_names=["dimension", "value"],
            )
        counts[dimension.name] = len(values)
        log.info(
            "top_k_allowlist_synced",
            dimension=dimension.name,
            values=len(values),
            budget=dimension.budget,
        )
    return counts


def rollup_columns(conn: Client, table: str = "telemetry.spans_1m") -> set[str]:
    """Columns that actually exist on the rollup table."""
    rows = conn.query(
        "SELECT name FROM system.columns WHERE database = %(db)s AND table = %(t)s",
        parameters={"db": table.split(".")[0], "t": table.split(".")[-1]},
    ).result_rows
    return {str(r[0]) for r in rows}


def sync(conn: Client, registry: Registry | None = None) -> SyncResult:
    """Sync the whole registry into ClickHouse and reload the dictionary.

    Validates the registry against the real rollup schema first. A dimension
    naming a column that does not exist would otherwise sync happily and then
    group by nothing -- the rollup would keep working and quietly lose that
    dimension. `Registry.validate_columns` was written for exactly this and,
    until the inert-guard audit, was never called from anywhere.
    """
    reg = registry or load_registry()
    reg.validate_columns(rollup_columns(conn))
    result = SyncResult()
    result.static_values = sync_static(conn, reg)
    result.top_k_values = sync_top_k(conn, reg)

    # The dictionary caches the allowlist in memory; without an explicit reload
    # the views keep using the previous contents until its LIFETIME expires,
    # which makes a manual sync look like it did nothing.
    conn.command(f"SYSTEM RELOAD DICTIONARY {ALLOWLIST_DICT}")
    return result


def status(
    conn: Client, registry: Registry | None = None, *, table: str = "telemetry.spans_1m"
) -> list[DimensionStatus]:
    """Report observed cardinality against budget for every dimension.

    Reads the rollup table, because that is where the bound is supposed to
    hold. spans_raw is deliberately unbounded and is not audited here.
    """
    reg = registry or load_registry()
    total = int(conn.query(f"SELECT count() FROM {table}").result_rows[0][0])

    out: list[DimensionStatus] = []
    for dimension in reg.dimensions:
        row = conn.query(
            f"""
            SELECT
                uniqExact({dimension.column}),
                countIf({dimension.column} = %(other)s),
                countIf({dimension.column} = %(none)s)
            FROM {table}
        """,
            parameters={"other": reg.other_bucket, "none": reg.none_bucket},
        ).result_rows[0]

        allowlisted = int(
            conn.query(
                f"SELECT count() FROM {ALLOWLIST_TABLE} FINAL WHERE dimension = %(d)s",
                parameters={"d": dimension.name},
            ).result_rows[0][0]
        )

        out.append(
            DimensionStatus(
                name=dimension.name,
                allowlisted=allowlisted,
                budget=dimension.budget,
                observed_distinct=int(row[0]),
                other_bucket_rows=int(row[1]),
                none_bucket_rows=int(row[2]),
                total_rows=total,
            )
        )
    return out
