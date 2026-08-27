"""Backfill rollups from raw spans.

Materialized views in ClickHouse are insert triggers, not continuous queries:
they see rows inserted *after* the view exists and nothing before. So creating
or replacing a rollup view leaves a hole covering everything already ingested.

This was not theoretical -- it showed up the first time the rollup views were
applied to a database that already held 107k rows, as a rollup that disagreed
with raw by exactly the pre-existing row count.

Backfill runs the same aggregation the view runs, over an explicit time window,
and inserts into the same target table. Three things make that safe:

  - it is chunked by hour, so a wide backfill does not build one enormous
    aggregation state and exhaust the memory budget;
  - it reads a bounded window, so it can be resumed after a failure;
  - AggregatingMergeTree merges states, so re-running an already-backfilled
    window double-counts. The caller must not overlap windows -- `plan()`
    reports the gap so the window can be chosen deliberately.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from clickhouse_connect.driver.client import Client

from telemetry_engine.common.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class BackfillPlan:
    """The gap between what raw holds and what the rollup covers."""

    raw_min: datetime | None
    raw_max: datetime | None
    rollup_min: datetime | None
    rollup_max: datetime | None
    raw_rows: int
    rollup_spans: int

    @property
    def missing_spans(self) -> int:
        """Spans present in raw but not represented in the rollup."""
        return max(0, self.raw_rows - self.rollup_spans)

    @property
    def needs_backfill(self) -> bool:
        return self.missing_spans > 0

    def describe(self) -> str:
        if not self.needs_backfill:
            return "rollup covers all raw spans; nothing to backfill"
        gap_end = self.rollup_min.isoformat() if self.rollup_min else "now"
        return (
            f"{self.missing_spans:,} spans in raw are not in the rollup "
            f"(raw starts {self.raw_min}, rollup starts {gap_end})"
        )


def plan(conn: Client) -> BackfillPlan:
    """Compare raw coverage against rollup coverage."""
    raw = conn.query("SELECT min(ts), max(ts), count() FROM telemetry.spans_raw").result_rows[0]
    rollup = conn.query(
        "SELECT min(ts_minute), max(ts_minute), countMerge(spans) FROM telemetry.spans_1m"
    ).result_rows[0]
    return BackfillPlan(
        raw_min=raw[0],
        raw_max=raw[1],
        raw_rows=int(raw[2]),
        rollup_min=rollup[0],
        rollup_max=rollup[1],
        rollup_spans=int(rollup[2] or 0),
    )


# The aggregation below must stay identical to 110_mv_spans_1m_v2.sql. They are
# two expressions of one definition, which is a real duplication -- if the view
# changes, this changes with it, and the integration test that compares
# backfilled output against view output is what catches a divergence.
_BACKFILL_SQL = """
INSERT INTO telemetry.spans_1m
SELECT
    toStartOfMinute(ts) AS ts_minute,
    multiIf(tenant_id = '', '__none__',
            dictHas('telemetry.dim_allowlist_dict', ('tenant_id', tenant_id)),
            tenant_id, '__other__') AS tenant_id,
    multiIf(tenant_tier = '', '__none__',
            dictHas('telemetry.dim_allowlist_dict', ('tenant_tier', tenant_tier)),
            tenant_tier, '__other__') AS tenant_tier,
    multiIf(model = '', '__none__',
            dictHas('telemetry.dim_allowlist_dict', ('model', model)),
            model, '__other__') AS model,
    multiIf(operation = '', '__none__',
            dictHas('telemetry.dim_allowlist_dict', ('operation', operation)),
            operation, '__other__') AS operation,
    multiIf(route = '', '__none__',
            dictHas('telemetry.dim_allowlist_dict', ('route', route)),
            route, '__other__') AS route,
    multiIf(region = '', '__none__',
            dictHas('telemetry.dim_allowlist_dict', ('region', region)),
            region, '__other__') AS region,
    multiIf(status_class = '', '__none__',
            dictHas('telemetry.dim_allowlist_dict', ('status_class', status_class)),
            status_class, '__other__') AS status_class,
    countState() AS spans,
    uniqState(trace_id) AS traces,
    sumState(input_tokens) AS input_tokens,
    sumState(output_tokens) AS output_tokens,
    sumState(cached_prompt_tokens) AS cached_tokens,
    quantilesTDigestStateIf(0.5, 0.95, 0.99)(ttft_ms, ttft_ms > 0) AS ttft,
    quantilesTDigestStateIf(0.5, 0.95, 0.99)(toFloat32(duration_ms), duration_ms > 0) AS duration,
    avgStateIf(itl_ms, itl_ms > 0) AS itl_avg,
    avgStateIf(queue_time_ms, queue_time_ms > 0) AS queue_time_avg,
    avgStateIf(kv_cache_utilization, kv_cache_utilization > 0) AS kv_cache_avg,
    maxState(kv_cache_utilization) AS kv_cache_max,
    countIfState(toUInt8(status_class != 'ok'), status_class != 'ok') AS errors
FROM telemetry.spans_raw
WHERE ts >= %(start)s AND ts < %(end)s
GROUP BY ts_minute, tenant_id, tenant_tier, model, operation, route, region, status_class
"""


def backfill(
    conn: Client,
    *,
    start: datetime,
    end: datetime,
) -> int:
    """Backfill spans_1m for [start, end), one hour at a time.

    Returns the number of raw spans aggregated. The caller is responsible for
    not overlapping a window that is already covered -- aggregate states merge
    rather than replace, so an overlap silently double-counts.
    """
    if start >= end:
        raise ValueError(f"empty backfill window: {start} >= {end}")

    total = int(
        conn.query(
            "SELECT count() FROM telemetry.spans_raw WHERE ts >= %(start)s AND ts < %(end)s",
            parameters={"start": start, "end": end},
        ).result_rows[0][0]
    )

    # Hour-sized chunks: bounded memory per statement, and a failure loses at
    # most one hour of work rather than the whole window.
    conn.command(
        """
        SET max_execution_time = 600
        """
    )
    cursor = start
    from datetime import timedelta

    while cursor < end:
        chunk_end = min(cursor + timedelta(hours=1), end)
        log.info("backfilling_rollup", start=cursor.isoformat(), end=chunk_end.isoformat())
        conn.command(_BACKFILL_SQL, parameters={"start": cursor, "end": chunk_end})
        cursor = chunk_end

    return total
