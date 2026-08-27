"""Correct pipeline-latency measurement.

This module exists because latency was measured wrongly twice during Phase 3,
in two different ways, and Phase 6 reports lag and recovery time as the
project's headline result. Encoding the corrections here -- rather than in a
note someone has to remember to re-read -- means the wrong measurement is not
merely discouraged, it is unavailable.

Two corrections, both of which inflated the number:

1. **Subtract the span's own duration.** Spans are backdated to trace start
   (see `emitters/otlp.py`), so `ingested_at - ts` includes the simulated LLM
   call itself. A 10-second call adds 10 seconds of "latency" that the pipeline
   had nothing to do with. Measuring from span *end* gave p50 3.2s where the
   naive form gave 5.9s.

2. **Never measure while a backlog is draining.** Fresh spans queue behind the
   backlog, so a measurement taken mid-drain describes recovery, not steady
   state. Filtering on `ingested_at` makes this worse by selecting backlog rows
   specifically. The naive form reported p95 24s against a true 8.5s.

`measure()` applies the first correction structurally and refuses to run
without the second being checked.
"""

from __future__ import annotations

from dataclasses import dataclass

from clickhouse_connect.driver.client import Client

from telemetry_engine.common.logging import get_logger

log = get_logger(__name__)

# Consumer lag, in messages, above which the pipeline is considered to be
# catching up rather than keeping up. Not zero: a healthy pipeline at 5k spans/s
# always has a little in flight.
DRAINED_LAG_THRESHOLD = 500


class BacklogNotDrainedError(RuntimeError):
    """Raised when a latency measurement is attempted mid-drain.

    Deliberately an error rather than a warning. A quietly wrong headline number
    is worse than a failed measurement, because it still gets reported.
    """


@dataclass(frozen=True)
class LatencyStats:
    """Pipeline latency from span end to queryable, in seconds."""

    rows: int
    avg_s: float
    p50_s: float
    p95_s: float
    p99_s: float
    max_s: float

    def format(self) -> str:
        return (
            f"n={self.rows:,} avg={self.avg_s:.2f}s p50={self.p50_s:.2f}s "
            f"p95={self.p95_s:.2f}s p99={self.p99_s:.2f}s max={self.max_s:.2f}s"
        )


# The correction lives in this one expression. `duration_ms` is subtracted so
# the result measures the pipeline rather than the simulated inference call.
LATENCY_EXPR = "(dateDiff('millisecond', ts, ingested_at) - duration_ms) / 1000.0"


def latency_sql(*, since_expr: str) -> str:
    """Build the latency query.

    `since_expr` must constrain **event time** (`ts`), not `ingested_at`.
    Filtering on ingest time selects exactly the backlog rows that corrupt the
    measurement, which is the trap this module exists to close.
    """
    if "ingested_at" in since_expr:
        raise ValueError(
            "filter latency windows on ts (event time), not ingested_at: "
            "an ingested_at filter selects backlog rows and inflates the result"
        )
    return f"""
        SELECT
            count(),
            avg({LATENCY_EXPR}),
            quantile(0.50)({LATENCY_EXPR}),
            quantile(0.95)({LATENCY_EXPR}),
            quantile(0.99)({LATENCY_EXPR}),
            max({LATENCY_EXPR})
        FROM telemetry.spans_raw
        WHERE {since_expr}
    """


def assert_drained(total_lag: int, *, threshold: int = DRAINED_LAG_THRESHOLD) -> None:
    """Refuse to proceed if the consumer is still catching up."""
    if total_lag > threshold:
        raise BacklogNotDrainedError(
            f"consumer lag is {total_lag:,} (> {threshold}); a latency measurement taken "
            "now describes backlog recovery, not steady state. Wait for lag to drain."
        )


def measure(
    conn: Client,
    *,
    since_expr: str,
    total_lag: int,
    require_drained: bool = True,
) -> LatencyStats:
    """Measure pipeline latency over an event-time window.

    `total_lag` must be the current consumer lag; pass it explicitly so the
    caller cannot forget that draining is a precondition. Set
    `require_drained=False` only when deliberately measuring recovery behaviour,
    as Phase 6's burst experiment does -- and label that number as recovery, not
    steady-state latency.
    """
    if require_drained:
        assert_drained(total_lag)

    row = conn.query(latency_sql(since_expr=since_expr)).result_rows[0]
    stats = LatencyStats(
        rows=int(row[0]),
        avg_s=float(row[1] or 0.0),
        p50_s=float(row[2] or 0.0),
        p95_s=float(row[3] or 0.0),
        p99_s=float(row[4] or 0.0),
        max_s=float(row[5] or 0.0),
    )
    log.info("pipeline_latency", **{"lag": total_lag, "stats": stats.format()})
    return stats
