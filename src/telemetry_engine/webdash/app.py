"""Live Pipeline: a deliberately small web view of the hot tier.

Grafana already covers operations in depth. This page exists to explain the
pipeline in thirty seconds, so it shows only what answers "is data flowing, at
volume, without loss?" -- four numbers, one throughput line, one tenant bar.

Everything is read from ClickHouse except consumer lag, which is a broker fact,
and dropped spans, which is a collector fact (see ingest/lag.py). Charts are
plain SVG drawn in the page, so a presentation laptop needs no internet access.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import FileResponse

from telemetry_engine.common.logging import get_logger
from telemetry_engine.config import Settings
from telemetry_engine.ingest.lag import read_collector, read_lag
from telemetry_engine.storage.client import connect

log = get_logger(__name__)

PAGE = Path(__file__).with_name("index.html")

# Spans land seconds after they are emitted (p95 ingest ~8.5s). Measuring the
# rate over a window that ends now would undercount the newest seconds and show
# throughput sagging while it is actually steady.
RATE_WINDOW_S = 60
RATE_LAG_S = 10


def summary(conn: Any, settings: Settings) -> dict[str, Any]:
    """Everything the page shows, in one round trip from the browser."""
    total, recent = conn.query(
        f"""
        SELECT count(),
               countIf(ts >= now() - {RATE_WINDOW_S + RATE_LAG_S}
                       AND ts < now() - {RATE_LAG_S})
        FROM telemetry.spans_raw
        """
    ).result_rows[0]

    rollup_rows = conn.query("SELECT count() FROM telemetry.spans_1m").result_rows[0][0]

    # Read from the rollup, not spans_raw: the chart is itself a small example of
    # the point the reduction tile makes.
    per_minute = conn.query(
        """
        SELECT toUnixTimestamp(ts_minute) AS t, countMerge(spans) AS n
        FROM telemetry.spans_1m
        WHERE ts_minute >= now() - INTERVAL 15 MINUTE
        GROUP BY t ORDER BY t
        """
    ).result_rows

    tenants = conn.query(
        """
        SELECT tenant_id, countMerge(spans) AS n
        FROM telemetry.spans_1m
        GROUP BY tenant_id ORDER BY n DESC
        """
    ).result_rows

    # Top 10 by name, everything else folded into __other__, which is the same
    # collapse the cardinality guard applies to unregistered tenants.
    top = [(t, n) for t, n in tenants if t != "__other__"][:10]
    shown = {t for t, _ in top}
    other = sum(n for t, n in tenants if t not in shown)

    collector = read_collector()
    try:
        lag: int | None = read_lag(settings.redpanda).total_lag
    except Exception as exc:  # broker down: show a gap, never a fabricated zero
        log.warning("webdash_lag_unavailable", error=str(exc))
        lag = None

    return {
        "total_spans": total,
        "spans_per_sec": round(recent / RATE_WINDOW_S),
        "consumer_lag": lag,
        "dropped_spans": collector.dropped_spans if collector else None,
        "rollup_rows": rollup_rows,
        "per_minute": [{"t": t, "n": n} for t, n in per_minute],
        "tenants": [{"name": t, "n": n} for t, n in top] + [{"name": "__other__", "n": other}],
    }


def create_app(settings: Settings) -> FastAPI:
    app = FastAPI(title="Live Pipeline", docs_url=None, redoc_url=None)

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(PAGE)

    @app.get("/api/summary")
    def api_summary() -> dict[str, Any]:
        # A fresh client per request: the page polls every few seconds, and a
        # dropped connection should cost one refresh rather than the server.
        conn = connect(settings.clickhouse)
        try:
            return summary(conn, settings)
        finally:
            conn.close()

    return app
