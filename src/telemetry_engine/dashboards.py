"""Grafana dashboards, generated as code.

Hand-maintained Grafana JSON drifts: panels get edited in the browser, never
exported, and vanish on the next redeploy. Generating it means a panel's query
lives next to the reasoning for it, and shared expressions (the corrected
latency formula, the cardinality bucket split) have exactly one definition.

Generated files land in `deploy/grafana/provisioning/dashboards/json/` and are
committed, so provisioning works on a fresh clone without running Python.

A note on the cardinality dashboard, because it is the one with a real design
constraint. Phase 4 shipped a bug where `__other__` conflated "this span has no
model" with "this model is not registered": 18,243 routine tool spans sat in the
same bucket as 300 genuinely unregistered ones. The totals looked healthy. That
failure mode is native to dashboards -- an aggregate that is technically correct
and practically blind -- so these panels are built so it cannot recur:

  - `__other__` and `__none__` are NEVER summed into one number or one series;
  - the unregistered count is shown as an absolute count with a red threshold at
    >0, not as a share, because a share of a large routine bucket rounds to
    nothing;
  - one panel lists the actual unregistered VALUES, so the dashboard names the
    offender ("shadow-deploy-v9") instead of reporting a percentage that someone
    would have to think to investigate.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from telemetry_engine.cardinality.registry import Registry, load_registry
from telemetry_engine.config import REPO_ROOT
from telemetry_engine.ingest.latency import LATENCY_EXPR

DASHBOARD_DIR = REPO_ROOT / "deploy" / "grafana" / "provisioning" / "dashboards" / "json"
DATASOURCE_UID = "clickhouse-telemetry"


# Time filtering uses the ClickHouse plugin's own macro, which the plugin
# BACKEND expands. Grafana's $__from / $__to globals were tried first and are
# interpolated by the frontend only: they render correctly in a browser and
# fail with a syntax error through /api/ds/query and in alert rules, where the
# raw "${__from}" reaches ClickHouse verbatim. Verified against both paths.
def time_filter(column: str) -> str:
    return f"$__timeFilter({column})"


FORMAT_TABLE = 1
FORMAT_TIMESERIES = 2

# ClickHouse alias trap, learned the hard way: never alias an aggregate to the
# name of the state column it reads. `countMerge(spans) AS "spans"` makes the
# alias shadow the column, so a SECOND `countMerge(spans)` in the same SELECT
# resolves to the UInt64 alias and fails with "Illegal type UInt64 ... with
# Merge suffix". It only breaks when a query references the column twice, so it
# lurks until someone adds a ratio.

_DATASOURCE = {"type": "grafana-clickhouse-datasource", "uid": DATASOURCE_UID}


def _target(sql: str, fmt: int, ref: str = "A") -> dict[str, Any]:
    return {
        "datasource": _DATASOURCE,
        "editorType": "sql",
        "format": fmt,
        "meta": {"builderOptions": {"fields": [], "limit": 100, "mode": "list"}},
        "queryType": "sql",
        "rawSql": sql.strip(),
        "refId": ref,
    }


def _panel(
    *,
    title: str,
    kind: str,
    sql: str,
    fmt: int,
    gridPos: dict[str, int],
    panel_id: int,
    unit: str | None = None,
    description: str = "",
    thresholds: list[dict[str, Any]] | None = None,
    color_mode: str = "value",
    extra_options: dict[str, Any] | None = None,
    overrides: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    defaults: dict[str, Any] = {"custom": {}}
    if unit:
        defaults["unit"] = unit
    if thresholds:
        defaults["thresholds"] = {"mode": "absolute", "steps": thresholds}
        defaults["color"] = {"mode": "thresholds"}

    options: dict[str, Any] = {}
    if kind == "stat":
        options = {
            "colorMode": color_mode,
            "graphMode": "area",
            "justifyMode": "auto",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "textMode": "auto",
        }
    elif kind == "timeseries":
        options = {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True}}
    elif kind == "table":
        options = {"showHeader": True}
    if extra_options:
        options.update(extra_options)

    panel: dict[str, Any] = {
        "id": panel_id,
        "type": kind,
        "title": title,
        "description": description,
        "datasource": _DATASOURCE,
        "gridPos": gridPos,
        "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
        "options": options,
        "targets": [_target(sql, fmt)],
    }
    return panel


def _text_panel(*, title: str, content: str, gridPos: dict[str, int], panel_id: int):
    return {
        "id": panel_id,
        "type": "text",
        "title": title,
        "gridPos": gridPos,
        "options": {"mode": "markdown", "content": content.strip()},
    }


def _dashboard(
    *, uid: str, title: str, description: str, panels: list[dict[str, Any]], refresh="30s"
):
    return {
        "uid": uid,
        "title": title,
        "description": description,
        "tags": ["llm-telemetry"],
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 1,
        "editable": True,
        "refresh": refresh,
        "time": {"from": "now-1h", "to": "now"},
        "panels": panels,
    }


def _steps(*pairs: tuple[str, float | None]) -> list[dict[str, Any]]:
    return [{"color": color, "value": value} for color, value in pairs]


# --- Overview -----------------------------------------------------------------


def build_overview() -> dict[str, Any]:
    """Workload dashboard: what the LLM fleet is doing.

    Reads spans_1m throughout. Querying spans_raw for these would work today and
    fall over at volume -- the rollups exist precisely so dashboards never scan
    raw telemetry.
    """
    window = time_filter("ts_minute")
    panels = [
        _panel(
            title="Spans/sec",
            kind="stat",
            panel_id=1,
            gridPos={"h": 4, "w": 4, "x": 0, "y": 0},
            fmt=FORMAT_TABLE,
            unit="ops",
            description="Span rate over the selected window. Spans, not requests: one "
            "agent invocation produces several.",
            sql=f"SELECT countMerge(spans) / 60 AS value FROM telemetry.spans_1m WHERE {window}",
        ),
        _panel(
            title="Traces",
            kind="stat",
            panel_id=2,
            gridPos={"h": 4, "w": 4, "x": 4, "y": 0},
            fmt=FORMAT_TABLE,
            description="Distinct traces (approximate, from mergeable uniq states).",
            sql=f"SELECT uniqMerge(traces) AS value FROM telemetry.spans_1m WHERE {window}",
        ),
        _panel(
            title="Output tokens/sec",
            kind="stat",
            panel_id=3,
            gridPos={"h": 4, "w": 4, "x": 8, "y": 0},
            unit="ops",
            fmt=FORMAT_TABLE,
            description="Generated tokens per second: the throughput number that maps "
            "most directly to serving cost.",
            sql=f"""
SELECT sumMerge(output_tokens) / 60 AS value
FROM telemetry.spans_1m WHERE {window}
""",
        ),
        _panel(
            title="Error rate",
            kind="stat",
            panel_id=4,
            gridPos={"h": 4, "w": 4, "x": 12, "y": 0},
            unit="percentunit",
            fmt=FORMAT_TABLE,
            thresholds=_steps(("green", None), ("yellow", 0.02), ("red", 0.05)),
            description="Share of spans whose status_class is not ok.",
            sql=f"""
SELECT countIfMerge(errors) / nullIf(countMerge(spans), 0) AS value
FROM telemetry.spans_1m WHERE {window}
""",
        ),
        _panel(
            title="KV-cache utilization (max)",
            kind="stat",
            panel_id=5,
            gridPos={"h": 4, "w": 4, "x": 16, "y": 0},
            unit="percentunit",
            fmt=FORMAT_TABLE,
            thresholds=_steps(("green", None), ("yellow", 0.8), ("red", 0.95)),
            description=(
                "Peak KV-cache occupancy. Above ~0.85 the scheduler starts queueing, "
                "which is the mechanism behind most TTFT spikes."
            ),
            sql=f"SELECT maxMerge(kv_cache_max) AS value FROM telemetry.spans_1m WHERE {window}",
        ),
        _panel(
            title="Active tenants",
            kind="stat",
            panel_id=6,
            gridPos={"h": 4, "w": 4, "x": 20, "y": 0},
            fmt=FORMAT_TABLE,
            description="Individually attributed tenants. Excludes the sentinel buckets, "
            "so this counts tenants you can actually name.",
            sql=f"""
SELECT uniqExact(tenant_id) AS value FROM telemetry.spans_1m
WHERE {window} AND tenant_id NOT IN ('__other__', '__none__')
""",
        ),
        _panel(
            title="Token throughput",
            kind="timeseries",
            panel_id=7,
            gridPos={"h": 8, "w": 12, "x": 0, "y": 4},
            fmt=FORMAT_TIMESERIES,
            unit="short",
            description="Input and output tokens per minute. The cost signal.",
            sql=f"""
SELECT ts_minute AS time,
       sumMerge(input_tokens) AS "input tokens",
       sumMerge(output_tokens) AS "output tokens"
FROM telemetry.spans_1m WHERE {window}
GROUP BY time ORDER BY time
""",
        ),
        _panel(
            title="Time to first token",
            kind="timeseries",
            panel_id=8,
            gridPos={"h": 8, "w": 12, "x": 12, "y": 4},
            fmt=FORMAT_TIMESERIES,
            unit="ms",
            description="What a user actually perceives as latency.",
            sql=f"""
SELECT ts_minute AS time,
       quantilesTDigestMerge(0.5)(ttft)[1] AS p50,
       quantilesTDigestMerge(0.95)(ttft)[1] AS p95,
       quantilesTDigestMerge(0.99)(ttft)[1] AS p99
FROM telemetry.spans_1m WHERE {window}
GROUP BY time ORDER BY time
""",
        ),
        _panel(
            title="TTFT p95 by model",
            kind="timeseries",
            panel_id=9,
            gridPos={"h": 8, "w": 12, "x": 0, "y": 12},
            fmt=FORMAT_TIMESERIES,
            unit="ms",
            description="Bigger models are slower per token, so these series should stay "
            "ordered by model size. They crossing over means something changed.",
            sql=f"""
SELECT ts_minute AS time, model,
       quantilesTDigestMerge(0.95)(ttft)[1] AS p95
FROM telemetry.spans_1m
WHERE {window} AND model NOT IN ('__none__')
GROUP BY time, model ORDER BY time
""",
        ),
        _panel(
            title="KV-cache utilization",
            kind="timeseries",
            panel_id=10,
            gridPos={"h": 8, "w": 12, "x": 12, "y": 12},
            fmt=FORMAT_TIMESERIES,
            unit="percentunit",
            description=(
                "Average and peak occupancy. The gap between them explains latency "
                "that an average alone makes look fine."
            ),
            sql=f"""
SELECT ts_minute AS time,
       avgMerge(kv_cache_avg) AS "avg",
       maxMerge(kv_cache_max) AS "max"
FROM telemetry.spans_1m WHERE {window}
GROUP BY time ORDER BY time
""",
        ),
        _panel(
            title="Top tenants",
            kind="table",
            panel_id=11,
            gridPos={"h": 9, "w": 12, "x": 0, "y": 20},
            fmt=FORMAT_TABLE,
            description="Zipfian by design: a handful of tenants dominate every window.",
            sql=f"""
SELECT tenant_id AS "tenant",
       any(tenant_tier) AS "tier",
       countMerge(spans) AS "span count",
       uniqMerge(traces) AS "trace count",
       sumMerge(output_tokens) AS "output tokens",
       round(quantilesTDigestMerge(0.95)(ttft)[1], 1) AS "ttft p95 (ms)",
       round(countIfMerge(errors) / nullIf(countMerge(spans), 0), 4) AS "error rate"
FROM telemetry.spans_1m WHERE {window}
GROUP BY tenant_id ORDER BY "span count" DESC LIMIT 20
""",
        ),
        _panel(
            title="Errors by class",
            kind="timeseries",
            panel_id=12,
            gridPos={"h": 9, "w": 12, "x": 12, "y": 20},
            fmt=FORMAT_TIMESERIES,
            description="server_error and timeout cluster under KV-cache pressure.",
            sql=f"""
SELECT ts_minute AS time, status_class,
       countMerge(spans) AS "error spans"
FROM telemetry.spans_1m
WHERE {window} AND status_class != 'ok'
GROUP BY time, status_class ORDER BY time
""",
        ),
    ]
    return _dashboard(
        uid="llm-overview",
        title="LLM Telemetry - Overview",
        description="Workload view: throughput, latency, cache pressure, per-tenant breakdown.",
        panels=panels,
    )


# --- Pipeline health ----------------------------------------------------------


def build_pipeline_health() -> dict[str, Any]:
    """The pipeline observing itself: lag, drops, ingest latency.

    Consumer lag is the primary SLI (ADR-003). Under this design overload shows
    up here as lag long before it shows up as loss, and the drop counter is
    where genuine loss appears.
    """
    window = time_filter("sampled_at")
    panels = [
        _panel(
            title="Consumer lag (current)",
            kind="stat",
            panel_id=1,
            gridPos={"h": 5, "w": 5, "x": 0, "y": 0},
            fmt=FORMAT_TABLE,
            color_mode="background",
            thresholds=_steps(("green", None), ("yellow", 5000), ("red", 100000)),
            description=(
                "Messages the ClickHouse consumer group has not read. The primary SLI: "
                "Redpanda absorbs bursts, so overload appears here as lag rather than loss."
            ),
            sql="""
SELECT total_lag AS value FROM telemetry.pipeline_health
ORDER BY sampled_at DESC LIMIT 1
""",
        ),
        _panel(
            title="Dropped spans (total)",
            kind="stat",
            panel_id=2,
            gridPos={"h": 5, "w": 5, "x": 5, "y": 0},
            fmt=FORMAT_TABLE,
            color_mode="background",
            # Any drop at all is worth seeing. The design accepts loss under
            # overload on purpose -- the point is that it is never silent.
            thresholds=_steps(("green", None), ("red", 1)),
            description=(
                "Spans the collector dropped rather than backpressuring the endpoint "
                "(send-failed + enqueue-failed). Non-zero is expected under burst; "
                "it must never be invisible."
            ),
            sql="""
SELECT send_failed_spans + enqueue_failed_spans AS value
FROM telemetry.pipeline_health ORDER BY sampled_at DESC LIMIT 1
""",
        ),
        _panel(
            title="Export queue",
            kind="stat",
            panel_id=3,
            gridPos={"h": 5, "w": 5, "x": 10, "y": 0},
            fmt=FORMAT_TABLE,
            unit="percentunit",
            thresholds=_steps(("green", None), ("yellow", 0.5), ("red", 0.9)),
            description="Collector export queue utilization. At 100% it drops rather than blocks.",
            sql="""
SELECT queue_size / nullIf(queue_capacity, 0) AS value
FROM telemetry.pipeline_health ORDER BY sampled_at DESC LIMIT 1
""",
        ),
        _panel(
            title="Refused spans",
            kind="stat",
            panel_id=4,
            gridPos={"h": 5, "w": 4, "x": 15, "y": 0},
            fmt=FORMAT_TABLE,
            thresholds=_steps(("green", None), ("yellow", 1)),
            description="Receiver refusals: the memory_limiter shedding load.",
            sql="""
SELECT refused_spans AS value FROM telemetry.pipeline_health
ORDER BY sampled_at DESC LIMIT 1
""",
        ),
        _panel(
            title="Active parts",
            kind="stat",
            panel_id=5,
            gridPos={"h": 5, "w": 5, "x": 19, "y": 0},
            fmt=FORMAT_TABLE,
            thresholds=_steps(("green", None), ("yellow", 300), ("red", 1000)),
            description="MergeTree parts on spans_raw. Runaway part count is the classic "
            "Kafka-engine failure: merges fall behind and inserts start failing.",
            sql="""
SELECT count() AS value FROM system.parts
WHERE database = 'telemetry' AND table = 'spans_raw' AND active
""",
        ),
        _panel(
            title="Consumer lag over time",
            kind="timeseries",
            panel_id=6,
            gridPos={"h": 9, "w": 12, "x": 0, "y": 5},
            fmt=FORMAT_TIMESERIES,
            description="Lag growing then recovering is the system working as designed. "
            "Lag that never recovers means ClickHouse cannot keep up at this rate.",
            sql=f"""
SELECT sampled_at AS time,
       total_lag AS "total lag",
       max_partition_lag AS "worst partition"
FROM telemetry.pipeline_health WHERE {window}
ORDER BY time
""",
        ),
        _panel(
            title="Dropped spans per sample",
            kind="timeseries",
            panel_id=7,
            gridPos={"h": 9, "w": 12, "x": 12, "y": 5},
            fmt=FORMAT_TIMESERIES,
            description="Differenced from the cumulative counter, so a burst of loss is "
            "visible as a spike rather than a step in a rising line.",
            sql=f"""
SELECT sampled_at AS time,
       greatest(0, toInt64(send_failed_spans + enqueue_failed_spans)
                   - toInt64(any(send_failed_spans + enqueue_failed_spans)
                             OVER (ORDER BY sampled_at ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)))
           AS "dropped"
FROM telemetry.pipeline_health WHERE {window}
ORDER BY time
""",
        ),
        _panel(
            title="Ingest latency (span end to queryable)",
            kind="timeseries",
            panel_id=8,
            gridPos={"h": 9, "w": 12, "x": 0, "y": 14},
            fmt=FORMAT_TIMESERIES,
            unit="s",
            description=(
                "Measured from span END, not span start: spans are backdated to trace "
                "start, so not subtracting duration_ms would report the simulated LLM "
                "call as pipeline latency."
            ),
            sql=f"""
SELECT toStartOfMinute(ts) AS time,
       quantile(0.5)({LATENCY_EXPR}) AS p50,
       quantile(0.95)({LATENCY_EXPR}) AS p95
FROM telemetry.spans_raw
WHERE {time_filter("ts")}
GROUP BY time ORDER BY time
""",
        ),
        _panel(
            title="Collector throughput",
            kind="timeseries",
            panel_id=9,
            gridPos={"h": 9, "w": 12, "x": 12, "y": 14},
            fmt=FORMAT_TIMESERIES,
            description="Accepted vs sent, differenced per sample. A widening gap means "
            "the exporter is falling behind the receiver.",
            sql=f"""
SELECT sampled_at AS time,
       greatest(0, toInt64(accepted_spans) - toInt64(any(accepted_spans)
           OVER (ORDER BY sampled_at ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING))) AS "accepted",
       greatest(0, toInt64(sent_spans) - toInt64(any(sent_spans)
           OVER (ORDER BY sampled_at ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING))) AS "sent"
FROM telemetry.pipeline_health WHERE {window}
ORDER BY time
""",
        ),
    ]
    return _dashboard(
        uid="pipeline-health",
        title="LLM Telemetry - Pipeline Health",
        description="The pipeline observing itself: consumer lag, dropped spans, ingest latency.",
        panels=panels,
        refresh="10s",
    )


# --- Cardinality --------------------------------------------------------------


def _unregistered_values_sql(registry: Registry) -> str:
    """List the actual dimension values that are not on the allowlist.

    This is the panel that turns "0.6% other" into "shadow-deploy-v9 is
    emitting". Reads spans_raw, because the rollup has already replaced the
    offending value with a sentinel -- by the time data reaches spans_1m, the
    information needed to act on it is gone by design.
    """
    parts = []
    for dimension in registry.dimensions:
        parts.append(
            f"""
SELECT '{dimension.name}' AS "dimension", {dimension.column} AS "value", count() AS "spans"
FROM telemetry.spans_raw
WHERE {time_filter("ts")}
  AND {dimension.column} != ''
  AND NOT dictHas('telemetry.dim_allowlist_dict', ('{dimension.name}', {dimension.column}))
GROUP BY "value"
""".strip()
        )
    union = "\nUNION ALL\n".join(parts)
    return f'SELECT * FROM (\n{union}\n) ORDER BY "spans" DESC LIMIT 50'


def _bucket_breakdown_sql(registry: Registry) -> str:
    """Per dimension: registered / unregistered / absent, as three columns.

    Deliberately three separate columns rather than one "bucketed %". Merging
    them is exactly the mistake that hid 300 unregistered spans behind 18,243
    routine ones -- the shape of this query is the fix.
    """
    parts = []
    for dimension in registry.dimensions:
        col = dimension.column
        parts.append(
            f"""
SELECT '{dimension.name}' AS "dimension",
       countMergeIf(spans, {col} NOT IN ('__other__', '__none__')) AS "registered",
       countMergeIf(spans, {col} = '__other__') AS "UNREGISTERED",
       countMergeIf(spans, {col} = '__none__') AS "absent (routine)",
       uniqExactIf({col}, {col} NOT IN ('__other__', '__none__')) AS "distinct values",
       {dimension.budget} AS "budget"
FROM telemetry.spans_1m
WHERE {time_filter("ts_minute")}
""".strip()
        )
    union = "\nUNION ALL\n".join(parts)
    return f'SELECT * FROM (\n{union}\n) ORDER BY "UNREGISTERED" DESC'


def build_cardinality() -> dict[str, Any]:
    """Cardinality guard dashboard.

    Built around one rule: never show a number that could be healthy in
    aggregate while hiding an unhealthy component. See the module docstring.
    """
    registry = load_registry()
    window = time_filter("ts_minute")

    # Sum of __other__ across every dimension, as an absolute count. Not a
    # share: a share is exactly the number that made this invisible before.
    other_terms = " + ".join(
        f"countMergeIf(spans, {d.column} = '__other__')" for d in registry.dimensions
    )
    none_terms = " + ".join(
        f"countMergeIf(spans, {d.column} = '__none__')" for d in registry.dimensions
    )

    panels = [
        _panel(
            title="UNREGISTERED spans (__other__)",
            kind="stat",
            panel_id=1,
            gridPos={"h": 6, "w": 7, "x": 0, "y": 0},
            fmt=FORMAT_TABLE,
            color_mode="background",
            # Red at >0, deliberately. An absolute count with a zero threshold
            # cannot be diluted by routine traffic the way a percentage can.
            thresholds=_steps(("green", None), ("red", 1)),
            description=(
                "Spans carrying a dimension value that is not on the allowlist. "
                "ANY non-zero value is actionable: a shadow deployment, a new region, "
                "or a stale allowlist. Shown as an absolute count, never a share."
            ),
            sql=f"SELECT {other_terms} AS value FROM telemetry.spans_1m WHERE {window}",
        ),
        _panel(
            title="Absent dimensions (__none__) - routine",
            kind="stat",
            panel_id=2,
            gridPos={"h": 6, "w": 7, "x": 7, "y": 0},
            fmt=FORMAT_TABLE,
            color_mode="none",
            description=(
                "Spans that simply do not carry a dimension: a tool span has no model. "
                "Expected and ignorable. Kept in its own panel because merging it with "
                "__other__ is what previously hid 300 unregistered spans behind 18,243 "
                "routine ones."
            ),
            sql=f"SELECT {none_terms} AS value FROM telemetry.spans_1m WHERE {window}",
        ),
        _panel(
            title="Rollup rows / minute",
            kind="stat",
            panel_id=3,
            gridPos={"h": 6, "w": 5, "x": 14, "y": 0},
            fmt=FORMAT_TABLE,
            description=(
                f"Worst minute in range. Declared bound is {registry.max_rows_per_bucket:,} "
                "(product of budgets + 2 sentinels) -- a true but loose ceiling, since "
                "dimensions correlate heavily in practice."
            ),
            sql=f"""
SELECT max(n) AS value FROM (
  SELECT ts_minute, count() AS n FROM telemetry.spans_1m WHERE {window} GROUP BY ts_minute
)
""",
        ),
        _panel(
            title="Rollup compression",
            kind="stat",
            panel_id=4,
            gridPos={"h": 6, "w": 5, "x": 19, "y": 0},
            fmt=FORMAT_TABLE,
            description="Raw rows per rollup row. If this is near 1, the rollup is not "
            "earning its storage.",
            sql="""
SELECT round(
    (SELECT sum(rows) FROM system.parts
     WHERE database='telemetry' AND table='spans_raw' AND active)
  / nullIf((SELECT sum(rows) FROM system.parts
     WHERE database='telemetry' AND table='spans_1m' AND active), 0), 1) AS value
""",
        ),
        _panel(
            title="Which values are unregistered?",
            kind="table",
            panel_id=5,
            gridPos={"h": 10, "w": 12, "x": 0, "y": 6},
            fmt=FORMAT_TABLE,
            description=(
                "The offender, by name. Reads spans_raw, because the rollup has already "
                "replaced the value with a sentinel -- by then the information needed to "
                "act on it is gone by design. This panel is why a 0.6% __other__ share "
                "does not require anyone to think to investigate."
            ),
            sql=_unregistered_values_sql(registry),
        ),
        _panel(
            title="Bucket breakdown by dimension",
            kind="table",
            panel_id=6,
            gridPos={"h": 10, "w": 12, "x": 12, "y": 6},
            fmt=FORMAT_TABLE,
            description=(
                "Three separate columns on purpose. A single 'bucketed %' column would "
                "average an actionable signal into a routine one."
            ),
            sql=_bucket_breakdown_sql(registry),
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "UNREGISTERED"},
                    "properties": [
                        {
                            "id": "custom.cellOptions",
                            "value": {"type": "color-background"},
                        },
                        {
                            "id": "thresholds",
                            "value": {
                                "mode": "absolute",
                                "steps": _steps(("green", None), ("red", 1)),
                            },
                        },
                    ],
                }
            ],
        ),
        _panel(
            title="Unregistered spans over time, by dimension",
            kind="timeseries",
            panel_id=7,
            gridPos={"h": 9, "w": 24, "x": 0, "y": 16},
            fmt=FORMAT_TIMESERIES,
            description=(
                "__none__ is excluded from this query by construction, so a routine "
                "bucket cannot swamp the series. A step change here means someone "
                "deployed something the registry does not know about."
            ),
            sql=f"""
SELECT ts_minute AS time,
       {
                ", ".join(
                    f'''countMergeIf(spans, {d.column} = '__other__') AS "{d.name}"'''
                    for d in registry.dimensions
                )
            }
FROM telemetry.spans_1m WHERE {window}
GROUP BY time ORDER BY time
""",
        ),
        _text_panel(
            title="How to read this dashboard",
            panel_id=8,
            gridPos={"h": 6, "w": 24, "x": 0, "y": 25},
            content="""
**`__other__` is actionable. `__none__` is not. They are never combined.**

- **`__other__`** — the span carried a dimension value that is not on the
  allowlist. Something is emitting data the registry does not know about: a
  shadow deployment, a new region, or an allowlist that has gone stale. Fix by
  registering the value in `schemas/clickhouse/dimensions.yaml` and running
  `telemetry-engine dimensions apply`, or by fixing the emitter.
- **`__none__`** — the span does not carry that dimension at all. A tool span
  has no model; an embeddings call has no completion. Routine.

These were one bucket until it was measured: 18,243 routine tool spans shared
it with exactly 300 genuinely unregistered ones, so the totals looked healthy
while the actionable signal was 2% of its own bucket. Every panel here is built
so that cannot recur — absolute counts rather than shares, separate series, and
a panel that names the offending value instead of reporting a percentage.
""",
        ),
    ]
    return _dashboard(
        uid="cardinality-guard",
        title="LLM Telemetry - Cardinality Guard",
        description="Is the cardinality bound holding, and is anything unregistered emitting?",
        panels=panels,
        refresh="1m",
    )


# --- Emit ---------------------------------------------------------------------


def build_all() -> dict[str, dict[str, Any]]:
    return {
        "llm-overview.json": build_overview(),
        "pipeline-health.json": build_pipeline_health(),
        "cardinality-guard.json": build_cardinality(),
    }


def write_all(target_dir: Path | None = None) -> list[Path]:
    """Generate every dashboard to disk."""
    out_dir = target_dir or DASHBOARD_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for filename, dashboard in build_all().items():
        path = out_dir / filename
        path.write_text(json.dumps(dashboard, indent=2) + "\n", encoding="utf-8")
        written.append(path)
    return written
