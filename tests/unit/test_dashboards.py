"""Structural invariants of the generated dashboards.

Dashboards are where "correct in aggregate, blind in practice" does the most
damage: a panel that averages an actionable signal into a routine one is not a
broken panel, it is a working panel showing a useless number. These tests
enforce the properties that keep that from happening, so a future edit to the
builder cannot quietly undo them.
"""

from __future__ import annotations

import json
import re

import pytest

from telemetry_engine.dashboards import (
    DASHBOARD_DIR,
    DATASOURCE_UID,
    build_all,
    build_cardinality,
)


@pytest.fixture(scope="module")
def dashboards():
    return build_all()


def _panels(dashboard):
    return dashboard["panels"]


def _queries(dashboard):
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            if target.get("rawSql"):
                yield panel, target["rawSql"]


# --- Wiring -------------------------------------------------------------------


def test_every_panel_has_a_query_or_is_text(dashboards) -> None:
    for name, dashboard in dashboards.items():
        for panel in _panels(dashboard):
            if panel["type"] == "text":
                continue
            assert panel.get("targets"), f"{name}: panel {panel['title']!r} has no query"


def test_every_query_targets_the_provisioned_datasource(dashboards) -> None:
    for name, dashboard in dashboards.items():
        for panel, _ in _queries(dashboard):
            assert panel["datasource"]["uid"] == DATASOURCE_UID, f"{name}: {panel['title']}"


def test_panels_have_descriptions(dashboards) -> None:
    """A panel nobody can interpret is a panel nobody acts on."""
    for name, dashboard in dashboards.items():
        for panel in _panels(dashboard):
            if panel["type"] == "text" or panel["title"] in {"Spans/sec", "Traces"}:
                continue
            assert panel.get("description"), f"{name}: {panel['title']!r} has no description"


def test_dashboard_uids_are_unique(dashboards) -> None:
    uids = [d["uid"] for d in dashboards.values()]
    assert len(uids) == len(set(uids))


def test_panel_ids_are_unique_within_a_dashboard(dashboards) -> None:
    for name, dashboard in dashboards.items():
        ids = [p["id"] for p in _panels(dashboard)]
        assert len(ids) == len(set(ids)), f"{name}: duplicate panel ids"


# --- Time filtering (regression guard) ----------------------------------------


def test_queries_use_the_plugin_time_macro_not_grafana_globals(dashboards) -> None:
    """Regression guard.

    The dashboards originally used Grafana's ${__from} / ${__to}, which the
    FRONTEND interpolates. Every panel rendered correctly in a browser and every
    query passed direct SQL verification, while the same queries failed through
    /api/ds/query and would fail in alert rules, with a literal "${__from}"
    reaching ClickHouse. $__timeFilter is expanded by the plugin backend and
    works in both paths.
    """
    for name, dashboard in dashboards.items():
        for panel, sql in _queries(dashboard):
            assert "${__from}" not in sql and "${__to}" not in sql, (
                f"{name}: {panel['title']!r} uses frontend-only time interpolation"
            )


def test_time_filtered_panels_reference_a_real_time_column(dashboards) -> None:
    valid_columns = {"ts", "ts_minute", "ts_hour", "sampled_at"}
    macro = re.compile(r"\$__timeFilter\(\s*(\w+)\s*\)")
    for name, dashboard in dashboards.items():
        for panel, sql in _queries(dashboard):
            for column in macro.findall(sql):
                assert column in valid_columns, f"{name}: {panel['title']} filters on {column}"


# --- Reading the right tier ---------------------------------------------------


def test_workload_panels_read_rollups_not_raw() -> None:
    """Dashboards must not scan spans_raw; that is what the rollups are for.

    The one exception is ingest latency, which is a property of individual
    spans and has no meaning after aggregation.
    """
    overview = build_all()["llm-overview.json"]
    for panel, sql in _queries(overview):
        assert "spans_raw" not in sql, f"{panel['title']!r} scans raw telemetry"


def test_unregistered_values_panel_must_read_raw() -> None:
    """The converse: naming the offender is only possible before bucketing.

    By the time a value reaches spans_1m the guard has replaced it with a
    sentinel, so the information needed to act on it is gone by design.
    """
    cardinality = build_cardinality()
    panel = next(p for p in cardinality["panels"] if p["title"] == "Which values are unregistered?")
    sql = panel["targets"][0]["rawSql"]
    assert "spans_raw" in sql
    assert "dictHas" in sql


# --- The cardinality dashboard's core constraint ------------------------------


def test_other_and_none_are_never_summed() -> None:
    """The whole point.

    A query that adds the two buckets, or filters on `IN ('__other__',
    '__none__')` for a total, reproduces exactly the failure this dashboard
    exists to prevent: 18,243 routine spans hiding 300 actionable ones.
    """
    cardinality = build_cardinality()
    for panel, sql in _queries(cardinality):
        normalized = re.sub(r"\s+", " ", sql)
        # Adding the two sentinels together in one expression.
        assert not re.search(r"__other__'\s*\)\s*\+\s*count\w*If\([^)]*'__none__", normalized), (
            f"{panel['title']!r} sums __other__ and __none__"
        )
        # Or lumping them into one membership test used as a total. `NOT IN
        # ('__other__', '__none__')` is the opposite and is fine: it means
        # "registered", which is precisely the separation being enforced.
        assert not re.search(
            r"(?<!NOT )IN\s*\(\s*'__other__'\s*,\s*'__none__'\s*\)\s*\)\s*AS", normalized
        ), f"{panel['title']!r} treats the two sentinels as one bucket"


def test_unregistered_panel_is_an_absolute_count_with_a_zero_threshold() -> None:
    """A share would round an actionable signal to nothing.

    Measured: a 25-span shadow deployment was 0.02% of traffic. As a percentage
    it is invisible; as a count with a red threshold at 1, the panel goes red.
    """
    cardinality = build_cardinality()
    panel = next(p for p in cardinality["panels"] if p["title"] == "UNREGISTERED spans (__other__)")
    sql = panel["targets"][0]["rawSql"]

    assert "/" not in sql, "unregistered count must not be expressed as a ratio"
    steps = panel["fieldConfig"]["defaults"]["thresholds"]["steps"]
    red = [s for s in steps if s["color"] == "red"]
    assert red, "the unregistered panel must have a red threshold"
    assert red[0]["value"] == 1, "any unregistered span at all must colour the panel"


def test_none_panel_is_not_alarming() -> None:
    """Routine data must not cry wolf, or the red panel stops meaning anything."""
    cardinality = build_cardinality()
    panel = next(p for p in cardinality["panels"] if "__none__" in p["title"])
    thresholds = panel["fieldConfig"]["defaults"].get("thresholds", {})
    steps = thresholds.get("steps", [])
    assert not [s for s in steps if s["color"] == "red"]


def test_bucket_breakdown_keeps_three_separate_columns() -> None:
    cardinality = build_cardinality()
    panel = next(p for p in cardinality["panels"] if p["title"] == "Bucket breakdown by dimension")
    sql = panel["targets"][0]["rawSql"]
    assert '"registered"' in sql
    assert '"UNREGISTERED"' in sql
    assert '"absent (routine)"' in sql


def test_unregistered_timeseries_excludes_the_routine_bucket() -> None:
    """The series must not include __none__, or the routine bucket dwarfs it."""
    cardinality = build_cardinality()
    panel = next(
        p for p in cardinality["panels"] if p["title"].startswith("Unregistered spans over time")
    )
    sql = panel["targets"][0]["rawSql"]
    assert "__other__" in sql
    assert "__none__" not in sql


def test_cardinality_dashboard_explains_the_distinction() -> None:
    """Someone reading this at 3am should not have to infer the difference."""
    cardinality = build_cardinality()
    text = [p for p in cardinality["panels"] if p["type"] == "text"]
    assert text, "the cardinality dashboard needs a legend"
    content = text[0]["options"]["content"]
    assert "__other__" in content and "__none__" in content
    assert "actionable" in content.lower()


# --- Committed output matches the builder -------------------------------------


def test_generated_files_are_up_to_date(dashboards) -> None:
    """Committed JSON is what Grafana provisions; the builder is what we edit.

    If they drift, the dashboard in the running stack is not the one in review.
    """
    for filename, expected in dashboards.items():
        path = DASHBOARD_DIR / filename
        assert path.is_file(), f"{filename} has not been generated"
        on_disk = json.loads(path.read_text(encoding="utf-8"))
        assert on_disk == expected, f"{filename} is stale; run `telemetry-engine dashboards`"
