#!/usr/bin/env python3
"""Run every dashboard panel's SQL against ClickHouse.

A dashboard whose JSON is valid but whose SQL is wrong renders as an empty
panel, which looks like "no data in this window" rather than like a bug. That
is the same class of failure as the __other__ bucket: technically fine,
practically blind. So every query gets executed.

Grafana's ${__from}/${__to} are substituted with a real range, since the point
is to prove the SQL parses and returns rows against actual data.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from telemetry_engine.dashboards import DASHBOARD_DIR  # noqa: E402
from telemetry_engine.storage.client import client  # noqa: E402

# A wide window so panels have data regardless of when the last load ran.
WINDOW_START = "(now() - INTERVAL 24 HOUR)"
WINDOW_END = "now()"

# Expands the plugin's $__timeFilter(col) the way the plugin backend does.
_TIME_FILTER = re.compile(r"\$__timeFilter\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)")


def substitute(sql: str) -> str:
    sql = _TIME_FILTER.sub(
        lambda m: f"{m.group(1)} >= {WINDOW_START} AND {m.group(1)} <= {WINDOW_END}", sql
    )
    sql = sql.replace("$__fromTime", WINDOW_START).replace("$__toTime", WINDOW_END)
    # Any remaining macro or variable is a builder mistake. Catching it here
    # matters: an unexpanded macro reaches ClickHouse verbatim and the panel
    # fails at render time, where it looks like "no data".
    leftover = re.findall(r"\$\{?__\w+\}?", sql)
    if leftover:
        raise ValueError(f"unsubstituted Grafana macros: {set(leftover)}")
    return sql


def main() -> int:
    failures: list[tuple[str, str, str]] = []
    checked = 0
    empty: list[tuple[str, str]] = []

    with client() as conn:
        for path in sorted(DASHBOARD_DIR.glob("*.json")):
            dashboard = json.loads(path.read_text(encoding="utf-8"))
            print(f"\n=== {path.name}: {dashboard['title']}")
            for panel in dashboard["panels"]:
                for target in panel.get("targets", []):
                    sql = target.get("rawSql")
                    if not sql:
                        continue
                    checked += 1
                    title = panel.get("title", "(untitled)")
                    try:
                        rows = conn.query(substitute(sql)).result_rows
                    except Exception as exc:
                        message = str(exc).split("\n")[0][:160]
                        failures.append((path.name, title, message))
                        print(f"  FAIL  {title}: {message}")
                        continue

                    if not rows or all(all(v is None for v in r) for r in rows):
                        empty.append((path.name, title))
                        print(f"  empty {title}")
                    else:
                        preview = str(rows[0])[:70]
                        print(f"  ok    {title}  -> {len(rows)} row(s) {preview}")

    print(f"\nchecked {checked} queries: {len(failures)} failed, {len(empty)} returned no rows")
    if empty:
        print("\nempty (may be legitimate if nothing matches in the window):")
        for name, title in empty:
            print(f"  {name}: {title}")
    if failures:
        print("\nFAILURES:")
        for name, title, message in failures:
            print(f"  {name}: {title}\n    {message}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
