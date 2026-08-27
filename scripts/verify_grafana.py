#!/usr/bin/env python3
"""Execute every dashboard panel through Grafana's own query API.

`verify_dashboards.py` runs the SQL directly against ClickHouse, which proves
the SQL is valid. It does not prove the panel works: the datasource wiring, the
`format` enum, and the plugin's time macros all sit between Grafana and
ClickHouse, and each can fail while the SQL is perfectly fine.

That gap is not hypothetical. The dashboards originally used Grafana's global
$__from / $__to, which the frontend interpolates: every query passed direct SQL
verification and every panel would have rendered in a browser, while alerting
and any API-driven use failed with a raw "${__from}" reaching ClickHouse. Only
querying through Grafana surfaced it.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from telemetry_engine.dashboards import build_all  # noqa: E402

GRAFANA = "http://localhost:3000"


def query(targets: list[dict], window_hours: int = 24) -> dict:
    now_ms = int(time.time() * 1000)
    body = {
        "from": str(now_ms - window_hours * 3600 * 1000),
        "to": str(now_ms),
        "queries": [dict(t, intervalMs=60_000, maxDataPoints=500) for t in targets],
    }
    request = urllib.request.Request(
        f"{GRAFANA}/api/ds/query",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        return json.loads(exc.read().decode() or "{}")


def main() -> int:
    failures: list[tuple[str, str, str]] = []
    checked = 0

    for filename, dashboard in build_all().items():
        print(f"\n=== {filename}: {dashboard['title']}")
        for panel in dashboard["panels"]:
            targets = panel.get("targets")
            if not targets:
                continue
            checked += 1
            title = panel.get("title", "(untitled)")
            result = query(targets).get("results", {}).get("A", {})

            if "error" in result:
                message = str(result["error"])[:140]
                failures.append((filename, title, message))
                print(f"  FAIL  {title}: {message}")
                continue

            frames = result.get("frames", [])
            values = frames[0]["data"]["values"] if frames and frames[0].get("data") else []
            rows = len(values[0]) if values else 0
            fields = [f["name"] for f in frames[0]["schema"]["fields"]] if frames else []
            status = "ok   " if rows else "empty"
            print(f"  {status} {title}  rows={rows} fields={fields[:5]}")

    print(f"\nchecked {checked} panels through Grafana: {len(failures)} failed")
    if failures:
        print("\nFAILURES:")
        for name, title, message in failures:
            print(f"  {name}: {title}\n    {message}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
