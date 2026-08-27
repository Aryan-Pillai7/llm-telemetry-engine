#!/usr/bin/env python3
"""One command from an empty machine to a populated, queryable pipeline.

    python tasks.py demo

Starts the stack, generates telemetry, waits for it to land, refreshes the
cardinality allowlist from real traffic, exports the cold tier, and prints what
to look at. Every step reports what it actually observed rather than announcing
success, because a demo that prints "done" without a row count is the same
category of thing this project spent eight phases removing.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from telemetry_engine.coldtier.query import duplication, stats  # noqa: E402
from telemetry_engine.config import Settings  # noqa: E402
from telemetry_engine.ingest.lag import read_lag  # noqa: E402
from telemetry_engine.storage.client import client  # noqa: E402

LOAD_SECONDS = 60
LOAD_RATE = 3_000
SETTLE_SECONDS = 25


def run(*args: str, check: bool = True) -> int:
    printable = " ".join(args[-4:])
    print(f"    $ ...{printable}", flush=True)
    result = subprocess.run(
        [sys.executable, "-m", "telemetry_engine.cli", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 and check:
        print(result.stdout[-2000:])
        print(result.stderr[-2000:], file=sys.stderr)
        raise SystemExit(f"step failed: {' '.join(args)}")
    return result.returncode


def step(number: int, total: int, title: str) -> None:
    print(f"\n[{number}/{total}] {title}")


def main() -> int:
    settings = Settings()
    total = 6

    print("=" * 72)
    print("  LLM & Agent Telemetry Analytics Engine - demo")
    print("=" * 72)

    step(1, total, "Starting the stack (Redpanda, ClickHouse, OTel Collector, Grafana)")
    # `up` also reconciles topics, applies migrations and syncs the allowlist.
    if subprocess.run([sys.executable, "tasks.py", "up"], cwd=REPO_ROOT).returncode != 0:
        raise SystemExit("stack failed to start; try `python tasks.py logs`")

    step(2, total, f"Generating {LOAD_SECONDS}s of telemetry at ~{LOAD_RATE:,} spans/s")
    print("    (zipfian tenants, long-tailed tokens, latency correlated with KV-cache)")
    run("load", "--duration", str(LOAD_SECONDS), "--rate", str(LOAD_RATE), "--workers", "4")

    step(3, total, f"Waiting {SETTLE_SECONDS}s for ingest to settle")
    print("    SDK batches ~1s, collector batches, ClickHouse flushes Kafka blocks every 3s")
    time.sleep(SETTLE_SECONDS)

    step(4, total, "Refreshing the cardinality allowlist from observed traffic")
    print("    top-K tenants become individually attributed; the tail collapses to __other__")
    run("dimensions", "apply")

    step(5, total, "Exporting the cold tier to Parquet")
    # 10s rather than the 5-minute default: everything just generated is
    # seconds old and the safe default would export nothing.
    run("cold", "export", "--lag-margin-seconds", "10")

    step(6, total, "Reading back what actually landed")

    with client(settings.clickhouse) as conn:
        raw = conn.query("""
            SELECT count(), uniqExact(tenant_id), uniqExact(trace_id),
                   round(quantile(0.95)(ttft_ms), 1)
            FROM telemetry.spans_raw
        """).result_rows[0]
        rollup = conn.query("""
            SELECT countMerge(spans), count()
            FROM telemetry.spans_1m
        """).result_rows[0]
        errors = conn.query("SELECT count() FROM telemetry.spans_ingest_errors").result_rows[0][0]

    lag = read_lag(settings.redpanda)
    lake = stats(settings.coldtier.root)
    dupes = duplication(settings.coldtier.root)

    print()
    print("=" * 72)
    print("  RESULT")
    print("=" * 72)
    print(f"  hot tier (spans_raw)   {raw[0]:>12,} spans")
    print(f"    tenants / traces     {raw[1]:>12,} / {raw[2]:,}")
    print(f"    TTFT p95             {raw[3]:>12} ms")
    print(f"  rollup (spans_1m)      {rollup[0]:>12,} spans in {rollup[1]:,} rows")
    if rollup[1]:
        print(f"    reduction            {raw[0] / rollup[1]:>12.0f}x fewer rows to scan")
    print(
        f"  cold tier (Parquet)    {lake.rows:>12,} rows in {lake.files} file(s), "
        f"{lake.mib:.1f} MiB"
    )
    if dupes.rows:
        print(
            f"    at-least-once dupes  {dupes.duplicates:>12,} "
            f"({dupes.duplicate_share:.1%}) - see spans_deduped"
        )
    print(f"  consumer lag           {lag.total_lag:>12,} messages")
    print(f"  dead-lettered messages {errors:>12,}")

    print()
    print("  LOOK AT")
    print("    Grafana        http://localhost:3000  (no login)")
    print("      - LLM Telemetry - Overview          throughput, TTFT, KV-cache, tenants")
    print("      - LLM Telemetry - Pipeline Health   lag, drops, ingest latency")
    print("      - LLM Telemetry - Cardinality Guard is anything unregistered emitting?")
    print()
    print("  TRY")
    print("    telemetry-engine cold query \\")
    print(
        '      "SELECT tenant_id, count(*), sum(output_tokens)'
        ' FROM spans GROUP BY 1 ORDER BY 3 DESC LIMIT 5"'
    )
    print()
    print("    telemetry-engine dimensions status     # cardinality vs budget")
    print("    telemetry-engine cold status           # lake coverage vs the TTL")
    print("    telemetry-engine backpressure          # the measured experiment (~6 min)")
    print()
    print("  STOP")
    print("    python tasks.py down                   # keep the data")
    print("    python tasks.py nuke                   # delete everything")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
