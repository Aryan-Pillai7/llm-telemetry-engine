#!/usr/bin/env python3
"""Find the load the generator can actually DELIVER, not just create.

The backpressure experiment is only meaningful if the load it claims to apply
actually reaches the pipeline. Measured during Phase 6 setup: at a nominal 20k
spans/s the generator created 17k/s and delivered ~1.8k/s to the collector, the
rest dying in the SDK's own export queue. An experiment run at that setting
would have reported "the pipeline absorbed a 20k/s burst" while the pipeline
saw a fraction of it.

This sweeps target rate and worker count, and reports for each combination:

    generated   - spans the generator built
    delivered   - spans the collector accepted (the load the pipeline saw)
    efficiency  - delivered / generated

The usable burst rate is the highest one where efficiency stays high. Above
that, the generator is the thing under test.
"""

from __future__ import annotations

import contextlib
import subprocess
import sys
import threading
import time
from itertools import pairwise
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from telemetry_engine.config import Settings  # noqa: E402
from telemetry_engine.ingest.lag import read_collector, read_lag  # noqa: E402

DURATION = 20
SETTLE = 25  # let the SDK and collector drain before reading final counters


def measure(rate: int, workers: int) -> dict:
    settings = Settings()
    samples: list[tuple[float, int]] = []
    stop = threading.Event()

    def poll() -> None:
        while not stop.is_set():
            with contextlib.suppress(Exception):
                samples.append((time.perf_counter(), read_collector().accepted_spans))
            stop.wait(1.0)

    thread = threading.Thread(target=poll, daemon=True)
    thread.start()

    before = read_collector().accepted_spans
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "telemetry_engine.cli",
            "load",
            "--duration",
            str(DURATION),
            "--rate",
            str(rate),
            "--workers",
            str(workers),
        ],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    # The SDK flushes on shutdown; without settling, delivery is undercounted.
    time.sleep(SETTLE)
    after = read_collector().accepted_spans
    stop.set()
    thread.join(timeout=5)

    generated = 0
    for line in proc.stdout.splitlines():
        if line.strip().startswith("spans:"):
            generated = int(line.split(":")[1].strip().replace(",", ""))

    delivered = after - before
    lag = read_lag(settings.redpanda).total_lag
    peak_rate = 0.0
    for (t0, v0), (t1, v1) in pairwise(samples):
        if t1 > t0:
            peak_rate = max(peak_rate, (v1 - v0) / (t1 - t0))

    return {
        "rate": rate,
        "workers": workers,
        "generated": generated,
        "delivered": delivered,
        "efficiency": delivered / generated if generated else 0.0,
        "generated_rate": generated / DURATION,
        "delivered_rate": delivered / DURATION,
        "peak_delivery_rate": peak_rate,
        "lag_after": lag,
    }


def main() -> int:
    grid = [
        (5_000, 4),
        (8_000, 4),
        (8_000, 6),
        (12_000, 6),
        (16_000, 6),
        (16_000, 8),
    ]
    print(
        f"{'target':>8} {'wrk':>4} {'generated':>10} {'delivered':>10} "
        f"{'eff':>6} {'gen/s':>8} {'del/s':>8} {'peak/s':>8} {'lag':>8}"
    )
    results = []
    for rate, workers in grid:
        r = measure(rate, workers)
        results.append(r)
        print(
            f"{r['rate']:>8} {r['workers']:>4} {r['generated']:>10,} {r['delivered']:>10,} "
            f"{r['efficiency']:>6.0%} {r['generated_rate']:>8,.0f} {r['delivered_rate']:>8,.0f} "
            f"{r['peak_delivery_rate']:>8,.0f} {r['lag_after']:>8,}"
        )

    usable = [r for r in results if r["efficiency"] >= 0.95]
    print()
    if usable:
        best = max(usable, key=lambda r: r["delivered_rate"])
        print(
            f"Highest rate delivered at >=95% efficiency: {best['rate']:,}/s "
            f"with {best['workers']} workers ({best['delivered_rate']:,.0f} spans/s delivered)"
        )
    else:
        print("No configuration reached 95% delivery efficiency.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
