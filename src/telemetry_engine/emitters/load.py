"""High-rate load driver.

Produces span volume directly, without going through HTTP. That is a deliberate
split from `endpoint.py`:

  - `endpoint.py` is a real instrumented service. It shows what an inference
    endpoint's instrumentation looks like, and it is what you point a client at.
    It cannot reach 5k spans/s from Python -- HTTP request overhead dominates.
  - `load.py` synthesizes the same spans the endpoint would emit, at rate. It is
    what the backpressure experiment runs against.

Both build spans from the same `WorkloadGenerator`, so the telemetry is
identical in shape; only the transport differs.

Achieved rate is reported alongside the target. A load generator that quietly
fails to reach its target turns a backpressure measurement into fiction, so the
shortfall is always printed.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import time
from dataclasses import dataclass, field

from telemetry_engine.common.logging import configure_logging, get_logger
from telemetry_engine.emitters.otlp import ExporterConfig, build_tracer_provider, emit_trace
from telemetry_engine.emitters.workload import LoadProfile, Profile, RateLimiter, WorkloadGenerator

log = get_logger(__name__)


@dataclass
class LoadResult:
    """What a load run actually achieved."""

    spans: int = 0
    traces: int = 0
    # Time spent generating, excluding the shutdown flush. Rates are computed
    # against this: counting flush time would understate the achieved rate and
    # make the generator look like the pipeline is throttling it.
    elapsed_s: float = 0.0
    # Wall-clock for the whole run, including the final flush. Reported
    # separately so a long flush is visible rather than silently folded in.
    wall_elapsed_s: float = 0.0
    target_spans: int = 0
    workers: int = 1
    per_worker_spans: list[int] = field(default_factory=list)

    @property
    def achieved_rate(self) -> float:
        return self.spans / self.elapsed_s if self.elapsed_s > 0 else 0.0

    @property
    def target_rate(self) -> float:
        return self.target_spans / self.elapsed_s if self.elapsed_s > 0 else 0.0

    @property
    def flush_s(self) -> float:
        return max(0.0, self.wall_elapsed_s - self.elapsed_s)

    @property
    def shortfall_pct(self) -> float:
        """How far the generator fell short of its own target.

        This measures the *generator*, not the pipeline. Spans the pipeline
        dropped still count as generated -- that is the collector's counter to
        report, not ours.
        """
        if self.target_spans <= 0:
            return 0.0
        return max(0.0, (1.0 - self.spans / self.target_spans) * 100.0)


def _run_worker(
    *,
    worker_id: int,
    duration_s: float,
    profile: LoadProfile,
    exporter: ExporterConfig,
    tenants: int,
    zipf_alpha: float,
    seed: int,
    rate_share: float,
    result_queue: mp.Queue | None = None,
) -> tuple[int, int, int, float]:
    """One worker process. Returns (spans, traces, target_spans, generation_s)."""
    provider = build_tracer_provider(exporter)
    tracer = provider.get_tracer("telemetry_engine.load")

    # Distinct seed per worker, or every worker replays the same tenant sequence
    # and the aggregate skew is wrong.
    generator = WorkloadGenerator(tenants=tenants, zipf_alpha=zipf_alpha, seed=seed + worker_id)
    limiter = RateLimiter()

    spans = 0
    traces = 0
    target_accumulator = 0.0
    started = time.monotonic()
    last_report = started

    try:
        while True:
            elapsed = time.monotonic() - started
            if elapsed >= duration_s:
                break

            rate = profile.target_rate(elapsed) * rate_share
            allowance = limiter.acquire(rate)
            if allowance <= 0:
                # Sub-millisecond sleep keeps the loop from spinning a core
                # while still being fine-grained enough for 20k spans/s.
                time.sleep(0.0005)
                continue

            emitted = 0
            while emitted < allowance:
                trace_data = generator.generate_trace()
                emitted += emit_trace(tracer, trace_data)
                traces += 1
            spans += emitted

            now = time.monotonic()
            if worker_id == 0 and now - last_report >= 10.0:
                log.info(
                    "load_progress",
                    elapsed_s=round(elapsed, 1),
                    target_rate=round(rate, 0),
                    spans=spans,
                    achieved_rate=round(spans / elapsed, 0) if elapsed else 0,
                )
                last_report = now
        generation_s = time.monotonic() - started
        # Integrated once, at the end. Doing it per batch costs time that grows
        # with the length of the run -- measurement overhead that distorts the
        # very number being measured.
        target_accumulator = _integrate_target(profile, generation_s, rate_share)
    finally:
        # Flush what is still queued in the BatchSpanProcessor, then shut down.
        # Without this the tail of every run is silently discarded.
        provider.shutdown()

    total_target = int(target_accumulator)
    if result_queue is not None:
        result_queue.put((spans, traces, total_target, generation_s))
    return spans, traces, total_target, generation_s


def _integrate_target(profile: LoadProfile, elapsed_s: float, rate_share: float) -> float:
    """Approximate the spans a worker *should* have produced by `elapsed_s`.

    Integrated numerically because the burst and ramp profiles are not constant.
    One-second steps are plenty: the profiles change on the order of seconds.
    """
    total = 0.0
    step = 1.0
    t = 0.0
    while t < elapsed_s:
        span = min(step, elapsed_s - t)
        total += profile.target_rate(t) * rate_share * span
        t += step
    return total


def run_load(
    *,
    duration_s: float,
    profile: LoadProfile,
    exporter: ExporterConfig,
    tenants: int = 50,
    zipf_alpha: float = 1.2,
    seed: int = 1234,
    workers: int | None = None,
) -> LoadResult:
    """Drive load for `duration_s`, across `workers` processes.

    Multiple processes because a single CPython process cannot build and
    serialize 5k spans/s alongside the exporter's own work -- the GIL makes the
    generator the bottleneck well before the pipeline is stressed.
    """
    if workers is None:
        # Leave headroom: the whole docker stack is running on this machine and
        # starving ClickHouse would measure the wrong thing entirely.
        workers = max(1, min(4, (os.cpu_count() or 4) // 3))

    started = time.monotonic()

    if workers == 1:
        spans, traces, target, generation_s = _run_worker(
            worker_id=0,
            duration_s=duration_s,
            profile=profile,
            exporter=exporter,
            tenants=tenants,
            zipf_alpha=zipf_alpha,
            seed=seed,
            rate_share=1.0,
        )
        wall = time.monotonic() - started
        return LoadResult(
            spans=spans,
            traces=traces,
            elapsed_s=generation_s,
            wall_elapsed_s=wall,
            target_spans=target,
            workers=1,
            per_worker_spans=[spans],
        )

    ctx = mp.get_context("spawn")
    queue: mp.Queue = ctx.Queue()
    procs = [
        ctx.Process(
            target=_run_worker,
            kwargs={
                "worker_id": i,
                "duration_s": duration_s,
                "profile": profile,
                "exporter": exporter,
                "tenants": tenants,
                "zipf_alpha": zipf_alpha,
                "seed": seed,
                "rate_share": 1.0 / workers,
                "result_queue": queue,
            },
            daemon=True,
        )
        for i in range(workers)
    ]
    for p in procs:
        p.start()

    collected: list[tuple[int, int, int, float]] = []
    for _ in procs:
        # Generous timeout: workers flush on shutdown, which can take a moment
        # when the collector is backed up.
        collected.append(queue.get(timeout=duration_s + 120.0))
    for p in procs:
        p.join(timeout=60.0)

    wall = time.monotonic() - started
    return LoadResult(
        spans=sum(c[0] for c in collected),
        traces=sum(c[1] for c in collected),
        # Workers run concurrently, so the generation window is the longest
        # worker's, not the sum.
        elapsed_s=max((c[3] for c in collected), default=0.0),
        wall_elapsed_s=wall,
        target_spans=sum(c[2] for c in collected),
        workers=workers,
        per_worker_spans=[c[0] for c in collected],
    )


def main() -> None:  # pragma: no cover - convenience entrypoint
    configure_logging()
    result = run_load(
        duration_s=30.0,
        profile=LoadProfile(
            profile=Profile.STEADY, sustained_spans_per_sec=5000, burst_spans_per_sec=20000
        ),
        exporter=ExporterConfig(),
    )
    log.info(
        "load_complete",
        **{
            "spans": result.spans,
            "achieved_rate": round(result.achieved_rate, 1),
        },
    )


if __name__ == "__main__":  # pragma: no cover
    main()
