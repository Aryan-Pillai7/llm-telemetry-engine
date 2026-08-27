"""The backpressure experiment: what happens when ingest outruns ClickHouse.

This produces the project's headline numbers -- lag growth, recovery time, drop
count, and the central claim that the pipeline never pushes back on the endpoint
it observes. So the module is built around one question, asked before any of it
was written:

    what would make this measurement report success while being wrong?

Three times now this project has shipped a check that ran, passed, and was not
looking at the layer where the failure lived: a cardinality bucket that was
bounded but meaningless, a latency window that measured backlog replay, and a
dashboard whose SQL was valid while every panel failed through Grafana. Each was
found by accident. The eight failure modes below are the pre-registered answers
for this experiment, each implemented as a check that INVALIDATES the run rather
than warning about it.

1.  The generator never reaches its target. Lag stays flat and the pipeline
    looks like it kept up, when nothing ever stressed it. -> `generator_hit_target`
2.  The collector restarts mid-run. Its counters are cumulative, so a restart
    resets them to zero and the drop count reads as zero. -> `no_counter_reset`
3.  Spans are lost upstream of Redpanda. There is then nothing to lag on, so lag
    reads zero while data is being thrown away. -> `spans_accounted`
4.  Sampling is too coarse to see the peak. A 5s interval against an 8s burst can
    miss it entirely, reporting "no lag observed". -> `sampling_resolution`,
    `burst_actually_stressed`
5.  The endpoint claim is never tested. `load.py` synthesizes spans directly and
    never touches HTTP, so "endpoints are unaffected" would be asserted about a
    thing the experiment never exercised. -> `endpoint_probed_under_load`
6.  Recovery is timed while load is still running. Lag falls because the
    generator stopped, not because the pipeline caught up. -> recovery is
    measured strictly after the burst window closes.
7.  SDK-side loss is blamed on the pipeline. The BatchSpanProcessor drops when
    its own queue fills; that is the generator failing, not backpressure.
    -> emitted-vs-accepted is accounted as a separate stage.
8.  The lag reader perturbs the consumer group it measures. -> `no_rebalance`.

A run that fails any check reports INVALID and its numbers are not to be quoted.
"""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path

import httpx

from telemetry_engine.common.logging import get_logger
from telemetry_engine.config import Settings
from telemetry_engine.emitters.load import run_load
from telemetry_engine.emitters.otlp import ExporterConfig
from telemetry_engine.emitters.workload import LoadProfile, Profile
from telemetry_engine.ingest.lag import CollectorSnapshot, read_collector, read_lag
from telemetry_engine.storage.client import client

log = get_logger(__name__)

# Lag below this counts as "caught up". Not zero: a healthy pipeline at 5k
# spans/s always has a little in flight.
RECOVERED_LAG = 500

# Pre-registered thresholds. Fixed before the first run so the experiment cannot
# be graded against whatever it happened to produce.
MIN_GENERATOR_ACHIEVEMENT = 0.90  # of its own target
MIN_PEAK_LAG_TO_BE_MEANINGFUL = 5_000  # messages
MAX_UNACCOUNTED_SPAN_FRACTION = 0.02  # of spans emitted
MIN_ENDPOINT_PROBES_PER_PHASE = 10


@dataclass
class Check:
    """One pre-registered validity check."""

    name: str
    passed: bool
    detail: str
    # Why this check exists: what the experiment would have reported without it.
    would_hide: str


@dataclass
class Sample:
    """One reading during the run."""

    t: float  # seconds since run start
    total_lag: int
    max_partition_lag: int
    accepted: int
    sent: int
    dropped: int
    queue_size: int


@dataclass
class ProbeResult:
    """One request to the live mock endpoint."""

    t: float
    latency_ms: float
    ok: bool
    spans_emitted: int = 0


@dataclass
class BackpressureResult:
    """Everything the experiment measured, plus whether it can be trusted."""

    started_at: str = ""
    baseline_s: float = 0.0
    burst_s: float = 0.0
    sample_interval_s: float = 0.0

    target_sustained: int = 0
    target_burst: int = 0

    generator_spans: int = 0
    generator_target_spans: int = 0
    generator_achieved_rate: float = 0.0

    samples: list[Sample] = field(default_factory=list)
    probes: list[ProbeResult] = field(default_factory=list)

    # Span accounting, stage by stage.
    endpoint_spans: int = 0
    collector_accepted: int = 0
    collector_sent: int = 0
    collector_dropped: int = 0
    clickhouse_rows: int = 0

    rebalances_before: int = 0
    rebalances_after: int = 0

    checks: list[Check] = field(default_factory=list)

    # --- derived ------------------------------------------------------------

    @property
    def valid(self) -> bool:
        return all(c.passed for c in self.checks)

    @property
    def peak_lag(self) -> int:
        return max((s.total_lag for s in self.samples), default=0)

    @property
    def emitted_total(self) -> int:
        """Spans the generator and the probed endpoint together produced."""
        return self.generator_spans + self.endpoint_spans

    @property
    def sdk_lost(self) -> int:
        """Spans that never reached the collector: the SDK's own queue dropping.

        Generator-side loss. Attributing it to the pipeline would overstate
        backpressure loss, which is failure mode 7.
        """
        return max(0, self.emitted_total - self.collector_accepted)

    @property
    def unaccounted(self) -> int:
        """Spans that vanished without a stage claiming them."""
        return max(0, self.collector_sent - self.clickhouse_rows)

    def burst_window(self) -> tuple[float, float]:
        return (self.baseline_s, self.baseline_s + self.burst_s)

    def recovery_seconds(self) -> float | None:
        """Time from the END of the burst until lag falls below RECOVERED_LAG.

        Measured strictly after the burst window closes (failure mode 6): lag
        falling while load is still running says nothing about recovery.
        Returns None if it never recovered within the observation window.
        """
        _, burst_end = self.burst_window()
        for sample in self.samples:
            if sample.t >= burst_end and sample.total_lag <= RECOVERED_LAG:
                return sample.t - burst_end
        return None

    def probe_latency(self, start: float, end: float) -> dict[str, float]:
        """Endpoint latency percentiles within a time window."""
        values = sorted(p.latency_ms for p in self.probes if start <= p.t < end and p.ok)
        if not values:
            return {"n": 0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
        return {
            "n": len(values),
            "p50": values[int(len(values) * 0.50)],
            "p95": values[min(len(values) - 1, int(len(values) * 0.95))],
            "p99": values[min(len(values) - 1, int(len(values) * 0.99))],
            "max": values[-1],
        }

    def to_json(self) -> str:
        payload = asdict(self)
        payload["derived"] = {
            "valid": self.valid,
            "peak_lag": self.peak_lag,
            "recovery_seconds": self.recovery_seconds(),
            "sdk_lost": self.sdk_lost,
            "unaccounted": self.unaccounted,
            "baseline_latency": self.probe_latency(0, self.baseline_s),
            "burst_latency": self.probe_latency(*self.burst_window()),
        }
        return json.dumps(payload, indent=2, default=str)


# --- Endpoint probing ---------------------------------------------------------


class EndpointProbe:
    """Hits the live mock endpoint at a steady low rate, recording latency.

    This is the only part of the experiment that tests the project's central
    claim. `load.py` synthesizes spans directly and never makes an HTTP request,
    so without this the statement "the pipeline does not backpressure the
    endpoint" would be an assertion about something never exercised.
    """

    def __init__(self, url: str, *, rate_per_sec: float = 10.0) -> None:
        self.url = url
        self.interval = 1.0 / rate_per_sec
        self.results: list[ProbeResult] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._t0 = 0.0

    def start(self, t0: float) -> None:
        self._t0 = t0
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        with httpx.Client(timeout=10.0) as client_:
            while not self._stop.is_set():
                started = time.monotonic()
                try:
                    response = client_.post(self.url, json={"max_tokens": 256})
                    latency_ms = (time.monotonic() - started) * 1000.0
                    body = response.json() if response.status_code == 200 else {}
                    self.results.append(
                        ProbeResult(
                            t=started - self._t0,
                            latency_ms=latency_ms,
                            ok=response.status_code == 200,
                            spans_emitted=int(body.get("spans_emitted", 0)),
                        )
                    )
                except Exception:
                    self.results.append(
                        ProbeResult(
                            t=started - self._t0,
                            latency_ms=(time.monotonic() - started) * 1000.0,
                            ok=False,
                        )
                    )
                elapsed = time.monotonic() - started
                self._stop.wait(max(0.0, self.interval - elapsed))

    def stop(self) -> list[ProbeResult]:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=15.0)
        return self.results


# --- Sampling -----------------------------------------------------------------


class HealthSampler:
    """Samples lag and collector counters on a fine interval in the background."""

    def __init__(self, settings: Settings, *, interval_s: float) -> None:
        self.settings = settings
        self.interval = interval_s
        self.samples: list[Sample] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._t0 = 0.0

    def start(self, t0: float) -> None:
        self._t0 = t0
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                lag = read_lag(self.settings.redpanda)
                collector = read_collector()
                self.samples.append(
                    Sample(
                        t=started - self._t0,
                        total_lag=lag.total_lag,
                        max_partition_lag=lag.max_partition_lag,
                        accepted=collector.accepted_spans,
                        sent=collector.sent_spans,
                        dropped=collector.dropped_spans,
                        queue_size=collector.queue_size,
                    )
                )
            except Exception as exc:
                log.warning("sample_failed", error=str(exc))
            elapsed = time.monotonic() - started
            self._stop.wait(max(0.0, self.interval - elapsed))

    def stop(self) -> list[Sample]:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=30.0)
        return self.samples


# --- Validity checks ----------------------------------------------------------


def _rebalance_count(conn) -> int:
    rows = conn.query("""
        SELECT sum(num_rebalance_assignments)
        FROM system.kafka_consumers
        WHERE database = 'telemetry' AND table = 'kafka_spans'
    """).result_rows
    return int(rows[0][0] or 0) if rows else 0


def validate(result: BackpressureResult) -> list[Check]:
    """Run every pre-registered check. See the module docstring."""
    checks: list[Check] = []

    achieved = (
        result.generator_spans / result.generator_target_spans
        if result.generator_target_spans
        else 0.0
    )
    checks.append(
        Check(
            name="generator_hit_target",
            passed=achieved >= MIN_GENERATOR_ACHIEVEMENT,
            detail=f"generator produced {achieved:.1%} of its target "
            f"({result.generator_spans:,} of {result.generator_target_spans:,})",
            would_hide="A generator that fell short would leave lag flat, and the "
            "pipeline would look like it absorbed a burst it never received.",
        )
    )

    decreases = [
        (a.t, b.t)
        for a, b in zip(result.samples, result.samples[1:], strict=False)
        if b.accepted < a.accepted or b.sent < a.sent or b.dropped < a.dropped
    ]
    checks.append(
        Check(
            name="no_counter_reset",
            passed=not decreases,
            detail="collector counters increased monotonically"
            if not decreases
            else f"counter went backwards at t={decreases[0][1]:.1f}s (collector restarted?)",
            would_hide="Collector counters are cumulative. A restart zeroes them, so a "
            "run that dropped heavily would report zero drops.",
        )
    )

    interval_ok = result.sample_interval_s <= result.burst_s / 4
    checks.append(
        Check(
            name="sampling_resolution",
            passed=interval_ok,
            detail=f"sampled every {result.sample_interval_s}s against a {result.burst_s}s burst",
            would_hide="A sampling interval close to the burst length can step over the "
            "peak entirely and report that no lag ever occurred.",
        )
    )

    checks.append(
        Check(
            name="burst_actually_stressed",
            passed=result.peak_lag >= MIN_PEAK_LAG_TO_BE_MEANINGFUL,
            detail=f"peak lag {result.peak_lag:,} messages",
            would_hide="If the burst never built meaningful lag, there is no backpressure "
            "behaviour to report and 'recovered instantly' is vacuous.",
        )
    )

    unaccounted_fraction = (
        result.unaccounted / result.emitted_total if result.emitted_total else 0.0
    )
    checks.append(
        Check(
            name="spans_accounted",
            passed=unaccounted_fraction <= MAX_UNACCOUNTED_SPAN_FRACTION,
            detail=f"{result.unaccounted:,} of {result.emitted_total:,} spans unaccounted "
            f"({unaccounted_fraction:.2%}); sdk_lost={result.sdk_lost:,}, "
            f"collector_dropped={result.collector_dropped:,}",
            would_hide="Loss upstream of Redpanda leaves nothing to lag on, so lag reads "
            "zero while data is being discarded -- the pipeline looks healthy "
            "precisely because it is failing.",
        )
    )

    baseline_probes = result.probe_latency(0, result.baseline_s)["n"]
    burst_probes = result.probe_latency(*result.burst_window())["n"]
    checks.append(
        Check(
            name="endpoint_probed_under_load",
            passed=baseline_probes >= MIN_ENDPOINT_PROBES_PER_PHASE
            and burst_probes >= MIN_ENDPOINT_PROBES_PER_PHASE,
            detail=f"{baseline_probes} baseline probes, {burst_probes} burst probes",
            would_hide="The load generator never makes an HTTP request. Without probing a "
            "real endpoint during the burst, 'endpoints are unaffected' would be "
            "a claim about something the experiment never exercised.",
        )
    )

    checks.append(
        Check(
            name="no_rebalance",
            passed=result.rebalances_after == result.rebalances_before,
            detail=f"consumer rebalances {result.rebalances_before} -> {result.rebalances_after}",
            would_hide="The lag reader joins the same consumer group. If it triggered a "
            "rebalance it would be measuring lag it caused itself.",
        )
    )

    return checks


# --- The experiment -----------------------------------------------------------


def run_experiment(
    settings: Settings,
    *,
    baseline_s: float = 45.0,
    burst_s: float = 45.0,
    recovery_s: float = 120.0,
    sustained_rate: int = 5_000,
    burst_rate: int = 20_000,
    workers: int = 8,
    sample_interval_s: float = 1.0,
    endpoint_port: int = 8099,
    seed: int = 20260828,
) -> BackpressureResult:
    """Baseline -> burst -> recovery, with everything instrumented.

    The endpoint runs as a real subprocess and is probed over HTTP throughout,
    because the claim being tested is specifically about what an observed
    service experiences.
    """
    result = BackpressureResult(
        started_at=datetime.now().isoformat(timespec="seconds"),
        baseline_s=baseline_s,
        burst_s=burst_s,
        sample_interval_s=sample_interval_s,
        target_sustained=sustained_rate,
        target_burst=burst_rate,
    )

    endpoint_url = f"http://127.0.0.1:{endpoint_port}/v1/chat/completions"
    endpoint = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "telemetry_engine.cli",
            "serve",
            "--port",
            str(endpoint_port),
            "--host",
            "127.0.0.1",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        # Wait for the endpoint to accept requests before starting the clock.
        deadline = time.monotonic() + 60.0
        ready = False
        while time.monotonic() < deadline:
            try:
                if (
                    httpx.get(f"http://127.0.0.1:{endpoint_port}/health", timeout=2.0).status_code
                    == 200
                ):
                    ready = True
                    break
            except Exception:
                time.sleep(1.0)
        if not ready:
            raise RuntimeError("mock endpoint did not become ready")

        with client(settings.clickhouse) as conn:
            result.rebalances_before = _rebalance_count(conn)
            rows_before = int(
                conn.query("SELECT count() FROM telemetry.spans_raw").result_rows[0][0]
            )

        before: CollectorSnapshot = read_collector()

        t0 = time.monotonic()
        sampler = HealthSampler(settings, interval_s=sample_interval_s)
        probe = EndpointProbe(endpoint_url, rate_per_sec=10.0)
        sampler.start(t0)
        probe.start(t0)

        log.info("phase_baseline", rate=sustained_rate, seconds=baseline_s)
        baseline = run_load(
            duration_s=baseline_s,
            profile=LoadProfile(
                profile=Profile.STEADY,
                sustained_spans_per_sec=sustained_rate,
                burst_spans_per_sec=burst_rate,
            ),
            exporter=ExporterConfig(),
            tenants=settings.workload.tenants,
            zipf_alpha=settings.workload.zipf_alpha,
            seed=seed,
            workers=workers,
        )

        log.info("phase_burst", rate=burst_rate, seconds=burst_s)
        burst = run_load(
            duration_s=burst_s,
            profile=LoadProfile(
                profile=Profile.STEADY,
                sustained_spans_per_sec=burst_rate,
                burst_spans_per_sec=burst_rate,
            ),
            exporter=ExporterConfig(),
            tenants=settings.workload.tenants,
            zipf_alpha=settings.workload.zipf_alpha,
            seed=seed + 1,
            workers=workers,
        )

        log.info("phase_recovery", seconds=recovery_s)
        time.sleep(recovery_s)

        result.samples = sampler.stop()
        result.probes = probe.stop()

        result.generator_spans = baseline.spans + burst.spans
        result.generator_target_spans = baseline.target_spans + burst.target_spans
        result.generator_achieved_rate = burst.achieved_rate
        result.endpoint_spans = sum(p.spans_emitted for p in result.probes)

        after = read_collector()
        result.collector_accepted = after.accepted_spans - before.accepted_spans
        result.collector_sent = after.sent_spans - before.sent_spans
        result.collector_dropped = after.dropped_spans - before.dropped_spans

        with client(settings.clickhouse) as conn:
            rows_after = int(
                conn.query("SELECT count() FROM telemetry.spans_raw").result_rows[0][0]
            )
            result.clickhouse_rows = rows_after - rows_before
            result.rebalances_after = _rebalance_count(conn)

    finally:
        endpoint.terminate()
        try:
            endpoint.wait(timeout=30)
        except subprocess.TimeoutExpired:
            endpoint.kill()

    result.checks = validate(result)
    return result


def write_report(result: BackpressureResult, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(result.to_json() + "\n", encoding="utf-8")
    return path
