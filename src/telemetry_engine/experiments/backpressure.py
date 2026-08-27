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

1.  The generator never reaches its target -- or reaches it only on paper. Lag
    stays flat and the pipeline looks like it kept up, when nothing ever
    stressed it. Checked at two layers, because the first version checked the
    wrong one: `generator_hit_target` counts spans CREATED, and a run passed it
    at 94.8% while the SDK discarded most of them before the collector saw
    anything. `load_actually_delivered` counts spans the collector ACCEPTED,
    which is the load the pipeline actually experienced.
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
8.  The lag reader perturbs the consumer group it measures.
    -> `lag_reader_is_noninvasive`, measured across the baseline only, since
    the deliberate stall causes rebalances of its own.

A run that fails any check reports INVALID and its numbers are not to be quoted.
That is not decoration: the first full run failed two of them and the numbers it
produced were indeed wrong. See `docs/backpressure.md`.

ONE CLOCK. Every timestamp in this module comes from `time.perf_counter()`.
Mixing it with `time.monotonic()` was an actual bug here: the probe thread was
switched to perf_counter for resolution while the run origin stayed on
monotonic, so every probe timestamp was the difference between two unrelated
epochs. Phase windowing then matched nothing, the endpoint check reported zero
probes, and -- more quietly -- the recovery time was computed against the same
broken axis. The two clocks have no defined relationship; they must never be
subtracted from each other.
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
from telemetry_engine.ingest.lag import read_collector, read_lag
from telemetry_engine.storage.client import client

log = get_logger(__name__)

# Lag below this counts as "caught up". Not zero: a healthy pipeline at 5k
# spans/s always has a little in flight.
RECOVERED_LAG = 500

# Pre-registered thresholds. Fixed before the first run so the experiment cannot
# be graded against whatever it happened to produce.
MIN_GENERATOR_ACHIEVEMENT = 0.90  # of its own target
# Fraction of created spans that must actually reach the collector. Calibrated:
# the generator delivers ~100% up to about 12.4k spans/s on this hardware and
# starts shedding in its own SDK queue above that. See
# scripts/calibrate_generator.py.
MIN_DELIVERY_EFFICIENCY = 0.95
MIN_PEAK_LAG_TO_BE_MEANINGFUL = 5_000  # messages
MAX_UNACCOUNTED_SPAN_FRACTION = 0.02  # of spans emitted
MIN_ENDPOINT_PROBES_PER_PHASE = 10

# A run must start from a quiesced pipeline. Without this, a previous run's
# recovery bleeds into the next run's baseline: rebalances from an earlier
# unpause were still arriving during a later baseline, which made the
# lag_reader check fire for a condition the lag reader had not caused.
QUIESCE_STABLE_S = 20.0
QUIESCE_TIMEOUT_S = 300.0


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
    collector_refused: int = 0
    collector_sent: int = 0
    collector_dropped: int = 0
    clickhouse_rows: int = 0

    rebalances_before: int = 0
    rebalances_after_baseline: int = 0
    rebalances_after: int = 0
    stall_released_at: float | None = None

    # Observed phase boundaries, in run-relative seconds. Recorded rather than
    # derived from the configured durations: a phase always takes slightly
    # longer than requested (process spawn, SDK flush), and windowing on the
    # nominal duration silently misattributes samples near the edges.
    baseline_started_at: float = 0.0
    baseline_ended_at: float = 0.0
    burst_started_at: float = 0.0
    burst_ended_at: float = 0.0

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
        """Spans that never reached the collector AND were not refused by it.

        This is genuinely generator-side loss: the SDK's own export queue
        overflowing. Refusals are subtracted out because they are a different
        thing with a different owner -- the collector deliberately shedding at
        the receiver under memory pressure (ADR-003 tier 2).

        Conflating the two was a real mis-attribution here: a run reported
        111,212 spans "lost in the SDK" when the collector had in fact refused
        them and said so in its logs. Same number, wrong owner, and it would
        have gone into the write-up as a generator weakness rather than as the
        memory limiter doing its job.
        """
        return max(0, self.emitted_total - self.collector_accepted - self.collector_refused)

    @property
    def unaccounted(self) -> int:
        """Spans that vanished without a stage claiming them."""
        return max(0, self.collector_sent - self.clickhouse_rows)

    def burst_window(self) -> tuple[float, float]:
        """Observed burst window, falling back to nominal if unrecorded."""
        if self.burst_ended_at > self.burst_started_at:
            return (self.burst_started_at, self.burst_ended_at)
        return (self.baseline_s, self.baseline_s + self.burst_s)

    def baseline_window(self) -> tuple[float, float]:
        if self.baseline_ended_at > self.baseline_started_at:
            return (self.baseline_started_at, self.baseline_ended_at)
        return (0.0, self.baseline_s)

    def recovery_seconds(self) -> float | None:
        """Time until lag falls below RECOVERED_LAG after pressure is removed.

        Measured strictly after the burst window closes, and after the stall is
        released when one was applied (failure mode 6): lag falling while load
        is still running, or while the consumer is still frozen, says nothing
        about recovery. Returns None if it never recovered in the window.
        """
        _, burst_end = self.burst_window()
        origin = max(burst_end, self.stall_released_at or 0.0)
        for sample in self.samples:
            if sample.t >= origin and sample.total_lag <= RECOVERED_LAG:
                return sample.t - origin
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
            "baseline_latency": self.probe_latency(*self.baseline_window()),
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
                # perf_counter, not monotonic: on Windows monotonic has ~15.6ms
                # granularity, which quantised the first run's endpoint latencies
                # to 0/15/16/31ms and made "p50 = 0.0ms" a timer artefact rather
                # than a measurement.
                started = time.perf_counter()
                try:
                    response = client_.post(self.url, json={"max_tokens": 256})
                    latency_ms = (time.perf_counter() - started) * 1000.0
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
                            latency_ms=(time.perf_counter() - started) * 1000.0,
                            ok=False,
                        )
                    )
                elapsed = time.perf_counter() - started
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
            started = time.perf_counter()
            try:
                lag = read_lag(self.settings.redpanda)
                collector = read_collector()
                if collector is None:
                    # Record nothing rather than zeros: a fabricated sample reads
                    # as a counter reset and invalidates the whole run.
                    log.warning("sample_skipped", reason="collector scrape failed")
                    self._stop.wait(max(0.0, self.interval))
                    continue
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
            elapsed = time.perf_counter() - started
            self._stop.wait(max(0.0, self.interval - elapsed))

    def stop(self) -> list[Sample]:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=30.0)
        return self.samples


# --- Validity checks ----------------------------------------------------------


def wait_until_quiesced(
    settings: Settings,
    *,
    stable_s: float = QUIESCE_STABLE_S,
    timeout_s: float = QUIESCE_TIMEOUT_S,
) -> None:
    """Block until lag is drained and the consumer group has stopped moving.

    Two conditions, both necessary: lag below the recovered threshold (so the
    baseline is not measuring an earlier run's backlog), and no rebalance for
    `stable_s` (so the group has finished reacting to whatever happened last).
    """
    deadline = time.perf_counter() + timeout_s
    stable_since: float | None = None
    last_rebalances: int | None = None

    while time.perf_counter() < deadline:
        lag = read_lag(settings.redpanda).total_lag
        with client(settings.clickhouse) as conn:
            rebalances = _rebalance_count(conn)

        moved = last_rebalances is not None and rebalances != last_rebalances
        last_rebalances = rebalances

        if lag > RECOVERED_LAG or moved:
            stable_since = None
        elif stable_since is None:
            stable_since = time.perf_counter()
        elif time.perf_counter() - stable_since >= stable_s:
            log.info("pipeline_quiesced", lag=lag, rebalances=rebalances)
            return

        time.sleep(2.0)

    raise RuntimeError(
        f"pipeline did not quiesce within {timeout_s}s "
        f"(lag={read_lag(settings.redpanda).total_lag}); refusing to start a run "
        "whose baseline would be measuring the previous run"
    )


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

    # Refused spans reached the collector and were shed there deliberately, so
    # they count as delivered for the purpose of "did the load arrive?". What
    # this check is really asking is whether the GENERATOR got the load out.
    delivered = result.collector_accepted + result.collector_refused
    delivery_efficiency = delivered / result.emitted_total if result.emitted_total else 0.0
    checks.append(
        Check(
            name="load_actually_delivered",
            passed=delivery_efficiency >= MIN_DELIVERY_EFFICIENCY,
            detail=f"{delivery_efficiency:.1%} of created spans reached the collector "
            f"({delivered:,} of {result.emitted_total:,}; "
            f"{result.collector_refused:,} refused there under memory pressure)",
            would_hide="generator_hit_target counts spans CREATED. A run passed it at "
            "94.8% while the SDK queue discarded most of them, so the pipeline "
            "was loaded at a fraction of the claimed rate and the experiment was "
            "measuring the generator, not the pipeline.",
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
            f"collector_refused={result.collector_refused:,}, "
            f"collector_dropped={result.collector_dropped:,}",
            would_hide="Loss upstream of Redpanda leaves nothing to lag on, so lag reads "
            "zero while data is being discarded -- the pipeline looks healthy "
            "precisely because it is failing.",
        )
    )

    baseline_probes = result.probe_latency(*result.baseline_window())["n"]
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

    # Measured across the BASELINE only. Pausing ClickHouse stops its consumers
    # heartbeating, so the group rebalances by design; counting those against
    # the lag reader made an earlier run fail this check for the wrong reason --
    # a check that fires on the wrong cause is only marginally better than one
    # that never fires. The baseline window has the lag reader running with no
    # stall applied, which is the condition that actually isolates its effect.
    stall_rebalances = result.rebalances_after - result.rebalances_after_baseline
    checks.append(
        Check(
            name="lag_reader_is_noninvasive",
            passed=result.rebalances_after_baseline == result.rebalances_before,
            detail=f"rebalances during baseline: {result.rebalances_before} -> "
            f"{result.rebalances_after_baseline} "
            f"({stall_rebalances} more followed the deliberate stall, as expected)",
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
    burst_rate: int = 12_000,
    workers: int = 6,
    sample_interval_s: float = 1.0,
    endpoint_port: int = 8099,
    seed: int = 20260828,
    stall_consumer: bool = True,
    settle_s: float = 25.0,
) -> BackpressureResult:
    """Baseline -> load with the consumer stalled -> recovery.

    WHY THE CONSUMER IS STALLED. The original design assumed load alone could
    outrun ClickHouse. Calibration showed otherwise: this machine's generator
    delivers at most ~12.4k spans/s, and ClickHouse consumed every one of them
    with lag back at 0 after each run. Pushing harder does not create
    backpressure, it makes the *generator* the bottleneck and produces a number
    about the wrong component.

    So overload is induced the way ADR-003 actually describes it -- by stalling
    the consumer. `docker pause` freezes ClickHouse without dropping
    connections or triggering a rebalance, which is precisely the "ClickHouse
    cannot keep up" condition the design claims to survive: Redpanda absorbs,
    lag grows, the endpoint is untouched, and consumption resumes on unpause.

    The endpoint runs as a real subprocess and is probed over HTTP throughout,
    because the claim under test is specifically about what an observed service
    experiences.
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
        deadline = time.perf_counter() + 60.0
        ready = False
        while time.perf_counter() < deadline:
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

        # Refuse to start on an unsettled pipeline. See wait_until_quiesced.
        log.info("waiting_for_quiesce")
        wait_until_quiesced(settings)

        with client(settings.clickhouse) as conn:
            result.rebalances_before = _rebalance_count(conn)
            rows_before = int(
                conn.query("SELECT count() FROM telemetry.spans_raw").result_rows[0][0]
            )

        before = read_collector()
        if before is None:
            raise RuntimeError("collector metrics unreachable; cannot establish a baseline")

        # perf_counter throughout: see the module docstring on ONE CLOCK.
        t0 = time.perf_counter()
        sampler = HealthSampler(settings, interval_s=sample_interval_s)
        probe = EndpointProbe(endpoint_url, rate_per_sec=10.0)
        sampler.start(t0)
        probe.start(t0)

        log.info("phase_baseline", rate=sustained_rate, seconds=baseline_s)
        result.baseline_started_at = time.perf_counter() - t0
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

        result.baseline_ended_at = time.perf_counter() - t0

        # Rebalances observed across the baseline, before any stall. This is the
        # window that isolates whether the lag reader perturbs the group.
        with client(settings.clickhouse) as conn:
            result.rebalances_after_baseline = _rebalance_count(conn)

        if stall_consumer:
            # Freeze, do not kill: connections stay open and the consumer group
            # keeps its assignment, so this is a stall rather than a rebalance.
            log.info("stalling_clickhouse")
            subprocess.run(["docker", "pause", "te-clickhouse"], check=True, capture_output=True)

        log.info("phase_burst", rate=burst_rate, seconds=burst_s, stalled=stall_consumer)
        result.burst_started_at = time.perf_counter() - t0
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

        result.burst_ended_at = time.perf_counter() - t0

        if stall_consumer:
            log.info("resuming_clickhouse")
            subprocess.run(["docker", "unpause", "te-clickhouse"], check=True, capture_output=True)
            result.stall_released_at = time.perf_counter() - t0

        log.info("phase_recovery", seconds=recovery_s)
        time.sleep(recovery_s)

        result.samples = sampler.stop()
        result.probes = probe.stop()

        result.generator_spans = baseline.spans + burst.spans
        result.generator_target_spans = baseline.target_spans + burst.target_spans
        result.generator_achieved_rate = burst.achieved_rate
        result.endpoint_spans = sum(p.spans_emitted for p in result.probes)

        # The SDK flushes on shutdown and ClickHouse drains afterwards. Reading
        # the counters before that settles undercounts delivery -- the mistake
        # that made an early calibration report 1.8k spans/s delivered against a
        # true 12.4k.
        time.sleep(settle_s)

        after = read_collector()
        if after is None:
            raise RuntimeError("collector metrics unreachable at the end of the run")
        result.collector_refused = after.refused_spans - before.refused_spans
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
        if stall_consumer:
            # Never leave the stack frozen because a run failed midway.
            subprocess.run(["docker", "unpause", "te-clickhouse"], check=False, capture_output=True)
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
