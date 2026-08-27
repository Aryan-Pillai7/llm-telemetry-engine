"""Synthetic LLM/agent workload generation.

This module produces *data*, never OpenTelemetry spans. `otlp.py` turns what
comes out of here into real spans. The separation is deliberate: tenant skew,
token/latency correlation, and rate control are the parts most likely to be
wrong, and keeping them free of the SDK means they can be tested exhaustively
without a collector, a broker, or a clock that actually advances.

The workload is intentionally *not* uniform. A pipeline benchmarked against
evenly distributed tenants and constant request sizes proves nothing about the
cardinality and backpressure problems this project is about:

  - tenant traffic is zipfian, so a handful of tenants dominate every window
  - prompt and completion lengths are long-tailed
  - latency is correlated with output length and KV-cache pressure, not random
  - errors cluster during high cache pressure, as they do on a real endpoint
"""

from __future__ import annotations

import math
import random
import time
from bisect import bisect_right
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from enum import StrEnum

from telemetry_engine.emitters import attributes as A

# --- Static workload vocabulary ----------------------------------------------

# Bounded by design: these are rollup dimensions, so their cardinality is part
# of the schema's cost, not an accident of the generator.
MODELS: tuple[str, ...] = (
    "llama-3.1-8b-instruct",
    "llama-3.1-70b-instruct",
    "mistral-7b-instruct",
    "qwen2.5-14b-instruct",
    "claude-haiku-proxy",
)

ROUTES: tuple[str, ...] = ("/v1/chat/completions", "/v1/completions", "/v1/embeddings")
REGIONS: tuple[str, ...] = ("us-east-1", "us-west-2", "eu-central-1")
TENANT_TIERS: tuple[str, ...] = ("free", "pro", "enterprise")
AGENT_NAMES: tuple[str, ...] = ("research-agent", "support-agent", "codegen-agent")
TOOL_NAMES: tuple[str, ...] = ("web_search", "sql_query", "vector_lookup", "code_exec")


class Operation(StrEnum):
    """GenAI operation names, per semantic convention."""

    CHAT = "chat"
    EMBEDDINGS = "embeddings"
    AGENT_INVOKE = "invoke_agent"
    TOOL = "execute_tool"


class StatusClass(StrEnum):
    """Coarse outcome, low-cardinality by construction."""

    OK = "ok"
    CLIENT_ERROR = "client_error"
    SERVER_ERROR = "server_error"
    TIMEOUT = "timeout"


@dataclass(frozen=True)
class ModelProfile:
    """Serving characteristics of one model.

    Larger models are slower per token and consume more KV cache per sequence.
    Encoding this rather than randomizing latency is what makes the resulting
    dashboards legible: p95 latency differs by model for a *reason*.
    """

    name: str
    ms_per_output_token: float
    base_ttft_ms: float
    kv_blocks_per_1k_tokens: float

    @property
    def is_embedding_model(self) -> bool:
        return "embed" in self.name


MODEL_PROFILES: dict[str, ModelProfile] = {
    "llama-3.1-8b-instruct": ModelProfile("llama-3.1-8b-instruct", 8.0, 90.0, 3.0),
    "llama-3.1-70b-instruct": ModelProfile("llama-3.1-70b-instruct", 28.0, 240.0, 9.0),
    "mistral-7b-instruct": ModelProfile("mistral-7b-instruct", 7.0, 80.0, 2.6),
    "qwen2.5-14b-instruct": ModelProfile("qwen2.5-14b-instruct", 12.0, 130.0, 4.5),
    "claude-haiku-proxy": ModelProfile("claude-haiku-proxy", 5.0, 160.0, 2.0),
}


# --- Tenant population --------------------------------------------------------


@dataclass
class TenantPool:
    """A population of tenants with zipfian request volume.

    Real multi-tenant traffic is dominated by a few large tenants. That skew is
    the whole reason per-tenant top-K and an `__other__` bucket are needed --
    with uniform tenants, any naive aggregation looks fine.
    """

    count: int
    alpha: float
    rng: random.Random

    ids: list[str] = field(init=False)
    tiers: dict[str, str] = field(init=False)
    _cumulative: list[float] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.count < 1:
            raise ValueError(f"tenant count must be >= 1, got {self.count}")
        if self.alpha <= 0:
            raise ValueError(f"zipf alpha must be > 0, got {self.alpha}")

        self.ids = [f"tenant-{i:04d}" for i in range(self.count)]

        # Bigger tenants get better tiers; tier is a rollup dimension and should
        # correlate with volume the way it would in a real customer base.
        self.tiers = {}
        for rank, tenant_id in enumerate(self.ids):
            if rank < max(1, self.count // 20):
                tier = "enterprise"
            elif rank < max(2, self.count // 4):
                tier = "pro"
            else:
                tier = "free"
            self.tiers[tenant_id] = tier

        # Zipf weights: w(rank) = 1 / rank^alpha. Precomputed as a cumulative
        # distribution so sampling is a binary search rather than an O(n) scan
        # -- this runs thousands of times per second.
        weights = [1.0 / ((rank + 1) ** self.alpha) for rank in range(self.count)]
        total = math.fsum(weights)
        acc = 0.0
        self._cumulative = []
        for w in weights:
            acc += w / total
            self._cumulative.append(acc)
        # Guard against float drift leaving the last bucket below 1.0.
        self._cumulative[-1] = 1.0

    def sample(self) -> str:
        """Draw one tenant, zipf-weighted."""
        return self.ids[bisect_right(self._cumulative, self.rng.random())]

    def share_of_top(self, n: int) -> float:
        """Fraction of traffic the top `n` tenants receive. Used in tests/docs."""
        if n <= 0:
            return 0.0
        return self._cumulative[min(n, self.count) - 1]


# --- Request shape ------------------------------------------------------------


@dataclass(frozen=True)
class SpanData:
    """One span, as plain data. No SDK types.

    `start_offset_ms` is relative to the trace's start, so a whole trace can be
    materialized at any wall-clock time -- including backdated, which is how the
    load generator produces realistic durations without actually waiting.
    """

    name: str
    kind: str  # "server" | "client" | "internal"
    start_offset_ms: float
    duration_ms: float
    attributes: dict[str, object]
    status: StatusClass
    children: tuple[SpanData, ...] = ()

    def walk(self) -> Iterator[SpanData]:
        """Yield this span and every descendant, parents first."""
        yield self
        for child in self.children:
            yield from child.walk()

    @property
    def span_count(self) -> int:
        return sum(1 for _ in self.walk())


@dataclass(frozen=True)
class TraceData:
    """A complete trace: one agent invocation and everything under it."""

    root: SpanData
    tenant_id: str

    @property
    def span_count(self) -> int:
        return self.root.span_count


def _sample_long_tail(rng: random.Random, median: float, sigma: float, cap: float) -> int:
    """Draw a long-tailed positive integer (log-normal).

    Prompt and completion lengths are not normally distributed: most are small,
    a few are enormous, and the big ones dominate cost. The cap stands in for a
    real endpoint's max token limit.
    """
    value = rng.lognormvariate(math.log(median), sigma)
    return int(max(1.0, min(value, cap)))


class WorkloadGenerator:
    """Produces traces that look like real LLM/agent traffic.

    Seeded for reproducibility: the same seed yields the same workload, which is
    what makes a backpressure measurement comparable across runs.
    """

    def __init__(
        self,
        *,
        tenants: int = 50,
        zipf_alpha: float = 1.2,
        seed: int | None = None,
        error_rate: float = 0.02,
    ) -> None:
        if not 0.0 <= error_rate < 1.0:
            raise ValueError(f"error_rate must be in [0, 1), got {error_rate}")
        self.rng = random.Random(seed)
        self.tenants = TenantPool(count=tenants, alpha=zipf_alpha, rng=self.rng)
        self.error_rate = error_rate
        # KV-cache occupancy drifts over time rather than being redrawn per
        # request: cache pressure is a property of the endpoint, and its
        # autocorrelation is what makes latency degrade in runs rather than
        # in isolated spikes.
        self._kv_utilization = 0.45

    # -- endpoint state --------------------------------------------------------

    def _advance_kv_cache(self) -> float:
        """Random-walk the KV-cache occupancy within [0.05, 0.99]."""
        drift = self.rng.gauss(0.0, 0.03)
        # Mean-reverting: pull gently back toward 0.55 so it neither pins at
        # 1.0 nor decays to empty over a long run.
        self._kv_utilization += drift + (0.55 - self._kv_utilization) * 0.02
        self._kv_utilization = min(0.99, max(0.05, self._kv_utilization))
        return self._kv_utilization

    def _status_for(self, kv_utilization: float) -> StatusClass:
        """Errors cluster under cache pressure, as on a real endpoint."""
        pressure_multiplier = 1.0 + 6.0 * max(0.0, kv_utilization - 0.85) / 0.15
        if self.rng.random() >= self.error_rate * pressure_multiplier:
            return StatusClass.OK
        roll = self.rng.random()
        if roll < 0.55:
            return StatusClass.SERVER_ERROR
        if roll < 0.85:
            return StatusClass.TIMEOUT
        return StatusClass.CLIENT_ERROR

    # -- span construction -----------------------------------------------------

    def _llm_call_span(
        self,
        *,
        tenant_id: str,
        tier: str,
        model: str,
        route: str,
        region: str,
        operation: Operation,
        start_offset_ms: float,
    ) -> SpanData:
        profile = MODEL_PROFILES[model]
        kv_utilization = self._advance_kv_cache()

        input_tokens = _sample_long_tail(self.rng, median=420, sigma=0.9, cap=32_000)
        if operation is Operation.EMBEDDINGS:
            output_tokens = 0
        else:
            output_tokens = _sample_long_tail(self.rng, median=180, sigma=1.0, cap=4_096)

        # Prefix caching: a chunk of the prompt is often already resident.
        cached_prompt_tokens = int(input_tokens * self.rng.betavariate(2.0, 5.0))

        # Queueing grows sharply once the KV cache is nearly full -- this is the
        # mechanism behind the latency cliff that dashboards should reveal.
        queue_time_ms = 0.0
        if kv_utilization > 0.8:
            queue_time_ms = (kv_utilization - 0.8) / 0.2 * self.rng.uniform(50.0, 900.0)

        # TTFT is prefill-dominated: it scales with the *uncached* prompt.
        uncached = max(0, input_tokens - cached_prompt_tokens)
        ttft_ms = (
            profile.base_ttft_ms + uncached * 0.05 * self.rng.uniform(0.8, 1.3) + queue_time_ms
        )

        itl_ms = profile.ms_per_output_token * self.rng.uniform(0.85, 1.4)
        decode_ms = output_tokens * itl_ms
        duration_ms = ttft_ms + decode_ms

        status = self._status_for(kv_utilization)
        if status is StatusClass.TIMEOUT:
            # A timeout truncates the response: fewer tokens, capped duration.
            duration_ms = min(duration_ms, 30_000.0)
            output_tokens = int(output_tokens * self.rng.uniform(0.0, 0.5))
        elif status is not StatusClass.OK:
            # A failed request does no useful decoding.
            duration_ms = ttft_ms * self.rng.uniform(0.2, 0.8)
            output_tokens = 0

        attrs: dict[str, object] = {
            A.TENANT_ID: tenant_id,
            A.TENANT_TIER: tier,
            A.ROUTE: route,
            A.ENDPOINT_REGION: region,
            A.STATUS_CLASS: status.value,
            A.GEN_AI_SYSTEM: "vllm",
            A.GEN_AI_OPERATION: operation.value,
            A.GEN_AI_REQUEST_MODEL: model,
            A.GEN_AI_RESPONSE_MODEL: model,
            A.GEN_AI_REQUEST_MAX_TOKENS: 4096,
            A.GEN_AI_REQUEST_TEMPERATURE: round(self.rng.uniform(0.0, 1.0), 2),
            A.GEN_AI_USAGE_INPUT_TOKENS: input_tokens,
            A.GEN_AI_USAGE_OUTPUT_TOKENS: output_tokens,
            A.LLM_KV_CACHE_UTILIZATION: round(kv_utilization, 4),
            A.LLM_KV_CACHE_BLOCKS: int(
                (input_tokens + output_tokens) / 1000 * profile.kv_blocks_per_1k_tokens
            ),
            A.LLM_TTFT_MS: round(ttft_ms, 2),
            A.LLM_ITL_MS: round(itl_ms, 3),
            A.LLM_QUEUE_TIME_MS: round(queue_time_ms, 2),
            A.LLM_CACHED_PROMPT_TOKENS: cached_prompt_tokens,
            A.LLM_BATCH_SIZE: self.rng.randint(1, 48),
            A.LLM_STREAMING: operation is Operation.CHAT,
            # High-cardinality: useful in raw spans, never a rollup key.
            # Drawn from the seeded RNG rather than uuid4: uuid4 is an
            # unseeded syscall, which both costs throughput at 20k spans/s and
            # makes a "reproducible" run not actually reproducible.
            "request.id": f"{self.rng.getrandbits(128):032x}",
            "prompt.hash": f"{self.rng.getrandbits(64):016x}",
        }
        if output_tokens and duration_ms > 0:
            attrs[A.LLM_TOKENS_PER_SECOND] = round(output_tokens / (duration_ms / 1000.0), 2)
        if status is not StatusClass.OK:
            attrs[A.ERROR_TYPE] = {
                StatusClass.SERVER_ERROR: "engine_overloaded",
                StatusClass.TIMEOUT: "deadline_exceeded",
                StatusClass.CLIENT_ERROR: "invalid_request",
            }[status]

        return SpanData(
            name=f"{operation.value} {model}",
            kind="client",
            start_offset_ms=start_offset_ms,
            duration_ms=duration_ms,
            attributes=attrs,
            status=status,
        )

    def _tool_span(self, *, tenant_id: str, start_offset_ms: float, step: int) -> SpanData:
        tool = self.rng.choice(TOOL_NAMES)
        duration_ms = self.rng.lognormvariate(math.log(45.0), 0.8)
        return SpanData(
            name=f"execute_tool {tool}",
            kind="internal",
            start_offset_ms=start_offset_ms,
            duration_ms=duration_ms,
            attributes={
                A.TENANT_ID: tenant_id,
                A.GEN_AI_OPERATION: Operation.TOOL.value,
                A.TOOL_NAME: tool,
                A.AGENT_STEP: step,
                A.STATUS_CLASS: StatusClass.OK.value,
            },
            status=StatusClass.OK,
        )

    def generate_trace(self) -> TraceData:
        """Build one agent invocation: LLM calls and tool calls under a root span.

        Multi-span traces are the point -- a pipeline that only ever sees flat,
        single-span requests never exercises parent/child relationships or the
        span-count amplification that makes trace volume outrun request volume.
        """
        tenant_id = self.tenants.sample()
        tier = self.tenants.tiers[tenant_id]
        model = self.rng.choice(MODELS)
        region = self.rng.choice(REGIONS)
        agent = self.rng.choice(AGENT_NAMES)

        # Most requests are a single LLM call; agent loops are the minority but
        # produce most of the spans.
        steps = self.rng.choices([1, 2, 3, 4], weights=[62, 22, 11, 5])[0]

        children: list[SpanData] = []
        cursor_ms = self.rng.uniform(0.5, 3.0)

        for step in range(steps):
            operation = Operation.EMBEDDINGS if self.rng.random() < 0.12 else Operation.CHAT
            route = (
                "/v1/embeddings"
                if operation is Operation.EMBEDDINGS
                else self.rng.choice(ROUTES[:2])
            )
            llm_span = self._llm_call_span(
                tenant_id=tenant_id,
                tier=tier,
                model=model,
                route=route,
                region=region,
                operation=operation,
                start_offset_ms=cursor_ms,
            )
            children.append(llm_span)
            cursor_ms += llm_span.duration_ms

            # A tool call typically follows an intermediate reasoning step.
            if step < steps - 1 and self.rng.random() < 0.7:
                tool_span = self._tool_span(
                    tenant_id=tenant_id, start_offset_ms=cursor_ms, step=step
                )
                children.append(tool_span)
                cursor_ms += tool_span.duration_ms

        # The root fails if any child failed -- errors propagate up an agent loop.
        worst = next(
            (c.status for c in children if c.status is not StatusClass.OK),
            StatusClass.OK,
        )
        root = SpanData(
            name=f"invoke_agent {agent}",
            kind="server",
            start_offset_ms=0.0,
            duration_ms=cursor_ms + self.rng.uniform(0.5, 4.0),
            attributes={
                A.TENANT_ID: tenant_id,
                A.TENANT_TIER: tier,
                A.ENDPOINT_REGION: region,
                A.ROUTE: "/v1/agents/invoke",
                A.STATUS_CLASS: worst.value,
                A.GEN_AI_OPERATION: Operation.AGENT_INVOKE.value,
                A.AGENT_NAME: agent,
                A.GEN_AI_REQUEST_MODEL: model,
                "request.id": f"{self.rng.getrandbits(128):032x}",
            },
            status=worst,
            children=tuple(children),
        )
        return TraceData(root=root, tenant_id=tenant_id)


# --- Rate shaping -------------------------------------------------------------


class Profile(StrEnum):
    """Load shapes. `burst` is the one Phase 6 measures against."""

    STEADY = "steady"
    BURST = "burst"
    RAMP = "ramp"


@dataclass(frozen=True)
class LoadProfile:
    """Target span rate as a function of elapsed time.

    Expressed in spans/s rather than requests/s because spans are what the
    pipeline actually moves, and one request produces a variable number of them.
    """

    profile: Profile
    sustained_spans_per_sec: int
    burst_spans_per_sec: int
    burst_period_s: float = 30.0
    burst_duration_s: float = 8.0
    ramp_duration_s: float = 120.0

    def target_rate(self, elapsed_s: float) -> float:
        """Spans/s the generator should be producing at `elapsed_s`."""
        if self.profile is Profile.STEADY:
            return float(self.sustained_spans_per_sec)

        if self.profile is Profile.BURST:
            # Sustained baseline with a periodic spike above the pipeline's
            # comfortable capacity -- the point is to make drops happen and
            # then measure how the system recovers.
            phase = elapsed_s % self.burst_period_s
            in_burst = phase < self.burst_duration_s
            return float(self.burst_spans_per_sec if in_burst else self.sustained_spans_per_sec)

        # RAMP: linear climb from 10% of sustained up to the burst rate, to find
        # the knee where lag starts growing without ever recovering.
        fraction = min(1.0, elapsed_s / self.ramp_duration_s)
        low = self.sustained_spans_per_sec * 0.1
        return low + (self.burst_spans_per_sec - low) * fraction


class RateLimiter:
    """Token-bucket pacer for a variable target rate.

    Uses a monotonic clock and accumulates fractional credit, so it stays
    accurate at rates far above the OS timer resolution -- at 20k spans/s,
    sleeping per span would be dominated by scheduler jitter.

    The clock is injectable so tests can advance time deterministically instead
    of sleeping.
    """

    def __init__(
        self,
        *,
        max_burst_credit: float = 5_000.0,
        clock: object | None = None,
    ) -> None:
        self._clock = clock or time.monotonic
        self._last = self._clock()  # type: ignore[operator]
        self._credit = 0.0
        self._max_burst_credit = max_burst_credit

    def acquire(self, rate_per_sec: float) -> int:
        """Return how many items may be emitted now at `rate_per_sec`.

        Returns 0 when the caller should wait. Credit is capped so a stalled
        generator cannot bank unlimited allowance and then dump it all at once.
        """
        now = self._clock()  # type: ignore[operator]
        elapsed = max(0.0, now - self._last)
        self._last = now
        self._credit = min(self._max_burst_credit, self._credit + elapsed * rate_per_sec)
        whole = int(self._credit)
        self._credit -= whole
        return whole


def zipf_share_table(pool: TenantPool, buckets: Sequence[int]) -> list[tuple[int, float]]:
    """(top-N, share of traffic) pairs -- for documenting the skew."""
    return [(n, pool.share_of_top(n)) for n in buckets]


__all__ = [
    "AGENT_NAMES",
    "MODELS",
    "MODEL_PROFILES",
    "REGIONS",
    "ROUTES",
    "LoadProfile",
    "ModelProfile",
    "Operation",
    "Profile",
    "RateLimiter",
    "SpanData",
    "StatusClass",
    "TenantPool",
    "TraceData",
    "WorkloadGenerator",
    "zipf_share_table",
]
