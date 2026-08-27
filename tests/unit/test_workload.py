"""Workload generation: tenant skew, request shape, and rate control.

All pure logic -- no SDK, no collector, no real clock. That is the payoff of
keeping generation separate from span emission.
"""

from __future__ import annotations

import random
from collections import Counter
from itertools import pairwise

import pytest

from telemetry_engine.emitters import attributes as A
from telemetry_engine.emitters.workload import (
    MODEL_PROFILES,
    MODELS,
    LoadProfile,
    Profile,
    RateLimiter,
    StatusClass,
    TenantPool,
    WorkloadGenerator,
)

# --- Tenant skew --------------------------------------------------------------


def test_tenant_traffic_is_actually_skewed() -> None:
    """Zipfian, not uniform.

    Uniform tenants would make every aggregation look well-behaved and the
    cardinality guard look unnecessary -- the workload has to reproduce the
    imbalance that makes top-K and `__other__` earn their place.
    """
    pool = TenantPool(count=50, alpha=1.2, rng=random.Random(7))
    draws = Counter(pool.sample() for _ in range(50_000))

    top_5_share = sum(count for _, count in draws.most_common(5)) / 50_000
    uniform_share = 5 / 50
    assert top_5_share > uniform_share * 3, (
        f"top 5 of 50 tenants took {top_5_share:.1%}; expected heavy skew"
    )


def test_share_of_top_matches_sampling() -> None:
    """The analytic share and the empirical share should agree."""
    pool = TenantPool(count=40, alpha=1.1, rng=random.Random(11))
    predicted = pool.share_of_top(5)
    draws = Counter(pool.sample() for _ in range(40_000))
    top_ids = {f"tenant-{i:04d}" for i in range(5)}
    observed = sum(c for t, c in draws.items() if t in top_ids) / 40_000
    assert predicted == pytest.approx(observed, abs=0.03)


def test_higher_alpha_means_more_skew() -> None:
    flat = TenantPool(count=50, alpha=0.6, rng=random.Random(1))
    steep = TenantPool(count=50, alpha=1.8, rng=random.Random(1))
    assert steep.share_of_top(5) > flat.share_of_top(5)


def test_every_tenant_is_reachable() -> None:
    """Even the smallest tenant must be sampleable, or the tail is fictional."""
    pool = TenantPool(count=20, alpha=1.2, rng=random.Random(3))
    seen = {pool.sample() for _ in range(200_000)}
    assert len(seen) == 20


def test_tiers_correlate_with_volume() -> None:
    """Big tenants get better tiers, as in a real customer base."""
    pool = TenantPool(count=100, alpha=1.2, rng=random.Random(5))
    assert pool.tiers["tenant-0000"] == "enterprise"
    assert pool.tiers["tenant-0099"] == "free"


def test_pool_rejects_nonsense_parameters() -> None:
    with pytest.raises(ValueError, match="tenant count"):
        TenantPool(count=0, alpha=1.0, rng=random.Random())
    with pytest.raises(ValueError, match="alpha"):
        TenantPool(count=5, alpha=0.0, rng=random.Random())


# --- Trace shape --------------------------------------------------------------


def test_generation_is_reproducible() -> None:
    """Same seed, same workload -- otherwise runs are not comparable."""
    a = WorkloadGenerator(seed=42).generate_trace()
    b = WorkloadGenerator(seed=42).generate_trace()
    assert a.tenant_id == b.tenant_id
    assert a.span_count == b.span_count
    assert a.root.duration_ms == pytest.approx(b.root.duration_ms)


def test_different_seeds_diverge() -> None:
    a = [WorkloadGenerator(seed=1).generate_trace().tenant_id for _ in range(20)]
    b = [WorkloadGenerator(seed=2).generate_trace().tenant_id for _ in range(20)]
    assert a != b


def test_traces_are_multi_span() -> None:
    """Span volume must outrun request volume, or trace amplification is untested."""
    gen = WorkloadGenerator(seed=9)
    total = sum(gen.generate_trace().span_count for _ in range(500))
    assert total / 500 > 1.5


def test_children_are_nested_under_the_root() -> None:
    gen = WorkloadGenerator(seed=13)
    trace = gen.generate_trace()
    assert trace.root.children, "an agent invocation should contain at least one call"
    assert all(child.start_offset_ms >= 0 for child in trace.root.children)


def test_child_spans_fit_inside_the_root() -> None:
    """A child that outlives its parent is a broken trace."""
    gen = WorkloadGenerator(seed=17)
    for _ in range(200):
        trace = gen.generate_trace()
        root_end = trace.root.start_offset_ms + trace.root.duration_ms
        for span in trace.root.walk():
            assert span.start_offset_ms + span.duration_ms <= root_end + 1e-6


def test_every_span_carries_the_tenant() -> None:
    """Tenant is the primary rollup dimension; a span without it is unattributable."""
    gen = WorkloadGenerator(seed=21)
    for _ in range(100):
        trace = gen.generate_trace()
        for span in trace.root.walk():
            assert span.attributes[A.TENANT_ID] == trace.tenant_id


def test_all_spans_have_a_status_class() -> None:
    gen = WorkloadGenerator(seed=23)
    valid = {s.value for s in StatusClass}
    for _ in range(100):
        for span in gen.generate_trace().root.walk():
            assert span.attributes[A.STATUS_CLASS] in valid


def test_root_status_reflects_child_failures() -> None:
    """Errors propagate up an agent loop rather than being hidden."""
    gen = WorkloadGenerator(seed=29, error_rate=0.5)
    saw_propagation = False
    for _ in range(300):
        trace = gen.generate_trace()
        child_failed = any(c.status is not StatusClass.OK for c in trace.root.children)
        if child_failed:
            assert trace.root.status is not StatusClass.OK
            saw_propagation = True
    assert saw_propagation, "error rate too low to exercise propagation"


def test_models_all_have_profiles() -> None:
    """A model without a profile would KeyError at generation time."""
    assert set(MODELS) == set(MODEL_PROFILES)


# --- Attribute realism --------------------------------------------------------


def test_llm_spans_carry_genai_and_kv_cache_attributes() -> None:
    gen = WorkloadGenerator(seed=31)
    llm_spans = [
        span
        for _ in range(50)
        for span in gen.generate_trace().root.walk()
        if A.GEN_AI_USAGE_INPUT_TOKENS in span.attributes
    ]
    assert llm_spans
    for span in llm_spans:
        assert span.attributes[A.GEN_AI_USAGE_INPUT_TOKENS] > 0
        assert 0.0 <= float(span.attributes[A.LLM_KV_CACHE_UTILIZATION]) <= 1.0
        assert float(span.attributes[A.LLM_TTFT_MS]) > 0
        assert span.attributes[A.GEN_AI_REQUEST_MODEL] in MODELS


def test_token_counts_are_long_tailed() -> None:
    """Most requests small, a few enormous -- the big ones dominate cost."""
    gen = WorkloadGenerator(seed=37)
    tokens = sorted(
        int(span.attributes[A.GEN_AI_USAGE_INPUT_TOKENS])
        for _ in range(400)
        for span in gen.generate_trace().root.walk()
        if A.GEN_AI_USAGE_INPUT_TOKENS in span.attributes
    )
    median = tokens[len(tokens) // 2]
    p99 = tokens[int(len(tokens) * 0.99)]
    assert p99 > median * 5, f"expected a long tail; median={median} p99={p99}"


def test_kv_cache_pressure_is_autocorrelated() -> None:
    """Cache occupancy drifts; it is a property of the endpoint, not per-request noise.

    Without autocorrelation, latency degradation shows up as isolated spikes
    rather than the sustained runs a real endpoint produces.
    """
    gen = WorkloadGenerator(seed=41)
    series = [
        float(span.attributes[A.LLM_KV_CACHE_UTILIZATION])
        for _ in range(300)
        for span in gen.generate_trace().root.walk()
        if A.LLM_KV_CACHE_UTILIZATION in span.attributes
    ]
    steps = [abs(b - a) for a, b in pairwise(series)]
    mean_step = sum(steps) / len(steps)
    # A uniform redraw over [0.05, 0.99] would average roughly 0.31 per step.
    assert mean_step < 0.10, f"KV cache looks like noise, not drift (mean step {mean_step:.3f})"


def test_high_cardinality_attributes_are_present_on_raw_spans() -> None:
    """They belong in raw spans for debugging -- just never in a rollup key."""
    gen = WorkloadGenerator(seed=43)
    span = gen.generate_trace().root
    assert "request.id" in span.attributes


def test_errors_cluster_under_cache_pressure() -> None:
    """Failures should correlate with load, as on a real endpoint."""
    gen = WorkloadGenerator(seed=47, error_rate=0.05)
    ok_pressure: list[float] = []
    err_pressure: list[float] = []
    for _ in range(2_000):
        for span in gen.generate_trace().root.walk():
            if A.LLM_KV_CACHE_UTILIZATION not in span.attributes:
                continue
            util = float(span.attributes[A.LLM_KV_CACHE_UTILIZATION])
            (ok_pressure if span.status is StatusClass.OK else err_pressure).append(util)
    assert err_pressure, "no errors generated"
    assert sum(err_pressure) / len(err_pressure) > sum(ok_pressure) / len(ok_pressure)


def test_error_rate_bounds_are_validated() -> None:
    with pytest.raises(ValueError, match="error_rate"):
        WorkloadGenerator(error_rate=1.5)


def test_rollup_and_high_cardinality_keys_are_disjoint() -> None:
    """An attribute cannot be both a grouping key and unbounded."""
    A.assert_dimensions_are_disjoint()


# --- Rate shaping -------------------------------------------------------------


class FakeClock:
    """Manually advanced monotonic clock."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_rate_limiter_grants_the_expected_number() -> None:
    clock = FakeClock()
    limiter = RateLimiter(clock=clock)
    clock.advance(1.0)
    assert limiter.acquire(5_000) == 5_000


def test_rate_limiter_accumulates_fractional_credit() -> None:
    """At 20k spans/s, per-span sleeps would be lost in scheduler jitter.

    Fractional credit must carry across calls or the achieved rate drifts low.
    """
    clock = FakeClock()
    limiter = RateLimiter(clock=clock)
    granted = 0
    for _ in range(1_000):
        clock.advance(0.001)  # 1ms at 5000/s = 5 spans exactly
        granted += limiter.acquire(5_000)
    assert granted == pytest.approx(5_000, rel=0.01)


def test_rate_limiter_grants_nothing_without_elapsed_time() -> None:
    clock = FakeClock()
    limiter = RateLimiter(clock=clock)
    assert limiter.acquire(1_000) == 0


def test_rate_limiter_caps_banked_credit() -> None:
    """A stalled generator must not bank unlimited allowance and dump it at once."""
    clock = FakeClock()
    limiter = RateLimiter(max_burst_credit=1_000.0, clock=clock)
    clock.advance(60.0)  # would be 300k spans uncapped
    assert limiter.acquire(5_000) == 1_000


def test_steady_profile_is_flat() -> None:
    p = LoadProfile(
        profile=Profile.STEADY, sustained_spans_per_sec=5_000, burst_spans_per_sec=20_000
    )
    assert p.target_rate(0) == 5_000
    assert p.target_rate(999) == 5_000


def test_burst_profile_alternates() -> None:
    """Burst must exceed sustained, or Phase 6 has nothing to measure."""
    p = LoadProfile(
        profile=Profile.BURST,
        sustained_spans_per_sec=5_000,
        burst_spans_per_sec=20_000,
        burst_period_s=30.0,
        burst_duration_s=8.0,
    )
    assert p.target_rate(1.0) == 20_000  # inside the burst
    assert p.target_rate(20.0) == 5_000  # between bursts
    assert p.target_rate(31.0) == 20_000  # next period


def test_ramp_profile_climbs_monotonically() -> None:
    p = LoadProfile(
        profile=Profile.RAMP,
        sustained_spans_per_sec=5_000,
        burst_spans_per_sec=20_000,
        ramp_duration_s=100.0,
    )
    rates = [p.target_rate(t) for t in range(0, 120, 10)]
    assert rates == sorted(rates)
    assert rates[0] < 5_000
    assert rates[-1] == pytest.approx(20_000)
