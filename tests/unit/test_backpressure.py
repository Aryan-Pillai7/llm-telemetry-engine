"""The backpressure experiment's validity checks.

These test the checks themselves, not the pipeline. That is the point: the
checks are what stand between a plausible-looking number and a wrong one, and
four consecutive runs proved they fire on real bugs. A check that silently stops
working would remove the only thing that caught them.
"""

from __future__ import annotations

import pytest

from telemetry_engine.experiments.backpressure import (
    MIN_DELIVERY_EFFICIENCY,
    RECOVERED_LAG,
    BackpressureResult,
    ProbeResult,
    Sample,
    validate,
)


def _sample(t: float, *, lag: int = 0, accepted: int = 0, sent: int = 0, dropped: int = 0):
    return Sample(
        t=t,
        total_lag=lag,
        max_partition_lag=lag,
        accepted=accepted,
        sent=sent,
        dropped=dropped,
        queue_size=0,
    )


def _healthy_result(**overrides) -> BackpressureResult:
    """A run that should pass every check, as a baseline to perturb."""
    result = BackpressureResult(
        baseline_s=45.0,
        burst_s=60.0,
        sample_interval_s=1.0,
        generator_spans=600_000,
        generator_target_spans=600_000,
        collector_accepted=600_000,
        collector_sent=600_000,
        clickhouse_rows=600_000,
        endpoint_spans=0,
        rebalances_before=5,
        rebalances_after_baseline=5,
        rebalances_after=9,
        baseline_started_at=0.0,
        baseline_ended_at=45.0,
        burst_started_at=45.0,
        burst_ended_at=105.0,
    )
    result.samples = [
        _sample(t, lag=50_000 if 45 <= t < 105 else 0, accepted=t * 100, sent=t * 100)
        for t in range(0, 200, 1)
    ]
    result.probes = [ProbeResult(t=float(t), latency_ms=8.0, ok=True) for t in range(0, 200, 1)]
    for key, value in overrides.items():
        setattr(result, key, value)
    return result


def _check(result: BackpressureResult, name: str):
    return next(c for c in validate(result) if c.name == name)


def test_a_healthy_run_passes_everything() -> None:
    result = _healthy_result()
    failed = [c.name for c in validate(result) if not c.passed]
    assert not failed, f"baseline fixture should pass: {failed}"


# --- Each check fires on the thing it exists for -------------------------------


def test_generator_shortfall_is_caught() -> None:
    """Failure mode 1: lag stays flat because nothing was ever sent."""
    result = _healthy_result(generator_spans=300_000, generator_target_spans=600_000)
    assert not _check(result, "generator_hit_target").passed


def test_undelivered_load_is_caught() -> None:
    """Run 1's actual bug: spans created but discarded before the collector.

    generator_hit_target passes here -- it counts creation. This is the check
    that looks at the layer that matters.
    """
    result = _healthy_result(collector_accepted=300_000, collector_refused=0)
    assert _check(result, "generator_hit_target").passed
    assert not _check(result, "load_actually_delivered").passed


def test_refused_spans_count_as_delivered() -> None:
    """A refused span reached the collector; the collector shed it deliberately.

    Counting refusals against the generator would blame the wrong component.
    """
    result = _healthy_result(collector_accepted=550_000, collector_refused=50_000)
    assert _check(result, "load_actually_delivered").passed


def test_counter_reset_is_caught() -> None:
    """Run 4's actual bug: a scrape timeout that read as every counter zeroing."""
    result = _healthy_result()
    result.samples = [
        _sample(0, accepted=100, sent=100),
        _sample(1, accepted=200, sent=200),
        _sample(2, accepted=0, sent=0),  # collector restarted, or a fabricated sample
    ]
    assert not _check(result, "no_counter_reset").passed


def test_coarse_sampling_is_caught() -> None:
    """Failure mode 4: an interval that can step over the peak entirely."""
    result = _healthy_result(sample_interval_s=30.0, burst_s=60.0)
    assert not _check(result, "sampling_resolution").passed


def test_a_burst_that_did_not_stress_anything_is_caught() -> None:
    """'Recovered instantly' is vacuous if lag never grew."""
    result = _healthy_result()
    result.samples = [_sample(t, lag=10) for t in range(0, 200)]
    assert not _check(result, "burst_actually_stressed").passed


def test_unaccounted_spans_are_caught() -> None:
    """Failure mode 3: loss upstream leaves nothing to lag on."""
    result = _healthy_result(clickhouse_rows=100_000)
    assert not _check(result, "spans_accounted").passed


def test_missing_endpoint_probes_are_caught() -> None:
    """Run 2's actual bug: mismatched clocks left the probe windows empty.

    Without this check the experiment would assert 'endpoints are unaffected'
    having never successfully measured one.
    """
    result = _healthy_result()
    result.probes = []
    assert not _check(result, "endpoint_probed_under_load").passed


def test_lag_reader_interference_is_caught() -> None:
    """Run 3's actual bug: the reader joined the group it was measuring."""
    result = _healthy_result(rebalances_before=5, rebalances_after_baseline=8)
    assert not _check(result, "lag_reader_is_noninvasive").passed


def test_stall_induced_rebalances_do_not_fail_the_check() -> None:
    """Pausing the consumer rebalances the group by design.

    Counting those against the lag reader made a run fail for the wrong reason,
    which is only marginally better than not failing at all.
    """
    result = _healthy_result(rebalances_before=5, rebalances_after_baseline=5, rebalances_after=25)
    assert _check(result, "lag_reader_is_noninvasive").passed


# --- Derived measurements ------------------------------------------------------


def test_recovery_is_measured_after_the_stall_is_released() -> None:
    """Failure mode 6: lag falling while frozen or loaded says nothing."""
    result = _healthy_result(stall_released_at=105.0)
    result.samples = [
        _sample(50, lag=1),  # low during the burst; must NOT count as recovery
        _sample(110, lag=90_000),
        _sample(140, lag=10),
    ]
    assert result.recovery_seconds() == pytest.approx(35.0)


def test_recovery_is_none_when_it_never_drains() -> None:
    result = _healthy_result(stall_released_at=105.0)
    result.samples = [_sample(t, lag=90_000) for t in range(100, 200, 10)]
    assert result.recovery_seconds() is None


def test_recovery_threshold_allows_data_in_flight() -> None:
    """A healthy pipeline always has a little lag; zero would never be reached."""
    result = _healthy_result(stall_released_at=100.0)
    result.samples = [_sample(120, lag=RECOVERED_LAG)]
    assert result.recovery_seconds() is not None


def test_windows_use_observed_boundaries_not_nominal_durations() -> None:
    """A phase always overruns slightly; windowing on nominal misattributes edges."""
    result = _healthy_result(burst_started_at=50.0, burst_ended_at=115.0)
    assert result.burst_window() == (50.0, 115.0)


def test_sdk_loss_excludes_collector_refusals() -> None:
    """Different owners: an SDK queue overflow is not the memory limiter shedding."""
    result = _healthy_result(
        generator_spans=100_000,
        endpoint_spans=0,
        collector_accepted=80_000,
        collector_refused=15_000,
    )
    assert result.sdk_lost == 5_000


def test_delivery_threshold_is_pre_registered() -> None:
    """The threshold must not be tunable after seeing a run's numbers."""
    assert MIN_DELIVERY_EFFICIENCY == 0.95


def test_every_check_explains_what_it_would_hide() -> None:
    """A check nobody understands gets deleted the first time it is inconvenient."""
    for check in validate(_healthy_result()):
        assert check.would_hide, f"{check.name} does not say what it protects against"
        assert len(check.would_hide) > 40
