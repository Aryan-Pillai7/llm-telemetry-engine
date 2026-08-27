"""The dimension registry and the cardinality bound it promises."""

from __future__ import annotations

import pytest
import yaml

from telemetry_engine.cardinality.registry import (
    DEFAULT_REGISTRY_FILE,
    Dimension,
    DimensionSource,
    load_registry,
)
from telemetry_engine.emitters import attributes as A
from telemetry_engine.emitters.workload import MODELS, REGIONS, ROUTES, Operation, StatusClass


@pytest.fixture(scope="module")
def registry():
    return load_registry()


def _write_registry(tmp_path, payload: dict):
    path = tmp_path / "dimensions.yaml"
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return path


# --- The bound ----------------------------------------------------------------


def test_the_bound_is_finite_and_declared(registry) -> None:
    """The headline property: rollup cardinality is a chosen number.

    Without the guard, tenant_id alone is unbounded and the rollup can exceed
    the raw data it summarises. With it, the worst case is the product of
    budgets -- large, but finite, and written down in a reviewed file.
    """
    assert registry.max_rows_per_bucket > 0
    assert all(d.budget > 0 for d in registry.dimensions)


def test_bound_accounts_for_both_sentinels(registry) -> None:
    """budget + 2: one bucket for unregistered, one for absent."""
    for dimension in registry.dimensions:
        assert dimension.max_distinct == dimension.budget + 2


def test_bound_is_the_product_of_dimension_budgets(registry) -> None:
    expected = 1
    for dimension in registry.dimensions:
        expected *= dimension.budget + 2
    assert registry.max_rows_per_bucket == expected


def test_sentinels_are_distinct(registry) -> None:
    """Collapsing them together buries the actionable signal under routine data.

    That is not hypothetical: the first version of the rollup view did exactly
    this, putting 18,243 tool spans with no model into the same bucket as 300
    genuinely unregistered ones.
    """
    assert registry.other_bucket != registry.none_bucket


def test_sentinels_cannot_collide_with_real_values(registry) -> None:
    """A real value equal to a sentinel would be silently misattributed."""
    for dimension in registry.static_dimensions:
        assert registry.other_bucket not in dimension.values
        assert registry.none_bucket not in dimension.values


# --- Registry integrity -------------------------------------------------------


def test_shipped_registry_parses(registry) -> None:
    assert registry.dimensions
    assert DEFAULT_REGISTRY_FILE.is_file()


def test_static_dimensions_declare_values(registry) -> None:
    for dimension in registry.static_dimensions:
        assert dimension.values, f"{dimension.name} is static but enumerates nothing"


def test_static_values_fit_their_budget() -> None:
    """Declaring more values than the budget allows is a contradiction."""
    with pytest.raises(ValueError, match="exceed budget"):
        Dimension(
            name="model",
            column="model",
            attribute="m",
            source=DimensionSource.STATIC,
            budget=2,
            values=("a", "b", "c"),
        )


def test_top_k_dimensions_do_not_enumerate_values() -> None:
    """A top-K allowlist comes from traffic; a static list would contradict it."""
    with pytest.raises(ValueError, match="must not enumerate"):
        Dimension(
            name="tenant_id",
            column="tenant_id",
            attribute="tenant.id",
            source=DimensionSource.TOP_K,
            budget=100,
            values=("tenant-1",),
        )


def test_budget_must_be_positive() -> None:
    with pytest.raises(ValueError, match="budget must be"):
        Dimension(name="x", column="x", attribute="x", source=DimensionSource.TOP_K, budget=0)


def test_duplicate_dimension_names_are_rejected(tmp_path) -> None:
    path = _write_registry(
        tmp_path,
        {
            "dimensions": [
                {"name": "d", "column": "d", "attribute": "a", "source": "top_k", "budget": 5},
                {"name": "d", "column": "d2", "attribute": "b", "source": "top_k", "budget": 5},
            ]
        },
    )
    with pytest.raises(ValueError, match="duplicate dimension"):
        load_registry(path)


def test_empty_registry_is_rejected(tmp_path) -> None:
    path = _write_registry(tmp_path, {"dimensions": []})
    with pytest.raises(ValueError, match="no dimensions"):
        load_registry(path)


def test_a_dimension_cannot_also_be_forbidden(tmp_path) -> None:
    """The contradiction this whole design exists to prevent."""
    path = _write_registry(
        tmp_path,
        {
            "dimensions": [
                {
                    "name": "trace",
                    "column": "trace_id",
                    "attribute": "trace_id",
                    "source": "top_k",
                    "budget": 10,
                }
            ],
            "forbidden_as_dimensions": ["trace_id"],
        },
    )
    with pytest.raises(ValueError, match="both registered dimensions and forbidden"):
        load_registry(path)


# --- Parity with the rest of the system ---------------------------------------


def test_unbounded_identifiers_are_forbidden(registry) -> None:
    """ADR-006: these belong in spans_raw and nowhere near a GROUP BY."""
    for attribute in ("trace_id", "span_id", "request.id", "prompt.hash", "error.type"):
        assert registry.is_forbidden(attribute), f"{attribute} must be forbidden as a dimension"


def test_registry_dimensions_match_the_attribute_vocabulary(registry) -> None:
    """Every registered dimension must correspond to an attribute the emitters set."""
    registered = {d.attribute for d in registry.dimensions}
    assert registered <= A.ROLLUP_DIMENSION_KEYS, (
        f"registry names attributes absent from the vocabulary: "
        f"{sorted(registered - A.ROLLUP_DIMENSION_KEYS)}"
    )


def test_forbidden_attributes_are_never_rollup_keys(registry) -> None:
    assert not (registry.forbidden & A.ROLLUP_DIMENSION_KEYS)


def test_static_values_cover_what_the_generator_emits(registry) -> None:
    """A workload value missing from the registry lands in __other__.

    That would be correct behaviour but a useless demo: the guard would bucket
    ordinary traffic and the `__other__` signal would stop meaning anything.
    """
    declared = {d.name: set(d.values) for d in registry.static_dimensions}

    assert set(MODELS) <= declared["model"], (
        f"models the generator emits are unregistered: {set(MODELS) - declared['model']}"
    )
    assert set(REGIONS) <= declared["region"]
    assert {o.value for o in Operation} <= declared["operation"]
    assert {s.value for s in StatusClass} <= declared["status_class"]
    # The generator also emits the agent-invoke route, which is not in ROUTES.
    assert set(ROUTES) <= declared["route"]
    assert "/v1/agents/invoke" in declared["route"]
