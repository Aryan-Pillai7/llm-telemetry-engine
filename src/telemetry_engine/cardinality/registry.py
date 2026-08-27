"""The dimension registry: what is allowed to become a rollup key.

Loads `schemas/clickhouse/dimensions.yaml` and turns it into something the rest
of the system can check against. The registry is the *contract*; `guard.py`
enforces it against live data.

The property worth stating plainly, because it is the entire point of this
design: the number of rows a rollup can contain per time bucket is bounded by

    product(budget + 2 for every dimension)

and that bound holds regardless of what the emitters send, because every value
is rewritten to one of three things: itself (if registered), `__other__` (if
present but unregistered), or `__none__` (if absent). Cardinality becomes a
number chosen in a reviewed file rather than an emergent property of whatever
traffic shows up.

The two sentinels are kept distinct on purpose. `__other__` means "something is
emitting a value nobody registered" and is worth alerting on; `__none__` means
"this dimension does not apply to this kind of span" and is routine. Collapsing
them together buries the first signal under the second -- which is exactly what
the first version of this design did.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from functools import cached_property
from pathlib import Path
from typing import Any

import yaml

from telemetry_engine.config import REPO_ROOT

DEFAULT_REGISTRY_FILE = REPO_ROOT / "schemas" / "clickhouse" / "dimensions.yaml"


class DimensionSource(StrEnum):
    """How a dimension's allowlist is populated."""

    # Enumerated in the registry file.
    STATIC = "static"
    # Computed from recent traffic: the top-K values by volume.
    TOP_K = "top_k"


@dataclass(frozen=True)
class Dimension:
    """One dimension that may appear in a rollup key."""

    name: str
    column: str
    attribute: str
    source: DimensionSource
    budget: int
    values: tuple[str, ...] = ()
    top_k_window_hours: int = 24

    def __post_init__(self) -> None:
        if self.budget < 1:
            raise ValueError(f"{self.name}: budget must be >= 1, got {self.budget}")
        if self.source is DimensionSource.STATIC:
            if not self.values:
                raise ValueError(f"{self.name}: static dimensions must enumerate values")
            if len(self.values) > self.budget:
                raise ValueError(
                    f"{self.name}: {len(self.values)} declared values exceed budget "
                    f"{self.budget} -- raise the budget deliberately or trim the list"
                )
        elif self.values:
            raise ValueError(
                f"{self.name}: top_k dimensions must not enumerate values; "
                "the allowlist is computed from traffic"
            )

    @property
    def max_distinct(self) -> int:
        """Budget plus both sentinel buckets (`__other__` and `__none__`)."""
        return self.budget + 2


@dataclass(frozen=True)
class Registry:
    """The parsed dimension registry."""

    version: int
    other_bucket: str
    none_bucket: str
    dimensions: tuple[Dimension, ...]
    forbidden: frozenset[str]

    @cached_property
    def by_name(self) -> dict[str, Dimension]:
        return {d.name: d for d in self.dimensions}

    @cached_property
    def static_dimensions(self) -> tuple[Dimension, ...]:
        return tuple(d for d in self.dimensions if d.source is DimensionSource.STATIC)

    @cached_property
    def top_k_dimensions(self) -> tuple[Dimension, ...]:
        return tuple(d for d in self.dimensions if d.source is DimensionSource.TOP_K)

    @property
    def max_rows_per_bucket(self) -> int:
        """Upper bound on rollup rows per time bucket.

        The product of every dimension's budget plus its two sentinel buckets.
        This is the number that makes the design's cardinality claim checkable
        rather than aspirational -- and it is asserted in the test suite.
        """
        total = 1
        for dimension in self.dimensions:
            total *= dimension.max_distinct
        return total

    def is_forbidden(self, attribute: str) -> bool:
        return attribute in self.forbidden

    def validate_columns(self, available: set[str]) -> None:
        """Check every registered dimension maps to a real column.

        Called against the rollup table's columns so a registry entry that names
        a column nobody created fails loudly at apply time rather than silently
        producing empty groups.
        """
        missing = {d.column for d in self.dimensions} - available
        if missing:
            raise ValueError(f"registry names columns that do not exist: {sorted(missing)}")


def load_registry(path: Path | None = None) -> Registry:
    """Parse the registry file."""
    src = path or DEFAULT_REGISTRY_FILE
    raw: dict[str, Any] = yaml.safe_load(src.read_text(encoding="utf-8")) or {}

    entries = raw.get("dimensions") or []
    if not entries:
        raise ValueError(f"no dimensions declared in {src}")

    dimensions = tuple(
        Dimension(
            name=str(e["name"]),
            column=str(e["column"]),
            attribute=str(e["attribute"]),
            source=DimensionSource(str(e.get("source", "static"))),
            budget=int(e["budget"]),
            values=tuple(str(v) for v in (e.get("values") or ())),
            top_k_window_hours=int(e.get("top_k_window_hours", 24)),
        )
        for e in entries
    )

    names = [d.name for d in dimensions]
    if len(names) != len(set(names)):
        raise ValueError(f"duplicate dimension names in {src}")

    registry = Registry(
        version=int(raw.get("version", 1)),
        other_bucket=str(raw.get("other_bucket", "__other__")),
        none_bucket=str(raw.get("none_bucket", "__none__")),
        dimensions=dimensions,
        forbidden=frozenset(str(f) for f in (raw.get("forbidden_as_dimensions") or ())),
    )

    # A dimension that is also on the forbidden list is a contradiction, and
    # exactly the mistake this design exists to prevent.
    conflicts = {d.attribute for d in dimensions} & registry.forbidden
    if conflicts:
        raise ValueError(
            f"attributes are both registered dimensions and forbidden: {sorted(conflicts)}"
        )

    return registry
