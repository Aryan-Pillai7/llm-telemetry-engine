"""Shared test fixtures.

Unit tests must never need the docker compose stack. Anything that does is
marked `integration` and deselected by default (see pyproject).
"""

from __future__ import annotations

import pytest

from telemetry_engine.config import Settings


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch, tmp_path) -> Settings:
    """Default settings with the cold-tier root redirected into tmp_path.

    Keeps tests from writing Parquet into the developer's real `data/` tree.
    """
    monkeypatch.setenv("TE_COLDTIER__ROOT", str(tmp_path / "cold"))
    return Settings()
