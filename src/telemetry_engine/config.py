"""Single source of truth for runtime configuration.

Everything is env-driven so the same code runs on a laptop, in CI, and inside a
container without edits. Defaults are the local `docker compose` stack, so a
fresh clone works with no `.env` at all.

Env vars are prefixed `TE_` and nested with `__`, e.g.:

    TE_REDPANDA__BOOTSTRAP_SERVERS=localhost:19092
    TE_CLICKHOUSE__PASSWORD=hunter2
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Repo root, resolved from this file: src/telemetry_engine/config.py -> ../../
REPO_ROOT = Path(__file__).resolve().parents[2]


class RedpandaSettings(BaseModel):
    """Kafka-API endpoint settings for the single-broker Redpanda dev cluster."""

    bootstrap_servers: str = "localhost:19092"
    spans_topic: str = "otel.spans"

    # Six partitions at the 5k spans/s sustained target (ADR-009). Messages are
    # keyed by trace_id rather than tenant_id so a hot tenant cannot own a
    # partition and cap throughput at one consumer's rate (ADR-004).
    partitions: int = 6

    # Redpanda is the shock absorber: retention long enough that a ClickHouse
    # stall shows up as consumer lag rather than data loss (ADR-003).
    retention_hours: int = 6


class ClickHouseSettings(BaseModel):
    """Connection settings for the single-node ClickHouse instance."""

    host: str = "localhost"
    http_port: int = 8123
    native_port: int = 9000
    database: str = "telemetry"
    user: str = "default"
    password: str = ""

    # How long raw, high-cardinality spans live in the hot tier before TTL drops
    # them. Rollups outlive raw data; the cold tier keeps the detail (ADR-006).
    raw_ttl_hours: int = 48


class ColdTierSettings(BaseModel):
    """Parquet lake layout for the DuckDB-queried cold tier (ADR-007, ADR-008)."""

    root: Path = REPO_ROOT / "data" / "cold"

    # Partition on date only; `hour` rides along as a column. Rows are sorted by
    # (tenant_id, ts) so DuckDB still prunes both via row-group statistics,
    # without the small-file explosion tenant-level directories would cause.
    partition_keys: tuple[str, ...] = ("dt",)
    sort_keys: tuple[str, ...] = ("tenant_id", "ts")

    # Compaction target. Small Parquet files are the classic lake failure mode.
    target_file_mb: int = 128


class WorkloadSettings(BaseModel):
    """Synthetic workload shape for the mock LLM/agent endpoints (ADR-009)."""

    tenants: int = 50
    zipf_alpha: float = 1.2  # traffic skew across tenants; >1 = a few loud tenants
    sustained_spans_per_sec: int = 5_000
    burst_spans_per_sec: int = 20_000


class Settings(BaseSettings):
    """Top-level application settings."""

    model_config = SettingsConfigDict(
        env_prefix="TE_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    environment: str = "local"
    log_level: str = "INFO"

    redpanda: RedpandaSettings = Field(default_factory=RedpandaSettings)
    clickhouse: ClickHouseSettings = Field(default_factory=ClickHouseSettings)
    coldtier: ColdTierSettings = Field(default_factory=ColdTierSettings)
    workload: WorkloadSettings = Field(default_factory=WorkloadSettings)

    schemas_dir: Path = REPO_ROOT / "schemas" / "clickhouse"


def get_settings() -> Settings:
    """Build settings from the environment.

    Deliberately not cached: tests monkeypatch the environment, and nothing in
    this pipeline constructs settings in a hot loop.
    """
    return Settings()
