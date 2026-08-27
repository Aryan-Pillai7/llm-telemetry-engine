"""ClickHouse connection helper.

Thin on purpose. `clickhouse-connect` is already a reasonable client; this only
centralizes connection construction so every entrypoint reads settings the same
way, and adds a readiness wait for the one case that genuinely needs it --
starting work right after `docker compose up`.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from telemetry_engine.common.logging import get_logger
from telemetry_engine.config import ClickHouseSettings, get_settings

log = get_logger(__name__)


def connect(settings: ClickHouseSettings | None = None, *, database: str | None = None) -> Client:
    """Open a ClickHouse client.

    `database` overrides the configured database, which matters for bootstrap:
    the first migration has to run before `telemetry` exists, so it connects to
    the server's default database instead.
    """
    cfg = settings or get_settings().clickhouse
    return clickhouse_connect.get_client(
        host=cfg.host,
        port=cfg.http_port,
        username=cfg.user,
        password=cfg.password,
        database=database if database is not None else cfg.database,
        # Compression pays for itself on the wide result sets the cold-tier
        # export job pulls.
        compress=True,
    )


@contextmanager
def client(
    settings: ClickHouseSettings | None = None, *, database: str | None = None
) -> Iterator[Client]:
    """Context-managed client, closed on exit."""
    conn = connect(settings, database=database)
    try:
        yield conn
    finally:
        conn.close()


def wait_until_ready(
    settings: ClickHouseSettings | None = None,
    *,
    timeout_s: float = 60.0,
    interval_s: float = 1.0,
) -> None:
    """Block until ClickHouse answers a trivial query, or raise on timeout.

    Compose healthchecks already gate service startup, but anything run
    immediately after `up` on a cold volume can still race the server's own
    initialization. Failing here with a clear message beats a connection error
    surfacing from the middle of a migration.
    """
    cfg = settings or get_settings().clickhouse
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            # Connect to the server default DB: `telemetry` may not exist yet.
            with client(cfg, database="default") as conn:
                conn.command("SELECT 1")
            return
        except Exception as exc:
            last_error = exc
            time.sleep(interval_s)

    raise TimeoutError(
        f"ClickHouse at {cfg.host}:{cfg.http_port} not ready after {timeout_s}s: {last_error}"
    )
