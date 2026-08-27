"""Integration checks against a running stack.

Requires `python tasks.py up`. Deselected by default and in CI, which has no
stack. These verify that reconciliation actually produced the shape the
architecture assumes -- not that the code paths merely run.
"""

from __future__ import annotations

import pytest
from confluent_kafka.admin import AdminClient, ConfigResource

from telemetry_engine.config import Settings
from telemetry_engine.ingest.topics import load_specs, reconcile
from telemetry_engine.storage.client import client
from telemetry_engine.storage.migrations import apply_all

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def live_settings() -> Settings:
    return Settings()


@pytest.fixture(scope="module")
def admin(live_settings: Settings) -> AdminClient:
    return AdminClient({"bootstrap.servers": live_settings.redpanda.bootstrap_servers})


def test_clickhouse_is_reachable_from_the_host(live_settings: Settings) -> None:
    """Guards the config.d mount trap: a directory mount over config.d hides the
    image's listen_host setting and ClickHouse binds loopback only."""
    with client(live_settings.clickhouse, database="default") as conn:
        assert conn.query("SELECT 1").result_rows == [(1,)]


def test_database_exists(live_settings: Settings) -> None:
    with client(live_settings.clickhouse, database="default") as conn:
        rows = conn.query(
            "SELECT name FROM system.databases WHERE name = %(db)s",
            parameters={"db": live_settings.clickhouse.database},
        ).result_rows
    assert rows, "run `python tasks.py bootstrap`"


def test_migrations_are_recorded_and_rerun_is_a_noop(live_settings: Settings) -> None:
    result = apply_all(live_settings)
    assert not result.applied, "bootstrap should have applied these already"
    assert not result.drifted, f"migrations edited after being applied: {result.drifted}"
    assert result.skipped


def test_spans_topic_has_the_configured_partition_count(
    admin: AdminClient, live_settings: Settings
) -> None:
    """ADR-004/009. A topic auto-created with one partition caps ingest at one
    consumer's throughput, which would quietly invalidate the whole experiment."""
    md = admin.list_topics(timeout=30.0)
    topic = md.topics.get(live_settings.redpanda.spans_topic)
    assert topic is not None, "run `python tasks.py bootstrap`"
    assert len(topic.partitions) == live_settings.redpanda.partitions


def test_topic_retention_matches_spec(admin: AdminClient, live_settings: Settings) -> None:
    """Retention is the backpressure buffer; if it is short, lag becomes loss."""
    name = live_settings.redpanda.spans_topic
    spec = next(s for s in load_specs() if s.name == name)
    resource = ConfigResource(ConfigResource.Type.TOPIC, name)
    entries = admin.describe_configs([resource])[resource].result(timeout=30.0)
    assert entries["retention.ms"].value == spec.config["retention.ms"]


def test_reconcile_is_idempotent() -> None:
    result = reconcile()
    assert not result.changed, f"unexpected changes on a reconciled broker: {result}"
    assert not result.refused
