"""Emitter integration: spans must actually reach the broker.

Requires `python tasks.py up`. These assert the transport really works, which
unit tests deliberately cannot: the workload tests run without an SDK at all.
"""

from __future__ import annotations

import time
import urllib.request

import pytest
from confluent_kafka import Consumer, TopicPartition
from fastapi.testclient import TestClient

from telemetry_engine.config import Settings
from telemetry_engine.emitters.endpoint import create_app
from telemetry_engine.emitters.otlp import ExporterConfig, build_tracer_provider, emit_trace
from telemetry_engine.emitters.workload import WorkloadGenerator

pytestmark = pytest.mark.integration

OTLP_ENDPOINT = "http://localhost:4317"


def _topic_span_count(settings: Settings) -> int:
    """Sum of high-watermark offsets across all partitions.

    Message count, not span count -- the collector batches many spans per
    message. Used only to detect that *something* arrived.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": settings.redpanda.bootstrap_servers,
            "group.id": f"test-probe-{time.time_ns()}",
            "enable.auto.commit": False,
        }
    )
    try:
        md = consumer.list_topics(settings.redpanda.spans_topic, timeout=30.0)
        topic = md.topics[settings.redpanda.spans_topic]
        total = 0
        for partition_id in topic.partitions:
            _, high = consumer.get_watermark_offsets(
                TopicPartition(settings.redpanda.spans_topic, partition_id), timeout=30.0
            )
            total += high
        return total
    finally:
        consumer.close()


def _collector_metric(name: str) -> float:
    """Read one counter from the collector's self-telemetry."""
    body = urllib.request.urlopen("http://localhost:8888/metrics", timeout=10).read().decode()
    for line in body.splitlines():
        if line.startswith(name) and not line.startswith("#"):
            return float(line.rsplit(" ", 1)[1])
    return 0.0


def test_emitted_spans_reach_the_topic() -> None:
    """The full emitter path: generator -> SDK -> collector -> Redpanda."""
    settings = Settings()
    before = _topic_span_count(settings)

    provider = build_tracer_provider(ExporterConfig(endpoint=OTLP_ENDPOINT))
    tracer = provider.get_tracer("test")
    generator = WorkloadGenerator(tenants=10, seed=99)

    emitted = sum(emit_trace(tracer, generator.generate_trace()) for _ in range(50))
    # shutdown() flushes the BatchSpanProcessor; without it the test races the
    # 1s schedule delay.
    provider.shutdown()
    assert emitted > 0

    deadline = time.monotonic() + 60.0
    while time.monotonic() < deadline:
        if _topic_span_count(settings) > before:
            return
        time.sleep(1.0)

    pytest.fail(f"no new messages on {settings.redpanda.spans_topic} after 60s")


def test_collector_reports_no_refusals() -> None:
    """The receiver must not be refusing spans at test volume.

    Refusals mean memory_limiter is shedding, which at this rate would point at
    a misconfigured collector rather than genuine pressure.
    """
    assert _collector_metric("otelcol_receiver_refused_spans") == 0.0


def test_spans_spread_across_partitions() -> None:
    """ADR-004: trace_id keying should fill every partition.

    If messages piled into one partition, ClickHouse could only consume them
    with a single consumer and the whole partition-count decision would be
    moot.
    """
    settings = Settings()
    consumer = Consumer(
        {
            "bootstrap.servers": settings.redpanda.bootstrap_servers,
            "group.id": f"test-spread-{time.time_ns()}",
            "enable.auto.commit": False,
        }
    )
    try:
        md = consumer.list_topics(settings.redpanda.spans_topic, timeout=30.0)
        topic = md.topics[settings.redpanda.spans_topic]
        highs = []
        for partition_id in topic.partitions:
            _, high = consumer.get_watermark_offsets(
                TopicPartition(settings.redpanda.spans_topic, partition_id), timeout=30.0
            )
            highs.append(high)
    finally:
        consumer.close()

    assert all(h > 0 for h in highs), f"some partitions are empty: {highs}"
    # Generous bound: this only needs to catch gross skew, such as every message
    # landing on one partition.
    assert max(highs) < min(highs) * 3, f"partition skew looks wrong: {highs}"


def test_mock_endpoint_serves_and_emits() -> None:
    """The mock endpoint returns a response and emits spans for it."""
    app = create_app(exporter=ExporterConfig(endpoint=OTLP_ENDPOINT), tenants=10, seed=5)
    with TestClient(app) as client:
        response = client.post("/v1/chat/completions", json={"max_tokens": 256})
        assert response.status_code == 200
        body = response.json()
        assert body["spans_emitted"] >= 1
        assert body["tenant_id"].startswith("tenant-")
        assert body["input_tokens"] > 0

        assert client.get("/health").json()["status"] == "ok"


def test_endpoint_response_is_not_blocked_by_telemetry() -> None:
    """The core claim, at endpoint scale.

    Export is a handoff to a background thread. If emitting ever blocked on the
    collector, the pipeline would be backpressuring the service it observes --
    the exact failure this project exists to avoid.
    """
    app = create_app(exporter=ExporterConfig(endpoint=OTLP_ENDPOINT), tenants=10, seed=6)
    with TestClient(app) as client:
        # Warm up: first request pays import and connection costs.
        client.post("/v1/chat/completions", json={})

        started = time.perf_counter()
        for _ in range(20):
            assert client.post("/v1/chat/completions", json={}).status_code == 200
        elapsed_ms = (time.perf_counter() - started) * 1000.0 / 20

    # Generous: this catches a synchronous export (hundreds of ms per call),
    # not ordinary variance on a loaded laptop.
    assert elapsed_ms < 100.0, f"mean {elapsed_ms:.1f}ms/request suggests blocking export"
