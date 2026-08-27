"""Consumer lag and collector drop counters: the pipeline's own SLIs.

Lag is the primary health signal of this design (ADR-003). Redpanda absorbs
bursts, so a ClickHouse stall shows up here as growing lag rather than as data
loss -- and the collector's send-failed counter is where genuine loss appears.

Both live outside ClickHouse: lag is a broker fact, drops are a collector fact.
Grafana in this stack has exactly one datasource, ClickHouse, so rather than
adding a Prometheus container just to graph two numbers, a sampler reads them
and writes them into a ClickHouse table. One writer, one datasource, no extra
standing service.

Phase 6's backpressure experiment reads the same table, so the numbers on the
dashboard and the numbers in the write-up come from one source.
"""

from __future__ import annotations

import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

from confluent_kafka import Consumer, TopicPartition

from telemetry_engine.common.logging import get_logger
from telemetry_engine.config import RedpandaSettings, Settings

log = get_logger(__name__)

COLLECTOR_METRICS_URL = "http://localhost:8888/metrics"

# Counters worth recording. Names come from the collector's self-telemetry.
_COLLECTOR_COUNTERS = (
    "otelcol_receiver_accepted_spans",
    "otelcol_receiver_refused_spans",
    "otelcol_exporter_sent_spans",
    "otelcol_exporter_send_failed_spans",
    "otelcol_exporter_enqueue_failed_spans",
    "otelcol_exporter_queue_size",
    "otelcol_exporter_queue_capacity",
)


@dataclass
class LagSnapshot:
    """Consumer lag at a point in time."""

    total_lag: int = 0
    per_partition: dict[int, int] = field(default_factory=dict)
    high_watermarks: dict[int, int] = field(default_factory=dict)

    @property
    def max_partition_lag(self) -> int:
        return max(self.per_partition.values(), default=0)

    @property
    def partitions(self) -> int:
        return len(self.per_partition)


@dataclass
class CollectorSnapshot:
    """Collector counters at a point in time.

    These are cumulative counters, not rates. Stored raw so the dashboard can
    difference them; storing a rate here would bake in a sampling interval.
    """

    accepted_spans: int = 0
    refused_spans: int = 0
    sent_spans: int = 0
    send_failed_spans: int = 0
    enqueue_failed_spans: int = 0
    queue_size: int = 0
    queue_capacity: int = 0

    @property
    def dropped_spans(self) -> int:
        """Spans the pipeline lost, on purpose, under pressure.

        `send_failed` is an export that exhausted its retries; `enqueue_failed`
        is the non-blocking queue rejecting a batch outright (ADR-003). Both are
        real loss and both are reportable -- the point of the design is that
        this number is known, not that it is zero.
        """
        return self.send_failed_spans + self.enqueue_failed_spans

    @property
    def queue_utilization(self) -> float:
        return self.queue_size / self.queue_capacity if self.queue_capacity else 0.0


def read_lag(settings: RedpandaSettings, *, group: str = "clickhouse-spans") -> LagSnapshot:
    """Read committed offsets against high watermarks for the consumer group.

    Creates a consumer in the target group but never subscribes or assigns, so
    it does not join the group and cannot trigger a rebalance of ClickHouse's
    consumers.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": group,
            "enable.auto.commit": False,
        }
    )
    snapshot = LagSnapshot()
    try:
        metadata = consumer.list_topics(settings.spans_topic, timeout=30.0)
        topic = metadata.topics.get(settings.spans_topic)
        if topic is None or topic.error:
            return snapshot

        partitions = [TopicPartition(settings.spans_topic, p) for p in topic.partitions]
        committed = consumer.committed(partitions, timeout=30.0)

        for tp in committed:
            _, high = consumer.get_watermark_offsets(tp, timeout=30.0, cached=False)
            # A partition with no committed offset yet reports a negative
            # sentinel; treat that as "everything is outstanding".
            position = tp.offset if tp.offset >= 0 else 0
            snapshot.per_partition[tp.partition] = max(0, high - position)
            snapshot.high_watermarks[tp.partition] = high

        snapshot.total_lag = sum(snapshot.per_partition.values())
        return snapshot
    finally:
        consumer.close()


def read_collector(url: str = COLLECTOR_METRICS_URL) -> CollectorSnapshot:
    """Scrape the collector's Prometheus endpoint.

    Returns zeros if the collector is unreachable rather than raising: a
    monitoring sampler that dies when one of its sources blips is worse than one
    that records a gap.
    """
    values: dict[str, float] = {}
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            body = response.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, OSError) as exc:
        log.warning("collector_metrics_unreachable", url=url, error=str(exc))
        return CollectorSnapshot()

    for line in body.splitlines():
        if line.startswith("#"):
            continue
        for counter in _COLLECTOR_COUNTERS:
            if line.startswith(counter):
                try:
                    values[counter] = values.get(counter, 0.0) + float(line.rsplit(" ", 1)[1])
                except (ValueError, IndexError):
                    continue

    return CollectorSnapshot(
        accepted_spans=int(values.get("otelcol_receiver_accepted_spans", 0)),
        refused_spans=int(values.get("otelcol_receiver_refused_spans", 0)),
        sent_spans=int(values.get("otelcol_exporter_sent_spans", 0)),
        send_failed_spans=int(values.get("otelcol_exporter_send_failed_spans", 0)),
        enqueue_failed_spans=int(values.get("otelcol_exporter_enqueue_failed_spans", 0)),
        queue_size=int(values.get("otelcol_exporter_queue_size", 0)),
        queue_capacity=int(values.get("otelcol_exporter_queue_capacity", 0)),
    )


def sample(settings: Settings) -> tuple[LagSnapshot, CollectorSnapshot]:
    """Take one reading of both sources."""
    return read_lag(settings.redpanda), read_collector()


def record(conn, settings: Settings) -> tuple[LagSnapshot, CollectorSnapshot]:
    """Take a reading and write it to telemetry.pipeline_health."""
    lag, collector = sample(settings)
    conn.insert(
        "telemetry.pipeline_health",
        [
            [
                lag.total_lag,
                lag.max_partition_lag,
                lag.partitions,
                collector.accepted_spans,
                collector.refused_spans,
                collector.sent_spans,
                collector.send_failed_spans,
                collector.enqueue_failed_spans,
                collector.queue_size,
                collector.queue_capacity,
            ]
        ],
        column_names=[
            "total_lag",
            "max_partition_lag",
            "partitions",
            "accepted_spans",
            "refused_spans",
            "sent_spans",
            "send_failed_spans",
            "enqueue_failed_spans",
            "queue_size",
            "queue_capacity",
        ],
    )
    return lag, collector


def monitor(conn, settings: Settings, *, interval_s: float = 5.0, duration_s: float | None = None):
    """Sample on an interval until `duration_s` elapses (or forever).

    Yields each reading so a caller can display progress.
    """
    started = time.monotonic()
    while duration_s is None or (time.monotonic() - started) < duration_s:
        yield record(conn, settings)
        time.sleep(interval_s)
