"""Declarative Redpanda topic reconciliation.

`deploy/redpanda/topics.yaml` describes the topics the pipeline needs; this
module makes the broker match it. Auto-topic-creation would be simpler, but it
creates topics with broker defaults -- one partition, default retention -- and
the partition count and retention window are load-bearing architectural
decisions here (ADR-003, ADR-004, ADR-009), not incidental settings.

Deliberately one-directional: create topics, grow partitions, update configs.
It never shrinks partitions (Kafka cannot) and never deletes topics (that would
make a config typo destroy buffered telemetry).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from confluent_kafka.admin import (
    AdminClient,
    ConfigResource,
    NewPartitions,
    NewTopic,
)

from telemetry_engine.common.logging import get_logger
from telemetry_engine.config import REPO_ROOT, RedpandaSettings, get_settings

log = get_logger(__name__)

DEFAULT_TOPICS_FILE = REPO_ROOT / "deploy" / "redpanda" / "topics.yaml"

# Admin operations against a cold broker can take a moment; these are futures'
# timeouts, not request timeouts.
_OP_TIMEOUT_S = 30.0


@dataclass(frozen=True)
class TopicSpec:
    """Desired state for a single topic."""

    name: str
    partitions: int
    replication_factor: int = 1
    config: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.partitions < 1:
            raise ValueError(f"{self.name}: partitions must be >= 1, got {self.partitions}")
        if self.replication_factor < 1:
            raise ValueError(
                f"{self.name}: replication_factor must be >= 1, got {self.replication_factor}"
            )


@dataclass
class ReconcileResult:
    """What reconciliation did, for reporting and tests."""

    created: list[str] = field(default_factory=list)
    partitions_grown: list[str] = field(default_factory=list)
    configs_updated: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    # Requested shrinks are refused rather than attempted, and reported here.
    refused: list[str] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        return bool(self.created or self.partitions_grown or self.configs_updated)


def load_specs(path: Path | None = None) -> list[TopicSpec]:
    """Parse topics.yaml into specs.

    All config values are coerced to strings because that is what the Kafka
    admin protocol carries; writing `retention.ms: 21600000` unquoted in YAML
    should not become a type error.
    """
    src = path or DEFAULT_TOPICS_FILE
    raw: dict[str, Any] = yaml.safe_load(src.read_text(encoding="utf-8")) or {}
    topics = raw.get("topics") or []
    if not topics:
        raise ValueError(f"no topics defined in {src}")

    return [
        TopicSpec(
            name=str(t["name"]),
            partitions=int(t["partitions"]),
            replication_factor=int(t.get("replication_factor", 1)),
            config={str(k): str(v) for k, v in (t.get("config") or {}).items()},
        )
        for t in topics
    ]


def _admin(settings: RedpandaSettings | None = None) -> AdminClient:
    cfg = settings or get_settings().redpanda
    return AdminClient({"bootstrap.servers": cfg.bootstrap_servers})


def _existing_partition_counts(admin: AdminClient) -> dict[str, int]:
    md = admin.list_topics(timeout=_OP_TIMEOUT_S)
    return {name: len(t.partitions) for name, t in md.topics.items() if not t.error}


def _current_config(admin: AdminClient, topic: str) -> dict[str, str]:
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
    futures = admin.describe_configs([resource])
    entries = futures[resource].result(timeout=_OP_TIMEOUT_S)
    return {name: str(entry.value) for name, entry in entries.items()}


def reconcile(
    specs: list[TopicSpec] | None = None,
    settings: RedpandaSettings | None = None,
    *,
    dry_run: bool = False,
) -> ReconcileResult:
    """Make the broker match the spec. Safe to run repeatedly."""
    desired = specs if specs is not None else load_specs()
    admin = _admin(settings)
    result = ReconcileResult()

    existing = _existing_partition_counts(admin)

    for spec in desired:
        if spec.name not in existing:
            log.info("creating_topic", topic=spec.name, partitions=spec.partitions)
            if not dry_run:
                new = NewTopic(
                    spec.name,
                    num_partitions=spec.partitions,
                    replication_factor=spec.replication_factor,
                    config=spec.config,
                )
                admin.create_topics([new])[spec.name].result(timeout=_OP_TIMEOUT_S)
            result.created.append(spec.name)
            continue

        current_partitions = existing[spec.name]
        topic_changed = False

        if spec.partitions > current_partitions:
            log.info(
                "growing_partitions",
                topic=spec.name,
                from_partitions=current_partitions,
                to_partitions=spec.partitions,
            )
            if not dry_run:
                admin.create_partitions([NewPartitions(spec.name, spec.partitions)])[
                    spec.name
                ].result(timeout=_OP_TIMEOUT_S)
            result.partitions_grown.append(spec.name)
            topic_changed = True
        elif spec.partitions < current_partitions:
            # Kafka cannot shrink partitions, and pretending otherwise would
            # fail confusingly at apply time.
            log.warning(
                "refusing_partition_shrink",
                topic=spec.name,
                current=current_partitions,
                requested=spec.partitions,
                hint="partition counts cannot be reduced; recreate the topic by hand",
            )
            result.refused.append(spec.name)

        if spec.config:
            current = _current_config(admin, spec.name)
            drift = {k: v for k, v in spec.config.items() if current.get(k) != v}
            if drift:
                log.info("updating_topic_config", topic=spec.name, changes=sorted(drift))
                if not dry_run:
                    resource = ConfigResource(ConfigResource.Type.TOPIC, spec.name)
                    for key, value in spec.config.items():
                        resource.set_config(key, value)
                    admin.alter_configs([resource])[resource].result(timeout=_OP_TIMEOUT_S)
                result.configs_updated.append(spec.name)
                topic_changed = True

        if not topic_changed and spec.name not in result.refused:
            result.unchanged.append(spec.name)

    return result
