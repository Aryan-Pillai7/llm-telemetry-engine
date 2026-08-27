"""Topic specification parsing and the config/topics.yaml contract."""

from __future__ import annotations

import pytest
import yaml

from telemetry_engine.config import Settings
from telemetry_engine.ingest.topics import DEFAULT_TOPICS_FILE, TopicSpec, load_specs


def test_shipped_topics_file_parses() -> None:
    specs = load_specs()
    assert specs, "topics.yaml must define at least one topic"
    assert all(isinstance(s, TopicSpec) for s in specs)


def test_config_values_are_coerced_to_strings(tmp_path) -> None:
    """The Kafka admin protocol carries config values as strings.

    Writing `retention.ms: 21600000` unquoted in YAML yields an int, which the
    admin client rejects. Coercion happens at parse time so topics.yaml stays
    readable.
    """
    path = tmp_path / "topics.yaml"
    path.write_text(
        yaml.safe_dump(
            {"topics": [{"name": "t", "partitions": 3, "config": {"retention.ms": 60}}]}
        ),
        encoding="utf-8",
    )
    spec = load_specs(path)[0]
    assert spec.config == {"retention.ms": "60"}


def test_rejects_nonsense_partition_counts() -> None:
    with pytest.raises(ValueError, match="partitions must be >= 1"):
        TopicSpec(name="t", partitions=0)


def test_empty_file_is_an_error(tmp_path) -> None:
    """An empty topics.yaml should fail loudly, not silently reconcile nothing."""
    path = tmp_path / "topics.yaml"
    path.write_text("topics: []\n", encoding="utf-8")
    with pytest.raises(ValueError, match="no topics defined"):
        load_specs(path)


# --- Parity between the code's defaults and the deployed topic spec ----------
# These two files are edited by different people at different times. If they
# drift, the symptom is a pipeline that runs at the wrong partition count or
# silently loses buffered data early -- both hard to spot at a glance.


def test_spans_topic_name_matches_settings() -> None:
    settings = Settings()
    names = {s.name for s in load_specs()}
    assert settings.redpanda.spans_topic in names


def test_partition_count_matches_settings() -> None:
    """ADR-004/009: six partitions is a decision, not a default."""
    settings = Settings()
    spec = next(s for s in load_specs() if s.name == settings.redpanda.spans_topic)
    assert spec.partitions == settings.redpanda.partitions


def test_retention_matches_settings() -> None:
    """Retention is the backpressure buffer (ADR-003); it must match config."""
    settings = Settings()
    spec = next(s for s in load_specs() if s.name == settings.redpanda.spans_topic)
    expected_ms = settings.redpanda.retention_hours * 3600 * 1000
    assert int(spec.config["retention.ms"]) == expected_ms


def test_single_broker_means_no_replication() -> None:
    """ADR-001: one broker. A replication factor above 1 cannot be satisfied."""
    assert all(s.replication_factor == 1 for s in load_specs())


def test_topic_max_message_bytes_covers_collector_batches() -> None:
    """The broker must accept messages at least as large as the collector sends.

    Otherwise large batches are rejected at exactly the moment the pipeline is
    under the most pressure -- the failure lands where it hurts most.
    """
    collector_max = 4194304  # producer.max_message_bytes in deploy/otelcol/config.yaml
    spec = next(s for s in load_specs() if s.name == "otel.spans")
    assert int(spec.config["max.message.bytes"]) >= collector_max


def test_topics_file_is_where_the_code_expects() -> None:
    assert DEFAULT_TOPICS_FILE.is_file()
