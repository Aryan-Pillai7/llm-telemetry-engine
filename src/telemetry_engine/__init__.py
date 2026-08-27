"""LLM & agent telemetry analytics engine.

A streaming pipeline that ingests OpenTelemetry data from LLM/agent endpoints
through Redpanda into ClickHouse (hot tier), and ages it out into
hive-partitioned Parquet queried by DuckDB (cold tier).
"""

__version__ = "0.1.0"
