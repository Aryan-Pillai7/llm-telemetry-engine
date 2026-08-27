# LLM & Agent Telemetry Analytics Engine

A streaming observability pipeline for LLM and agent workloads — token
throughput, KV-cache utilization, prompt/response latency, and distributed
traces from model endpoints, ingested and aggregated in real time.

The design problem it is built around: **ingest high-cardinality, multi-tenant
trace metrics at scale without applying backpressure to the inference endpoints
being observed.** A telemetry pipeline that stalls its own subject has become
the outage it was meant to detect.

## Architecture

```
[ Mock LLM/agent endpoints ]
            │ OTLP
            ▼
   [ OTel Collector ]         memory_limiter → batch → kafka exporter
            │                 bounded, non-blocking queue: drop and count
            ▼
      [ Redpanda ]            single broker, 6 partitions, keyed by trace_id
            │                 disk-backed retention = the shock absorber
            ▼
     [ ClickHouse ]           Kafka table engine consumes directly
            │                 raw spans (short TTL) + 1m/1h rollups
     ┌──────┴──────┐
     ▼             ▼
[ Grafana ]   [ Python batch job ]
 hot tier      DuckDB + hive-partitioned Parquet (cold tier)
```

Deliberately lean: no Kafka cluster, no ZooKeeper, no Iceberg catalog server, no
separate consumer microservice. DuckDB is a library, not a container — the cold
tier costs disk, not standing RAM.

## Status

Phase 0 (scaffold) complete. See the build plan for what lands next.

## Requirements

- Docker Engine with Compose v2
- Python 3.11+

## Quickstart

```bash
git clone <repo> && cd llm-telemetry-engine
python tasks.py install     # editable install + dev deps
python tasks.py up          # start redpanda, clickhouse, otelcol, grafana
```

Defaults target the local compose stack, so no `.env` is needed for a fresh
clone. Copy `.env.example` to `.env` only to override something.

### Task runner

There is no Makefile — the primary dev environment is Windows, and one
cross-platform `tasks.py` beats a Makefile plus a PowerShell shim that drift
apart. Run `python tasks.py` to list targets.

| Target | Does |
| --- | --- |
| `install` | Editable install with dev extras |
| `lint` / `fmt` | Ruff check / autofix + format |
| `test` | Unit tests (no stack needed) |
| `test-integration` | Integration tests (stack required) |
| `ci` | Everything CI runs |
| `up` / `down` / `nuke` | Start / stop / stop-and-delete-volumes |
| `logs` / `ps` | Tail logs / container status |

### CLI

```bash
telemetry-engine config     # print effective, env-resolved settings
telemetry-engine version
```

## Layout

| Path | Contains |
| --- | --- |
| `deploy/` | All infra config: compose file, otelcol, ClickHouse, Grafana provisioning |
| `schemas/clickhouse/` | Versioned DDL — the source of truth for hot-tier tables |
| `src/telemetry_engine/cardinality/` | Dimension registry and the cardinality guard |
| `src/telemetry_engine/emitters/` | Mock LLM endpoints and workload generation |
| `src/telemetry_engine/ingest/` | Topic reconciliation, consumer-lag SLI |
| `src/telemetry_engine/storage/` | ClickHouse client and migration runner |
| `src/telemetry_engine/coldtier/` | Parquet export, compaction, DuckDB query layer |
| `tests/unit/` | Pure logic tests — no stack required |
| `tests/integration/` | Marked tests that need `python tasks.py up` |
| `docs/` | Architecture, backpressure characterization, cardinality, runbook |

## Design notes

Two decisions carry most of the weight:

**Backpressure.** Nothing in the pipeline blocks upstream. The collector's
export queue is bounded and non-blocking: on overflow it drops and increments a
counter rather than pushing back toward the endpoint. Redpanda's disk-backed
retention absorbs bursts, so a ClickHouse stall shows up as *consumer lag*, not
data loss. Lag is the pipeline's primary SLI; dropped spans are counted and
dashboarded, never hidden.

**Cardinality.** Facts and dimensions are strictly separated. Unbounded
identifiers (trace_id, request_id, prompt hash) live only in the raw table,
under a short TTL, and are never grouped by. Rollups may key only on dimensions
declared in `schemas/clickhouse/dimensions.yaml`; unknown or over-budget tenants
collapse into an explicit `__other__` bucket at the ingest boundary. The bound
is enforced in code and asserted in tests, not left to convention.

Full rationale for both, plus measured numbers, lands in `docs/`.
