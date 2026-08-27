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

Phase 3 complete — the hot path moves data end to end. On a cold stack,
106,737 emitted spans produced exactly 106,737 rows in ClickHouse, with zero
dead-lettered messages and consumer lag back to 0.

Measured over 1.5M rows: pipeline latency (span end → queryable) p50 3.2s /
p95 8.5s, 8 active MergeTree parts, 2.6x compression, tenant-scoped queries in
81 ms. Phase 4 (rollups and the cardinality guard) is next.

## Requirements

- Docker Engine with Compose v2
- Python 3.11+

## Quickstart

```bash
git clone <repo> && cd llm-telemetry-engine
python tasks.py install     # create .venv, install package + dev deps
python tasks.py up          # start the stack, then reconcile topics and schema
```

`up` is a cold-start path: from empty volumes it brings up all four services,
waits for each to be genuinely ready, creates the `otel.spans` topic at the
configured partition count and retention, and applies the ClickHouse schema.
Re-running it is a no-op.

| Service | Host address | Notes |
| --- | --- | --- |
| Redpanda | `localhost:19092` | Kafka API; admin/metrics on `:9644` |
| ClickHouse | `localhost:8123` | HTTP; native protocol on `:9000` |
| OTel Collector | `localhost:4317` / `:4318` | OTLP gRPC / HTTP; self-telemetry on `:8888` |
| Grafana | http://localhost:3000 | anonymous admin, no login |

Idle footprint is roughly 540 MiB across the four containers, against a ~4.3 GiB
ceiling that only matters under load.

Defaults target the local compose stack, so no `.env` is needed for a fresh
clone. Copy `.env.example` to `.env` only to override something.

### Task runner

There is no Makefile — the primary dev environment is Windows, and one
cross-platform `tasks.py` beats a Makefile plus a PowerShell shim that drift
apart. Run `python tasks.py` to list targets.

| Target | Does |
| --- | --- |
| `install` | Create `.venv`, editable install with dev extras |
| `lint` / `fmt` | Ruff check / autofix + format |
| `test` | Unit tests (no stack needed) |
| `test-integration` | Integration tests (stack required) |
| `ci` | Everything CI runs |
| `up` / `down` / `nuke` | Start + bootstrap / stop / stop-and-delete-volumes |
| `up-bare` | Start containers without bootstrapping |
| `bootstrap` / `migrate` | Reconcile topics + schema / schema only |
| `serve` | Run the mock inference endpoint on :8080 |
| `load` / `load-burst` | Drive steady / bursty synthetic telemetry |
| `logs` / `ps` | Tail logs / container status |

### CLI

```bash
telemetry-engine config         # print effective, env-resolved settings
telemetry-engine stack wait     # block until every service is truly ready
telemetry-engine topics apply   # reconcile Redpanda topics from topics.yaml
telemetry-engine topics list    # show topics as they exist on the broker
telemetry-engine migrate        # apply pending ClickHouse migrations
telemetry-engine bootstrap      # stack wait + topics apply + migrate
telemetry-engine serve          # mock inference endpoint (demo / manual pokes)
telemetry-engine load           # synthetic load at a target span rate
```

### Seeing data land

```bash
python tasks.py up                               # stack + topics + schema
telemetry-engine load --duration 20 --rate 5000  # emit
# then, after a few seconds:
```
```sql
SELECT tenant_id, count(), quantile(0.95)(ttft_ms)
FROM telemetry.spans_raw GROUP BY tenant_id ORDER BY 2 DESC LIMIT 5
```

`spans_raw` is the high-cardinality landing table: one row per span, a 48-hour
TTL, and an open `attributes` Map so a newly emitted attribute is queryable
immediately without a schema change. It is for trace debugging and cold-tier
export — **not** for dashboards, which will read the Phase 4 rollups.

### Generating telemetry

Two emitters, both producing identical span shapes from the same generator:

```bash
telemetry-engine serve                                  # instrumented HTTP endpoint
telemetry-engine load --duration 30 --rate 5000         # 5k spans/s sustained
telemetry-engine load --profile burst --duration 60     # spikes above capacity
telemetry-engine load --profile ramp --duration 120     # find the knee
```

`serve` is a real instrumented service — point a client at it and watch traces
appear. It cannot reach the 5k spans/s target, because HTTP overhead in Python
dominates; `load` synthesizes the same spans directly and is what the
backpressure experiment runs against.

`load` always reports achieved rate against target, and warns when the
*generator* falls short. A load generator that quietly misses its target turns
a backpressure measurement into fiction — the gap has to be attributable to the
generator or the pipeline, never ambiguous.

Most commands take `--dry-run`.

## Layout

| Path | Contains |
| --- | --- |
| `deploy/` | All infra config: compose file, otelcol, ClickHouse, Grafana provisioning |
| `deploy/redpanda/topics.yaml` | Declarative topic spec, reconciled by the CLI |
| `schemas/clickhouse/` | Versioned DDL — the source of truth for hot-tier tables |
| `src/telemetry_engine/cardinality/` | Dimension registry and the cardinality guard |
| `src/telemetry_engine/emitters/` | Mock LLM endpoints, workload generation, OTLP export |
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
