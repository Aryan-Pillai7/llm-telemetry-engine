# LLM & Agent Telemetry Analytics Engine

A streaming observability pipeline for LLM and agent workloads — token
throughput, KV-cache utilization, prompt/response latency, and distributed
traces from model endpoints, ingested and aggregated in real time.

The design problem it is built around: **ingest high-cardinality, multi-tenant
trace metrics at scale without applying backpressure to the inference endpoints
being observed.** A telemetry pipeline that stalls its own subject has become
the outage it was meant to detect.

```bash
python tasks.py install && python tasks.py demo
```

---

## What this project actually turned out to be about

The pipeline works, and the numbers below are real. But the throughline of the
build was something else, and it is the part worth reading:

> **Several of this system's verification mechanisms initially looked correct
> while checking the wrong thing — or while not running at all.**

Not one bug of that shape. A recurring one, at every level of the stack, found
five separate times:

| Where | The mechanism | What it was actually doing |
| --- | --- | --- |
| Aggregation | The `__other__` cardinality bucket | Bounded and *meaningless* — 18,243 routine spans hid exactly 300 unregistered ones, so the actionable signal was 2% of its own bucket (ADR-015) |
| Ingest | The pipeline-latency query | Ran, returned numbers, and measured backlog replay instead of steady state — p95 of 24s against a true 8.5s |
| Dashboards | Panel SQL validation | Every query valid against ClickHouse; every panel failing through Grafana, because the time variables are interpolated frontend-only (ADR-018) |
| Experiment | The backpressure harness | **Four consecutive runs reported INVALID.** Three of the four bugs were in the measuring apparatus, not the pipeline (ADR-021) |
| Cold tier | The export's value-fingerprint check | Written, documented, and **never called** — the exporter verified row counts only, exactly the gap the function existed to close (ADR-027) |

That last one is a distinct failure mode from the rest, and the most
interesting. A check examining the wrong layer at least *runs* — its output can
be compared against reality. **An inert check emits nothing**: nothing
contradicts it, no test fails, and no amount of runtime testing can find it.
Only reading the code can.

So that reading is automated. The response to each of these was to make the
wrong version *unavailable* rather than documented:

- **ADR-027** — `scripts/find_inert_guards.py` parses the source for guards
  never called from the path they protect, and runs in CI. First run found a
  real one: `Registry.validate_columns`, which promised in its own docstring to
  fail "at apply time" and was called from nowhere.
- **ADR-028** — the same failure one level up. mypy was configured in Phase 0
  and invoked by nothing for eight phases. First run: 13 errors, including
  `None - timedelta` in `rollup-backfill`. A configured tool nobody runs is
  indistinguishable from no tool, except it looks like coverage.
- **ADR-029** — CI now runs the integration suite against a real stack.
  Simulating that job locally before pushing found two ordering bugs in the
  workflow itself. A CI job is code; writing one without running it is the same
  mistake as writing a guard without calling it.

Elsewhere the same principle: `ingest/latency.py` **raises** if a latency window
filters on ingest time instead of event time. `read_collector()` returns `None`
rather than a zeroed snapshot, because fabricated data survives review while a
gap does not. The lag reader cannot join the consumer group it measures.

Full account of the four invalid backpressure runs:
[docs/backpressure.md](docs/backpressure.md).

---

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
            │                 raw spans (48h TTL) → 1m → 1h rollups
     ┌──────┴──────┐
     ▼             ▼
[ Grafana ]   [ Python batch job ]
 hot tier      DuckDB + hive-partitioned Parquet (cold tier)
```

Deliberately lean: no Kafka cluster, no ZooKeeper, no Iceberg catalog server, no
separate consumer microservice. DuckDB is a library, not a container — the cold
tier costs disk, not standing RAM. Full design and rationale:
[docs/architecture.md](docs/architecture.md).

## Status

Phases 0–9 complete. 150 unit + 40 integration tests, no skips; ruff and mypy
clean; CI runs the whole suite against a live stack.

### Throughput: the honest number

**Sustained delivered throughput is ~12.4k spans/s at 100% delivery.**

Phase 2 reported 19.7k spans/s. That was true about spans *created* by the load
generator and was never verified as spans *delivered* to the pipeline — at that
rate the SDK's own export queue discarded a large share before the collector saw
them. Calibration ([`scripts/calibrate_generator.py`](scripts/calibrate_generator.py))
established the delivered figure; ADR-020 records the correction.

**Quote 12.4k/s.** The larger number measures the generator, not this pipeline.
It is left in the build log rather than deleted, because how it was wrong is
more useful than the number was.

### Measured

| | |
| --- | --- |
| Sustained delivered throughput | ~12.4k spans/s at 100% delivery |
| Ingest latency (span end → queryable) | p50 3.2 s, p95 8.5 s |
| Peak lag under a 68 s ClickHouse stall | 135,447 messages |
| Recovery once the consumer resumed | 34.2 s |
| Endpoint p99 during that stall | 28.4 ms → 32.6 ms (+15%) |
| Rollup fidelity | totals match raw exactly; tDigest p95 within 0.44% |
| Rollup reduction | 95× fewer rows to scan |
| Tenant-scoped query on 1.7M rows | 81 ms |
| Cold tier | 50.5 bytes/row; tenant-scoped query 55 ms |
| At-least-once duplication in the lake | 4.9% (both views published) |

## Requirements

- Docker Engine with Compose v2
- Python 3.11+

## Quickstart

```bash
git clone <repo> && cd llm-telemetry-engine
python tasks.py install     # create .venv, install package + dev deps
python tasks.py demo        # stack + telemetry + rollups + cold tier, ~2 min
```

The demo prints what actually landed — row counts, lag, duplication, dead-letter
count — rather than announcing success. Then open **http://localhost:3000** (no
login) for three provisioned dashboards.

To drive it yourself instead:

```bash
python tasks.py up                                  # stack + topics + schema
telemetry-engine load --duration 30 --rate 5000     # emit
telemetry-engine cold export                        # age out to Parquet
```

| Service | Host address |
| --- | --- |
| Grafana | http://localhost:3000 |
| ClickHouse | `localhost:8123` (HTTP), `:9000` (native) |
| Redpanda | `localhost:19092` (Kafka API) |
| OTel Collector | `:4317` / `:4318` OTLP, `:8888` self-telemetry |

Idle footprint ~540 MiB across four containers.

### Task runner

No Makefile — the primary dev environment is Windows, and one cross-platform
`tasks.py` beats a Makefile plus a PowerShell shim that drift apart. Run
`python tasks.py` to list targets.

| Target | Does |
| --- | --- |
| `install` / `demo` | Set up / one-command populated pipeline |
| `lint` `fmt` `typecheck` `audit-guards` | Static checks |
| `test` / `test-integration` / `coverage` | Test suites |
| `ci` | Everything the static CI jobs run |
| `up` `down` `nuke` `logs` `ps` | Stack lifecycle |
| `load` / `load-burst` / `serve` | Generate telemetry |

### CLI

```bash
telemetry-engine config            # effective, env-resolved settings
telemetry-engine bootstrap         # topics + schema + allowlist
telemetry-engine load              # synthetic load at a target span rate
telemetry-engine monitor           # sample lag and drop counters
telemetry-engine dimensions status # observed cardinality vs budget
telemetry-engine rollup-status     # do the rollups cover all of raw?
telemetry-engine cold status       # lake coverage vs the hot-tier TTL
telemetry-engine backpressure      # the measured experiment, graded
telemetry-engine dashboards        # regenerate Grafana JSON
```

## Design notes

**Backpressure** ([measured](docs/backpressure.md)). Nothing in the pipeline
blocks upstream. The collector's export queue is bounded and non-blocking: on
overflow it drops and increments a counter rather than pushing back toward the
endpoint. Redpanda's disk-backed retention absorbs bursts, so a ClickHouse stall
shows up as *consumer lag*, not data loss. Lag is the primary SLI; dropped spans
are counted and dashboarded, never hidden.

**Cardinality** ([design](docs/architecture.md)). Facts and dimensions are
strictly separated. Unbounded identifiers (trace_id, request_id, prompt hash)
live only in `spans_raw` under a short TTL and are never grouped by. Rollups key
only on dimensions declared in `schemas/clickhouse/dimensions.yaml`, each with a
budget, enforced by a ClickHouse dictionary the rollup views consult per row.

| Outcome | Meaning | Action |
| --- | --- | --- |
| the value | registered | — |
| `__other__` | present but **unregistered** | investigate: shadow deploy, new region, stale allowlist |
| `__none__` | absent from this span | routine (a tool span has no model) |

Those two sentinels started as one, and separating them is what makes
`__other__` worth alerting on. Verified against a planted 25-span shadow deploy
at 0.02% of traffic: the panel goes red and names the model.

**Hot/cold lifecycle** ([design](docs/coldtier.md)). Raw spans live 48h in
ClickHouse; 1m rollups 7 days; 1h rollups 90 days; Parquet indefinitely. The
exporter is the one job whose failure is permanent, so its watermark advances
only after a written file is read back and verified against the source on both
row count and values.

## Layout

| Path | Contains |
| --- | --- |
| `deploy/` | Compose file, otelcol, ClickHouse, Grafana provisioning |
| `schemas/clickhouse/` | Versioned DDL + `dimensions.yaml` (the cardinality contract) |
| `src/telemetry_engine/cardinality/` | Dimension registry and guard |
| `src/telemetry_engine/emitters/` | Mock endpoints, workload generation, OTLP |
| `src/telemetry_engine/ingest/` | Topics, consumer-lag SLI, latency measurement |
| `src/telemetry_engine/storage/` | ClickHouse client, migrations, rollup backfill |
| `src/telemetry_engine/coldtier/` | Parquet layout, verified export, DuckDB queries |
| `src/telemetry_engine/experiments/` | The graded backpressure experiment |
| `scripts/` | Demo, calibration, dashboard verifiers, guard audit |
| `docs/` | Architecture, backpressure, cold tier, runbook |

## Documentation

| Document | Covers |
| --- | --- |
| [docs/architecture.md](docs/architecture.md) | Design, data model, why each tier exists |
| [docs/backpressure.md](docs/backpressure.md) | The measured experiment and the four invalid runs |
| [docs/coldtier.md](docs/coldtier.md) | Parquet layout, at-least-once, export verification |
| [docs/runbook.md](docs/runbook.md) | Operating it: symptoms, diagnosis, recovery |
