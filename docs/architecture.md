# Architecture

How the pipeline is put together, and why each piece is the shape it is.
Decisions are recorded as ADRs in `decisions.md`; this document is the map.

## The constraint everything follows from

Observability infrastructure sits next to the thing it observes. If it ever
applies pressure back toward that thing — by blocking, by consuming its CPU, by
holding its request thread — it has stopped being observability and started
being an outage. That single constraint decides most of what follows: why the
export queue drops instead of blocking, why ClickHouse consumes Kafka directly,
why the cold-tier export is memory-capped, and why lag rather than loss is the
primary health signal.

## Data flow

```
mock endpoints ──OTLP──> collector ──> Redpanda ──> ClickHouse ──> Grafana
                            │                          │
                       drop + count              Parquet + DuckDB
```

| Stage | Component | Responsibility |
| --- | --- | --- |
| Emit | `emitters/` | Realistic LLM/agent spans: tokens, KV-cache, TTFT, trace trees |
| Collect | OTel Collector | Shed under memory pressure; batch; produce to Kafka, never block |
| Buffer | Redpanda | Absorb bursts and consumer stalls on disk (6h retention) |
| Ingest | ClickHouse Kafka engine | Consume directly; parse OTLP JSON in a materialized view |
| Store hot | `spans_raw` | One row per span, 48h TTL, high cardinality, never grouped by |
| Aggregate | `spans_1m` / `spans_1h` | Bounded-cardinality rollups; what dashboards read |
| Store cold | Parquet + DuckDB | Detail that outlives the TTL; costs disk, not RAM |

### Why no consumer microservice

ClickHouse's Kafka table engine reads Redpanda directly (ADR-002). The
conventional shape — Kafka → consumer service → OLAP store — is one more
container, one more deploy unit, one more thing to be down at 3am. The cost is
that transformation must be expressible in a materialized view's SELECT, and
consumer tuning happens through ClickHouse settings rather than application
code.

That cost has a sharp edge worth knowing: **if the parsing view raises, Kafka
consumption stalls for the entire consumer group.** Every extraction in
`030_mv_spans_raw.sql` is therefore total — `toUInt32OrZero`, map lookups that
yield empty strings — so a malformed span produces a row with empty fields
rather than halting ingestion for everyone. A unit test greps for any bare
`toUInt`/`toFloat` that sneaks in.

## The three tiers

| Tier | Table | Retention | Cardinality | Purpose |
| --- | --- | --- | --- | --- |
| Raw | `spans_raw` | 48 hours | Unbounded | Trace debugging, cold-tier source |
| Rollup | `spans_1m` | 7 days | Bounded by budget | Dashboards, recent analysis |
| Rollup | `spans_1h` | 90 days | Bounded by budget | Trend analysis |
| Cold | Parquet | Indefinite | Unbounded | History past the TTL |

`spans_1h` is built from `spans_1m` via `-MergeState`, not from raw. That is the
point of storing aggregate *states* rather than finished numbers: hourly figures
cost a grouped pass over a small table, and keep working after raw data has
aged out.

### Facts and dimensions are separate (ADR-006)

The rule that keeps the rollups small:

- **Unbounded identifiers** — `trace_id`, `span_id`, `request_id`, prompt hash —
  live only in `spans_raw`, under a short TTL, and are never in a `GROUP BY`.
- **Bounded dimensions** — tenant, tier, model, operation, route, region, status
  class — are the only things a rollup may key on, and each has a declared
  budget in `schemas/clickhouse/dimensions.yaml`.

A rollup's row count is the product of its dimensions' cardinalities. One
unbounded dimension turns a compact aggregate into something larger than the raw
data it summarises.

### The cardinality guard (ADR-005, ADR-014, ADR-015)

`dimensions.yaml` is the contract. `telemetry-engine dimensions apply` syncs it
into a ClickHouse table backed by a `COMPLEX_KEY_HASHED` dictionary, and the
rollup materialized view calls `dictHas` per dimension per row. Enforcement is
in ClickHouse because there is no application process on the ingest path — and
because a guard that can be bypassed is not a guard.

Each dimension resolves to one of three things:

| Result | When | Meaning |
| --- | --- | --- |
| the value | registered | normal |
| `__other__` | present, unregistered | **actionable** — shadow deploy, new region, stale allowlist |
| `__none__` | absent from this span | routine — a tool span has no model |

Tenant is the interesting case. Tenants cannot be enumerated ahead of time, so
the allowlist is the **top-K by recent volume** (budget 200). With zipfian
traffic the top 200 cover essentially everything; the long tail aggregates into
`__other__` and was never going to be read per-tenant anyway.

The bound is `product(budget + 2)` per time bucket. That is a true ceiling and a
loose one — dimensions correlate heavily in practice, and observed is ~19k rows
for 350k spans. The claim worth making is that cardinality is *finite and
chosen*, not that the product is a useful estimate.

## Backpressure, tier by tier (ADR-003)

Nothing blocks upstream. Four layers, each with a defined shedding behaviour:

1. **Endpoint → Collector.** OTLP export is off the request path; the SDK hands
   spans to a background thread.
2. **Inside the collector.** `memory_limiter` is first in every pipeline, so it
   refuses at the receiver under pressure rather than being OOM-killed.
3. **Collector → Redpanda.** The export queue is bounded and **non-blocking**:
   on overflow it drops and increments `otelcol_exporter_send_failed_spans`. A
   dropped span is cheap; a blocked inference request is an incident.
4. **Redpanda → ClickHouse.** Disk-backed retention means a stalled consumer
   produces *lag*, not loss. Lag is the primary SLI.

Measured under a deliberate 68-second ClickHouse stall: lag grew to 135,447
messages and drained in 34.2s, while endpoint p99 moved 28.4 → 32.6 ms. Full
method in [backpressure.md](backpressure.md).

Redpanda's partition key is `trace_id`, not `tenant_id` (ADR-004). Keying by
tenant gives per-tenant ordering but hands one hot tenant an entire partition;
trace_id spreads evenly while keeping a trace's spans co-partitioned.

## Hot/cold boundary

The exporter is the only job whose failure is permanent — a window it skips is
deleted by the TTL and stops existing. So the watermark advances only after a
written file has been read back and verified against the source on both row
count *and* a value fingerprint computed by a different engine. A failed window
halts the run and leaves the watermark behind it. Details, and the at-least-once
duplication the lake made visible, in [coldtier.md](coldtier.md).

## Resource budget

Everything runs on a laptop alongside an IDE and a browser.

| Container | Limit | Notes |
| --- | --- | --- |
| ClickHouse | 2 GiB | 0.75 ratio for queries; small merge pool with matched mutation thresholds |
| Collector | 1.5 GiB | Raised from 768 MiB after it, not ClickHouse, saturated first |
| Redpanda | 1.5 GiB | `--smp 1 --overprovisioned`: yields CPU instead of busy-polling |
| Grafana | 512 MiB | |

Idle footprint is ~540 MiB. The per-query caps matter as much as the container
limits: a `GROUP BY` producing more than 5M keys is rejected rather than
allowed to OOM the node and take ingest down with it. A dashboard panel failing
loudly is a much better outcome than a restarted server.

## Where the interesting decisions are

| Topic | ADRs |
| --- | --- |
| Lean stack, no consumer service | 001, 002, 008 |
| Backpressure and shedding | 003, 026 |
| Partitioning and keys | 004, 007, 013 |
| Cardinality and the guard | 005, 006, 014, 015, 019 |
| Ingest robustness | 011, 012 |
| Rollups and backfill | 016 |
| Dashboards | 017, 018, 019 |
| Measurement honesty | 020, 021, 022 |
| Cold tier | 023, 024, 025 |
| Verification that actually runs | 027, 028, 029 |
