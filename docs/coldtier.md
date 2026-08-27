# Cold tier

The hot tier drops raw spans after 48 hours. The cold tier is where anything
that should outlive that goes: hive-partitioned Parquet on disk, queried on
demand by DuckDB, with no standing process.

**Measured:** 5,745,432 rows across 5 files, 276.6 MiB (50.5 bytes/row) —
against 43 MiB for 349k rows in ClickHouse, so roughly comparable per-row cost
with none of the standing RAM. Full-scan aggregates run in ~110 ms, tenant-
scoped queries in ~55 ms.

```bash
telemetry-engine cold export     # ClickHouse -> Parquet, since the watermark
telemetry-engine cold status     # coverage, and whether the TTL is winning
telemetry-engine cold verify     # readable, complete, and actually sorted
telemetry-engine cold query      # DuckDB against the lake
```

## Layout

```
data/cold/dt=2026-08-27/spans-20260827T150000-20260827T160000.parquet
```

`dt` is the only partition key. `hour` is a column. Rows are sorted by
`(tenant_id, ts)` within each file, in 128k-row row groups.

Partitioning by tenant would have been the obvious choice and is the wrong one:
with a zipfian tail of mostly-idle tenants it multiplies file count by tenant
cardinality and produces thousands of tiny files. Sorting by tenant instead
gives DuckDB per-row-group min/max statistics, so a tenant filter skips row
groups without opening them — the same pruning, without the file explosion.

**The sort is load-bearing.** An unsorted lake returns correct answers while
reading every byte of every query, and nothing surfaces except a slow dashboard
nobody attributes to the layout. `cold verify` checks it explicitly.

## Why the failure mode here is different

Every other tier fails temporarily. This one fails *permanently*: a window the
exporter skips is deleted from ClickHouse on schedule and stops existing. That
shapes the design more than performance does.

- **The watermark advances only after verification.** Rows go to a staging
  file, get read back, counted, and compared against ClickHouse on both row
  count *and* aggregates. Only then is the file renamed into place and the
  watermark moved.
- **Verification compares values, not just counts.** A row count matches
  whether or not the columns line up. The fingerprint sums three numeric
  columns and counts distinct values in two identifier columns, computed by
  DuckDB — a different engine than the one that wrote the data.
- **A failed window halts the run** and leaves the watermark behind it. Carrying
  on would advance past a gap and turn a retryable error into permanent loss.
  Observed working: a window failed mid-run, the watermark stayed at 19:00, and
  the retry filled it rather than skipping it.
- **Filenames are deterministic**, so a retry overwrites rather than adds a
  second copy. Duplicates in a lake are much harder to notice than gaps.
- **Windows never cross midnight**, so each file belongs to exactly one
  partition.

## The pipeline is at-least-once, not exactly-once

The lake contains **281,437 duplicate spans (4.9%)** — same `span_id`, same
event timestamp, different `ingested_at`. These are not an export bug. ClickHouse's
Kafka engine redelivers messages when a consumer is interrupted, which is
exactly what happened when the consumer was paused and resumed during the
backpressure experiment.

Two views are offered rather than silently picking one:

| View | Contains | Use for |
| --- | --- | --- |
| `spans` | every row as ingested | pipeline questions ("what did we actually store?") |
| `spans_deduped` | one row per `span_id` | analytics ("how many requests did this tenant make?") |

Deduplicating by default would hide a real property of the system; not offering
the deduplicated view would make every analytical query quietly wrong. The size
of the duplication is reported by `cold status` so it stays visible.

Deduplication is *not* done at export time, deliberately: it needs to track
every `span_id` in the window, and ClickHouse on this node is memory-constrained
enough that it already spills the sort to disk. Paying that cost once at query
time in DuckDB is cheaper than risking the export.

## Operational notes

**The export must yield to ingest.** It is capped at 400 MB
(`max_memory_usage`) against the server's 1.5 GiB. Before that cap, an export
took roughly 1 GiB and left too little for the Kafka materialized view, which
hit the server limit mid-parse and shed 61 messages into the dead-letter table.
A background job degraded live ingest. The dead-letter design worked exactly as
ADR-012 intended — the consumer shed and recovered rather than stalling — but
the right fix is for the batch job not to be greedy.

**`cold status` flags two distinct risks:**

- *AT RISK* — the watermark is more than 75% of the TTL behind now. Unexported
  data is approaching deletion.
- *INCONSISTENT* — the watermark claims coverage but the lake is empty. Happens
  when the lake is deleted or restored from an older backup while the
  bookkeeping stays put. Both components are individually correct; together
  they would skip those windows forever. Recover with
  `cold export --since '<timestamp>'`, which is the only way to move a watermark
  backwards (the table is `ReplacingMergeTree(watermark)`, so it cannot regress
  by accident).

## Bugs this phase surfaced

The pre-mortem asked what would make the cold tier look correct while being
wrong. Five of the seven answers turned out to be real:

| Bug | Would have looked like |
| --- | --- |
| **Watermark ties.** `ReplacingMergeTree(updated_at)` with second-resolution `DateTime` — an export advances the watermark several times per second, so the version column tied and `FINAL` returned an arbitrary row. Observed: an export advanced to 20:04:30 and the table reported 14:34:30. | Silent re-export at best; a skipped window at worst. Fixed by versioning on `watermark` itself, which cannot tie because it only moves forward. |
| **Timezone asymmetry.** The client returns `DateTime` as naive UTC but `insert()` treats a naive datetime as *local*, so every watermark was stored 5.5 hours in the past. SELECT parameter binding does *not* do this, which is why the exported windows were always correct and only the bookkeeping drifted. | A watermark permanently behind reality, and a spurious TTL alarm. |
| **Parquet stamped with the exporter's timezone.** Arrow batches carried `Asia/Calcutta` on timestamp columns, so the archive recorded whichever laptop wrote it and every value shifted on read. | A filter on a known window returning zero rows. |
| **Window planning floored to the hour.** A watermark at 14:58 re-planned from 14:00, re-exporting 58 minutes under a *differently named* file. Deterministic filenames only prevent duplication when the window is identical. | Overlapping files, duplicate rows, every total quietly too large. Caught by a unit test. |
| **Verification written but never called.** `_source_fingerprint` was implemented and then not wired into the check it existed for. | Exactly the failure it was written to prevent. |

The first four are all the same shape as the bugs in earlier phases: a mechanism
that ran, returned plausible output, and was not measuring the thing it was
supposed to measure. The fifth is worse and simpler — a guard that exists in the
source and never executes.
