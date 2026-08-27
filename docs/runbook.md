# Runbook

Symptom → diagnosis → fix. Every command here is safe to run against a live
stack unless marked otherwise.

## First response

```bash
python tasks.py ps                  # are all four containers healthy?
telemetry-engine stack wait         # is everything actually accepting work?
telemetry-engine monitor --duration 20   # lag and drop counters, live
telemetry-engine cold status        # is the exporter ahead of the TTL?
telemetry-engine dimensions status  # is anything unregistered emitting?
```

The three dashboards at http://localhost:3000 answer most of this visually.
**Pipeline Health** is the one to open first.

---

## Consumer lag is growing

**What it means.** ClickHouse is not keeping up with Redpanda. By design this is
*not* data loss: Redpanda holds 6 hours, so lag is a budget being spent, not
data gone.

```bash
docker exec te-redpanda rpk group describe clickhouse-spans
```

| Check | Command | If bad |
| --- | --- | --- |
| Is the consumer erroring? | `SELECT consumer_id, arrayStringConcat(\`exceptions.text\`, ' \| ') FROM system.kafka_consumers WHERE table='kafka_spans'` | A raising materialized view stalls the whole group — see below |
| Is it committing? | same table, `last_commit_time` | Reading but not committing means it is failing inside the MV |
| Are all 6 partitions assigned? | `length(assignments.partition_id)` per consumer | An unassigned partition accumulates lag forever while the rest look fine |
| Is ClickHouse memory-starved? | `docker stats te-clickhouse` | A concurrent export or heavy query is competing with ingest |

**Most common cause in practice:** a background job competing for ClickHouse
memory. The cold-tier export is capped at 400 MB for exactly this reason
(ADR-026); an uncapped one took two thirds of the server budget and pushed the
Kafka view into shedding.

**Recovery.** Lag drains on its own once the pressure is gone — measured at
34.2s from a 135k-message peak. If it does not, the consumer is stuck, not slow.

---

## Messages are landing in the dead-letter table

```sql
SELECT count(), any(error) FROM telemetry.spans_ingest_errors;
```

**What it means.** The Kafka engine could not process a message and routed it
aside instead of stalling the partition (ADR-012). This table *should* stay
empty — `JSONAsString` accepts any byte sequence, so arrivals here are usually
resource errors, not malformed data.

If the error mentions memory: something is competing with ingest. Find it, cap
it, and clear the table so the "should be empty" signal means something again:

```sql
TRUNCATE TABLE telemetry.spans_ingest_errors;
```

---

## Dropped spans are non-zero

**This is not necessarily a fault.** The export queue is deliberately
non-blocking: under sustained overload it drops and counts rather than pushing
back on the endpoint. The design accepts loss; what it does not accept is
*silent* loss.

```bash
curl -s localhost:8888/metrics | grep -E "send_failed|enqueue_failed|refused"
```

| Counter | Meaning |
| --- | --- |
| `send_failed_spans` | Export exhausted its retries |
| `enqueue_failed_spans` | The bounded queue rejected a batch outright |
| `refused_spans` | `memory_limiter` shedding at the receiver |

Refusals mean the collector is memory-bound; drops mean the path to Redpanda is
the bottleneck. They call for different fixes.

---

## `__other__` appeared on the cardinality dashboard

**What it means.** Something is emitting a dimension value nobody registered:
a shadow deployment, a new region, or an allowlist that has gone stale. Any
non-zero value is worth looking at — the panel is an absolute count with a red
threshold at 1 precisely so a small number cannot be diluted (ADR-019).

The **"Which values are unregistered?"** panel names the offender. Then either:

```bash
# The value is legitimate: register it and re-sync.
$EDITOR schemas/clickhouse/dimensions.yaml
telemetry-engine dimensions apply

# Or it is a tenant that grew: refresh the top-K from traffic.
telemetry-engine dimensions apply
```

`__none__` is *not* this. It means the dimension does not apply to that span
kind — a tool span has no model — and is routine.

---

## Rollups disagree with raw

```bash
telemetry-engine rollup-status
```

**Most likely cause:** materialized views do not backfill. Rows ingested before
a view existed are absent from it, which shows up as a gap exactly the size of
the pre-existing data.

```bash
telemetry-engine rollup-backfill --hours 6
```

Backfill fills the window immediately *before* the rollup's earliest row, so it
cannot overlap what the view already covers. **Do not run overlapping windows:**
aggregate states merge rather than replace, so an overlap silently double-counts.

---

## The cold tier is behind, or empty

```bash
telemetry-engine cold status
```

Two distinct alarms:

| Alarm | Meaning | Fix |
| --- | --- | --- |
| **AT RISK** | Watermark is >75% of the TTL behind. Unexported data is approaching deletion. | `telemetry-engine cold export` |
| **INCONSISTENT** | Watermark claims coverage but the lake is empty — deleted or restored from an older backup. Those windows would never be re-exported. | `telemetry-engine cold export --since '<timestamp>'` |

`--since` is the only way to move a watermark backwards; the table is
`ReplacingMergeTree(watermark)` so it cannot regress by accident (ADR-023).
Re-exporting is safe — filenames are deterministic, so a retry overwrites.

If an export halts partway, that is the design working: the watermark stays
behind the failed window and a retry fills the gap rather than skipping it.

---

## A dashboard panel is empty

Empty is ambiguous — it can mean "no data in this window" or "this query is
broken". Distinguish them:

```bash
python scripts/verify_dashboards.py   # does the SQL run against ClickHouse?
python scripts/verify_grafana.py      # does the panel work through Grafana?
```

**Both are needed.** SQL validity and panel correctness are different
properties: the dashboards once passed the first and failed the second for every
panel, because Grafana's global time variables are interpolated frontend-only
(ADR-018).

If a panel was edited in the browser, it will be overwritten — the JSON files
are the source of truth. Edit `src/telemetry_engine/dashboards.py` and run
`telemetry-engine dashboards`.

---

## Before trusting any measurement

This pipeline has produced confidently wrong numbers more than once. Before
quoting one:

- **Backpressure figures** — only from a run that printed `RUN VALID`. A run
  failing any pre-registered check prints `RUN INVALID` and exits non-zero; four
  consecutive early runs did, and all four were wrong (ADR-021).
- **Latency** — use `ingest/latency.py`. It raises on a window filtered by
  ingest time rather than event time, and refuses to measure while a backlog is
  draining. Both mistakes inflated an early figure roughly threefold.
- **Throughput** — spans *created* is not spans *delivered*. Check
  `otelcol_receiver_accepted_spans`, not the generator's own count (ADR-020).
- **Cold-tier totals** — decide whether the question is about the pipeline
  (`spans`) or about requests (`spans_deduped`). The lake is at-least-once and
  the two differ by ~5% (ADR-025).

---

## Full reset

```bash
python tasks.py nuke     # DESTRUCTIVE: stops the stack and deletes all volumes
rm -rf data/cold         # DESTRUCTIVE: deletes the Parquet lake
python tasks.py demo     # rebuild from nothing, ~2 minutes
```

`nuke` removes ClickHouse data, Redpanda's log, and Grafana state. It does not
touch the Parquet lake, which is why the second command is separate — and why
`cold status` warns when the watermark and the lake disagree.
