# Backpressure characterization

**What this measures:** whether the pipeline can absorb its consumer falling over
without pushing back on the inference endpoints it observes — the claim ADR-003
is built on.

**Result:** yes, with a measured cost. During a 68-second ClickHouse stall at
~6.2k spans/s, consumer lag grew to 135,447 messages and drained in 34.2s once
the consumer resumed. Endpoint p99 moved from 28.4 ms to 32.6 ms. Nothing was
lost between the collector and ClickHouse; the collector shed 16,541 spans
(2.7%) at its own non-blocking export queue, on purpose.

Raw data: [`backpressure-run.json`](backpressure-run.json). Reproduce with
`telemetry-engine backpressure`.

---

## Why the experiment stalls the consumer instead of over-driving the load

The original design was "burst above capacity and watch lag grow". Calibration
([`scripts/calibrate_generator.py`](../scripts/calibrate_generator.py)) showed
that is not achievable here:

| Target | Workers | Generated | Delivered | Efficiency | Lag after |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 5,000/s | 4 | 105,990 | 105,990 | 100% | 0 |
| 8,000/s | 6 | 169,604 | 169,604 | 100% | 0 |
| 12,000/s | 6 | 234,242 | 234,242 | 100% | 0 |
| 16,000/s | 6 | 248,854 | 248,854 | 100% | 0 |
| 16,000/s | 8 | 275,124 | 255,238 | 93% | 0 |

**ClickHouse consumed everything the generator could deliver, with lag back at 0
after every run.** Pushing harder does not create backpressure — it makes the
generator the bottleneck and produces a number about the wrong component.

So overload is induced the way ADR-003 actually describes it: by stalling the
consumer. `docker pause` freezes ClickHouse without closing connections, which
is precisely the "ClickHouse cannot keep up" condition the design claims to
survive.

This also corrects ADR-009's 20k burst target: **20k spans/s is reachable as
generated load but not as delivered load on this hardware.** Phase 2 reported
19.7k spans/s, and that number was true about span *creation* — it was never
verified as delivery, and it is not.

---

## Method

Three phases, with a real HTTP endpoint probed at 10 rps throughout:

| Phase | Duration | What happens |
| --- | --- | --- |
| Baseline | 67.7 s | 5k spans/s, ClickHouse consuming normally |
| Stalled burst | 67.5 s | 6k spans/s with ClickHouse **paused** |
| Recovery | 180 s | ClickHouse resumed, load stopped |

The endpoint runs as a real subprocess and is probed over HTTP because the claim
under test is about what an observed service experiences. The load generator
never makes an HTTP request, so without the probe, "endpoints are unaffected"
would be an assertion about something the experiment never exercised.

A run must start from a quiesced pipeline: lag drained and no consumer rebalance
for 20 s. Otherwise the previous run's recovery bleeds into the next run's
baseline.

---

## Results

### Lag grows and drains

```
  t=   0.0s  lag=         0   baseline
  t=  15.2s  lag=     4,640   baseline
  t=  60.2s  lag=    17,454   baseline
  t=  75.6s  lag=    12,660   STALLED
  t=  90.7s  lag=    39,988   STALLED
  t= 105.9s  lag=    70,186   STALLED
  t= 120.0s  lag=   100,407   STALLED
  t= 135.1s  lag=   128,291   STALLED   <- peak 135,447
  t= 150.2s  lag=   123,247   recovering
  t= 165.4s  lag=    25,070   recovering
  t= 180.5s  lag=        14   recovering
  t= 195.6s  lag=         0   recovering
```

Lag climbs linearly under the stall at roughly 2,000 messages/s, then drains in
**34.2 s** — faster than it accumulated, because ClickHouse consumes faster than
the generator produces once it is running again. That asymmetry is the property
that makes the design work: a stall of bounded length is always recoverable, and
Redpanda's 6-hour retention sets the bound on how long a stall can last before
lag becomes loss.

Note the baseline is not flat: lag reached 17,454 before the stall even began.
At 6.2k spans/s with the full harness running (4 load workers, a live endpoint,
a probe client, and the whole stack on one laptop), ClickHouse runs slightly
behind and catches up. That is honest to report and does not affect the result.

### The endpoint is not backpressured

Measured over 1,231 real HTTP requests:

| Phase | n | p50 | p95 | p99 | max |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline | 623 | 7.9 ms | 16.4 ms | 28.4 ms | 78 ms |
| **Stalled burst** | 608 | **6.1 ms** | **17.7 ms** | **32.6 ms** | 1,608 ms |

**p99 moved 28.4 → 32.6 ms (+15%) while 135,447 messages of lag accumulated
behind it.** p50 was marginally *lower* during the burst. The endpoint had no
idea its telemetry consumer was frozen, which is the entire point of ADR-003.

The 1.6 s max during the burst is one outlier out of 608 requests. It is
reported rather than trimmed; a single-digit-millisecond p50 with a
1.6 s max means one request stalled, most likely on a GC pause or an export
handoff, not sustained pressure.

### Span accounting

Every span is attributed to a stage:

| Stage | Spans |
| --- | ---: |
| Emitted (generator + endpoint) | 607,902 |
| Accepted by collector | 607,902 (100%) |
| Sent to Redpanda | 591,361 |
| **Dropped at the collector's export queue** | **16,541 (2.7%)** |
| Refused by memory_limiter | 0 |
| Lost in the SDK's own queue | 0 |

The 16,541 dropped spans are the design working as specified: the export queue
is bounded and non-blocking, so on overflow it drops and counts rather than
pushing back toward the endpoint. **The pipeline lost 2.7% of telemetry, on
purpose, and knows exactly how much.**

ClickHouse row count grew by 620,056 against 591,361 sent, i.e. more rows landed
than this run produced. The row delta is a coarse measure — anything else
arriving in the window counts toward it — so it establishes that nothing went
missing, not an exact equality.

---

## What would have made this report success while being wrong

Eight failure modes were written down before the experiment was built, each as a
check that **invalidates the run** rather than warning. This was not
precautionary theatre: **the first four runs all reported INVALID, and their
numbers were all wrong.**

| Check | What it caught |
| --- | --- |
| `generator_hit_target` | Generator falling short, leaving lag flat |
| `load_actually_delivered` | **Run 1: 94.8% of spans "created" while the SDK discarded most before the collector saw them.** The pipeline was loaded at a fraction of the claimed rate. |
| `no_counter_reset` | **Run 4: a single scrape timeout returned a zeroed snapshot, which is indistinguishable from every counter resetting.** |
| `sampling_resolution` | A sampling interval too coarse to see the peak |
| `burst_actually_stressed` | A "burst" that never built meaningful lag |
| `spans_accounted` | Loss upstream of Redpanda, which leaves nothing to lag on |
| `endpoint_probed_under_load` | **Run 2: 0 baseline probes** — the probe clock was `perf_counter` while the run origin was `monotonic`, so every timestamp was a difference between unrelated epochs. |
| `lag_reader_is_noninvasive` | **Run 3: the lag reader was joining ClickHouse's consumer group** and causing the rebalances it then measured. |

Four real bugs, each of which would have produced a confident, quotable, wrong
number. Three were in the measurement apparatus rather than the pipeline.

### The pattern

Every one of these is the same shape, and it is the third time this project has
hit it:

- **ADR-015** — a cardinality bucket that was bounded (so the check passed) but
  meaningless, because routine data and the actionable signal shared it.
- **Phase 3 latency** — a window that ran and returned numbers, but measured
  backlog replay rather than steady state.
- **ADR-018** — dashboard SQL that was valid against ClickHouse while every
  panel failed through Grafana.

In each case *a mechanism ran and looked fine while not checking the layer where
the failure lived*. The countermeasure that works is not more checks; it is
asking, before trusting any measurement, **what would make this report success
while being wrong** — and then making the wrong version unavailable rather than
documented. `ingest/latency.py` raises if a latency window filters on ingest
time. `read_collector` returns `None` instead of zeros. The lag reader cannot
join the group it measures. None of those are things anyone has to remember.

---

## Operational conclusions

1. **Consumer lag is the SLI.** It moves first and by orders of magnitude; drop
   count and endpoint latency barely move at all.
2. **Redpanda retention sets the outage budget.** Lag accumulated at ~2,000
   msg/s under a full stall. At 6 hours of retention that is a stall of many
   hours before lag becomes loss — far beyond any realistic restart.
3. **The collector's memory limit is a real constraint, and it sheds correctly.**
   At 768 MiB it began refusing at the receiver at ~6k spans/s. Raised to
   1536 MiB with the export queue bounded to 500 batches, refusals went to zero.
   Both behaviours are correct; the first was simply the wrong component
   saturating first for the experiment's purposes.
4. **Drops are concentrated at the export queue during flush.** The 2.7% loss
   arrives when the generator shuts down and flushes, overflowing a deliberately
   small queue. Enlarging it trades memory for fewer drops.
5. **20k spans/s is not a delivered-load target on this hardware.** ~12.4k is,
   at 100% delivery. ADR-009's number stands as a *generation* figure only.
