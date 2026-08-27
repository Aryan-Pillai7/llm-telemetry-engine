"""Operator entrypoint for the telemetry engine.

Subcommands are added as each phase lands. Everything an operator does to this
pipeline should be reachable from here rather than from a pile of loose scripts.
"""

from __future__ import annotations

import json
from pathlib import Path

import typer

from telemetry_engine import __version__
from telemetry_engine.common.logging import configure_logging, get_logger
from telemetry_engine.config import get_settings

app = typer.Typer(
    name="telemetry-engine",
    help="LLM & agent telemetry analytics engine.",
    no_args_is_help=True,
    add_completion=False,
)
topics_app = typer.Typer(help="Manage Redpanda topics.", no_args_is_help=True)
app.add_typer(topics_app, name="topics")
stack_app = typer.Typer(help="Inspect the local docker stack.", no_args_is_help=True)
app.add_typer(stack_app, name="stack")
dims_app = typer.Typer(help="Manage the cardinality allowlist.", no_args_is_help=True)
app.add_typer(dims_app, name="dimensions")
cold_app = typer.Typer(help="Parquet cold tier.", no_args_is_help=True)
app.add_typer(cold_app, name="cold")

log = get_logger(__name__)


@app.command()
def version() -> None:
    """Print the package version."""
    typer.echo(__version__)


@app.command("config")
def show_config() -> None:
    """Print the effective, env-resolved configuration.

    First stop when the pipeline talks to the wrong broker or database.
    """
    settings = get_settings()
    typer.echo(json.dumps(json.loads(settings.model_dump_json()), indent=2, default=str))


@app.command()
def migrate(
    dry_run: bool = typer.Option(False, "--dry-run", help="Report pending work, change nothing."),
    wait: bool = typer.Option(True, help="Wait for ClickHouse to accept connections first."),
) -> None:
    """Apply pending ClickHouse migrations from schemas/clickhouse."""
    from telemetry_engine.storage.client import wait_until_ready
    from telemetry_engine.storage.migrations import apply_all

    settings = get_settings()
    configure_logging(settings.log_level)

    if wait:
        wait_until_ready(settings.clickhouse)

    result = apply_all(settings, dry_run=dry_run)
    verb = "pending" if dry_run else "applied"

    for name in result.applied:
        typer.echo(f"  {verb}: {name}")
    for name in result.skipped:
        typer.echo(f"  up to date: {name}")
    for name in result.drifted:
        typer.secho(f"  DRIFT: {name} changed after being applied", fg=typer.colors.YELLOW)

    if result.drifted:
        # Loud, but not fatal: the operator has to decide whether the edit was
        # an idempotent fix or should have been a new numbered migration.
        typer.secho(
            "one or more applied migrations were edited; review before relying on this schema",
            fg=typer.colors.YELLOW,
        )
    if not result.applied and not dry_run:
        typer.echo("  nothing to do")


@topics_app.command("apply")
def topics_apply(
    dry_run: bool = typer.Option(False, "--dry-run", help="Report changes, change nothing."),
) -> None:
    """Reconcile Redpanda topics with deploy/redpanda/topics.yaml."""
    from telemetry_engine.ingest.topics import reconcile

    settings = get_settings()
    configure_logging(settings.log_level)

    result = reconcile(settings=settings.redpanda, dry_run=dry_run)

    for name in result.created:
        typer.echo(f"  created: {name}")
    for name in result.partitions_grown:
        typer.echo(f"  partitions grown: {name}")
    for name in result.configs_updated:
        typer.echo(f"  config updated: {name}")
    for name in result.unchanged:
        typer.echo(f"  up to date: {name}")
    for name in result.refused:
        typer.secho(f"  REFUSED (cannot shrink partitions): {name}", fg=typer.colors.YELLOW)

    if not result.changed:
        typer.echo("  nothing to do")


@topics_app.command("list")
def topics_list() -> None:
    """Show topics as they currently exist on the broker."""
    from confluent_kafka.admin import AdminClient

    settings = get_settings()
    admin = AdminClient({"bootstrap.servers": settings.redpanda.bootstrap_servers})
    md = admin.list_topics(timeout=30.0)

    for name, topic in sorted(md.topics.items()):
        if topic.error:
            typer.secho(f"  {name}: {topic.error}", fg=typer.colors.RED)
        else:
            typer.echo(f"  {name}: {len(topic.partitions)} partition(s)")


@stack_app.command("wait")
def stack_wait(
    timeout: float = typer.Option(90.0, help="Seconds to wait for each service."),
) -> None:
    """Wait until every service the pipeline talks to is actually ready.

    Compose's `--wait` covers the services with healthchecks. The collector is
    distroless and cannot carry one, so it is probed here from the host.
    """
    from telemetry_engine.common.health import wait_for_http
    from telemetry_engine.storage.client import wait_until_ready

    settings = get_settings()
    configure_logging(settings.log_level)

    wait_until_ready(settings.clickhouse, timeout_s=timeout)
    typer.echo("  clickhouse: ready")

    wait_for_http(
        "http://localhost:13133/",
        predicate=lambda body: "StatusOK" in body or "Server available" in body,
        timeout_s=timeout,
        name="otel collector",
    )
    typer.echo("  otelcol: ready")

    wait_for_http(
        "http://localhost:8888/metrics",
        timeout_s=timeout,
        name="otel collector self-telemetry",
    )
    typer.echo("  otelcol self-telemetry: ready")


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", help="Bind address for the mock endpoint."),
    port: int = typer.Option(8080, help="Port for the mock endpoint."),
    otlp: str = typer.Option("http://localhost:4317", help="Collector OTLP gRPC endpoint."),
) -> None:
    """Run the mock LLM/agent inference endpoint.

    A real instrumented service for demos and manual pokes. For volume, use
    `load` -- HTTP overhead caps this well below the 5k spans/s target.
    """
    import uvicorn

    from telemetry_engine.emitters.endpoint import create_app
    from telemetry_engine.emitters.otlp import ExporterConfig

    settings = get_settings()
    configure_logging(settings.log_level)

    app_instance = create_app(
        exporter=ExporterConfig(endpoint=otlp),
        tenants=settings.workload.tenants,
        zipf_alpha=settings.workload.zipf_alpha,
    )
    uvicorn.run(app_instance, host=host, port=port, log_level=settings.log_level.lower())


@app.command()
def load(
    duration: float = typer.Option(30.0, help="Seconds to generate load for."),
    profile: str = typer.Option("steady", help="Load shape: steady | burst | ramp."),
    rate: int | None = typer.Option(None, help="Override sustained spans/s."),
    burst_rate: int | None = typer.Option(None, help="Override burst spans/s."),
    workers: int | None = typer.Option(None, help="Worker processes (default: auto)."),
    otlp: str = typer.Option("http://localhost:4317", help="Collector OTLP gRPC endpoint."),
    seed: int = typer.Option(1234, help="Workload seed, for reproducible runs."),
) -> None:
    """Drive synthetic telemetry at a target span rate.

    Reports achieved rate against target. A generator that silently misses its
    target turns a backpressure measurement into fiction, so any shortfall is
    printed rather than hidden.
    """
    from telemetry_engine.emitters.load import run_load
    from telemetry_engine.emitters.otlp import ExporterConfig
    from telemetry_engine.emitters.workload import LoadProfile, Profile

    settings = get_settings()
    configure_logging(settings.log_level)

    try:
        shape = Profile(profile.lower())
    except ValueError:
        raise typer.BadParameter(
            f"unknown profile {profile!r}; expected one of: " + ", ".join(p.value for p in Profile)
        ) from None

    load_profile = LoadProfile(
        profile=shape,
        sustained_spans_per_sec=rate or settings.workload.sustained_spans_per_sec,
        burst_spans_per_sec=burst_rate or settings.workload.burst_spans_per_sec,
    )

    typer.echo(
        f"  profile={shape.value} duration={duration}s "
        f"sustained={load_profile.sustained_spans_per_sec}/s "
        f"burst={load_profile.burst_spans_per_sec}/s"
    )

    result = run_load(
        duration_s=duration,
        profile=load_profile,
        exporter=ExporterConfig(endpoint=otlp),
        tenants=settings.workload.tenants,
        zipf_alpha=settings.workload.zipf_alpha,
        seed=seed,
        workers=workers,
    )

    typer.echo(f"  workers:       {result.workers}")
    typer.echo(f"  traces:        {result.traces:,}")
    typer.echo(f"  spans:         {result.spans:,}")
    typer.echo(f"  generating:    {result.elapsed_s:.1f}s")
    typer.echo(f"  final flush:   {result.flush_s:.1f}s")
    typer.echo(f"  achieved rate: {result.achieved_rate:,.0f} spans/s")
    typer.echo(f"  target rate:   {result.target_rate:,.0f} spans/s")

    if result.shortfall_pct > 5.0:
        # The generator, not the pipeline, fell behind. Saying so keeps a
        # measurement honest instead of attributing the gap to ClickHouse.
        typer.secho(
            f"  generator fell {result.shortfall_pct:.1f}% short of its own target "
            f"-- add --workers or lower --rate before drawing conclusions",
            fg=typer.colors.YELLOW,
        )


@dims_app.command("apply")
def dimensions_apply() -> None:
    """Sync dimensions.yaml into the ClickHouse allowlist.

    Static values are written as declared; top-K dimensions (tenants) are
    recomputed from recent traffic. Until this runs, every dimension value
    collapses to `__other__` -- bounded, but useless.
    """
    from telemetry_engine.cardinality.guard import sync
    from telemetry_engine.cardinality.registry import load_registry
    from telemetry_engine.storage.client import client

    settings = get_settings()
    configure_logging(settings.log_level)
    registry = load_registry()

    with client(settings.clickhouse) as conn:
        result = sync(conn, registry)

    typer.echo(f"  static values:  {result.static_values}")
    for name, count in sorted(result.top_k_values.items()):
        budget = registry.by_name[name].budget
        typer.echo(f"  top-K {name}: {count}/{budget}")
    typer.echo(f"  max rollup rows per minute: {registry.max_rows_per_bucket:,}")


@dims_app.command("status")
def dimensions_status(
    table: str = typer.Option("telemetry.spans_1m", help="Rollup table to audit."),
) -> None:
    """Report observed cardinality against budget for every dimension."""
    from telemetry_engine.cardinality.guard import status as guard_status
    from telemetry_engine.cardinality.registry import load_registry
    from telemetry_engine.storage.client import client

    settings = get_settings()
    registry = load_registry()

    with client(settings.clickhouse) as conn:
        rows = guard_status(conn, registry, table=table)

    typer.echo(f"  {'dimension':<14}{'allowed':>9}{'budget':>8}{'distinct':>10}{'__other__':>12}")
    for row in rows:
        flag = "" if row.within_budget else "  OVER BUDGET"
        typer.secho(
            f"  {row.name:<14}{row.allowlisted:>9}{row.budget:>8}"
            f"{row.observed_distinct:>10}{row.other_share:>11.1%}{flag}",
            fg=None if row.within_budget else typer.colors.RED,
        )
    typer.echo("")
    typer.echo(f"  theoretical max rows/minute: {registry.max_rows_per_bucket:,}")


@app.command("rollup-status")
def rollup_status() -> None:
    """Report whether the rollups cover everything in spans_raw.

    Materialized views do not backfill: rows ingested before a view existed are
    absent from it. This is how you find out.
    """
    from telemetry_engine.storage.client import client
    from telemetry_engine.storage.rollups import plan

    settings = get_settings()
    with client(settings.clickhouse) as conn:
        result = plan(conn)

    typer.echo(f"  raw:    {result.raw_rows:,} spans  [{result.raw_min} .. {result.raw_max}]")
    typer.echo(
        f"  1m:     {result.rollup_spans:,} spans  [{result.rollup_min} .. {result.rollup_max}]"
    )
    if result.needs_backfill:
        typer.secho(f"  {result.describe()}", fg=typer.colors.YELLOW)
        typer.echo("  run: telemetry-engine rollup-backfill --hours N")
    else:
        typer.echo("  rollup covers all raw spans")


@app.command("rollup-backfill")
def rollup_backfill(
    hours: float = typer.Option(..., help="Backfill this many hours ending at the rollup start."),
    yes: bool = typer.Option(False, "--yes", help="Skip the confirmation prompt."),
) -> None:
    """Aggregate historical raw spans into spans_1m.

    Backfills the window immediately BEFORE the rollup's current earliest row,
    so it cannot overlap what the view already covers. Overlapping would double
    count: aggregate states merge, they do not replace.
    """
    from datetime import timedelta

    from telemetry_engine.storage.client import client
    from telemetry_engine.storage.rollups import backfill, plan

    settings = get_settings()
    configure_logging(settings.log_level)

    with client(settings.clickhouse) as conn:
        current = plan(conn)
        if not current.needs_backfill:
            typer.echo("  rollup already covers all raw spans; nothing to do")
            return

        # End at the rollup's earliest row so the windows abut without touching.
        end = current.rollup_min or current.raw_max
        start = end - timedelta(hours=hours)
        if current.raw_min and start < current.raw_min:
            start = current.raw_min

        typer.echo(f"  backfilling [{start} .. {end})")
        if not yes:
            typer.confirm("  proceed?", abort=True)

        aggregated = backfill(conn, start=start, end=end)

    typer.echo(f"  aggregated {aggregated:,} raw spans")


@app.command("dashboards")
def dashboards_build() -> None:
    """Regenerate Grafana dashboard JSON from the builder.

    Dashboards are generated so a panel's query lives next to the reasoning for
    it, and so shared expressions have one definition. Generated files are
    committed; Grafana provisioning picks them up within 30s.
    """
    from telemetry_engine.dashboards import write_all

    for path in write_all():
        typer.echo(f"  wrote {path.relative_to(get_settings().schemas_dir.parents[1])}")


@app.command()
def monitor(
    interval: float = typer.Option(5.0, help="Seconds between samples."),
    duration: float | None = typer.Option(None, help="Stop after this many seconds."),
) -> None:
    """Sample consumer lag and collector counters into telemetry.pipeline_health.

    Run this alongside a load test. Lag is the pipeline's primary SLI: under
    this design's backpressure model, overload appears here before it appears
    anywhere else.
    """
    from telemetry_engine.ingest.lag import monitor as monitor_loop
    from telemetry_engine.storage.client import client

    settings = get_settings()
    configure_logging(settings.log_level)

    typer.echo(f"  sampling every {interval}s (ctrl-c to stop)")
    with client(settings.clickhouse) as conn:
        try:
            for lag, collector in monitor_loop(
                conn, settings, interval_s=interval, duration_s=duration
            ):
                typer.echo(
                    f"  lag={lag.total_lag:>8,}  max_partition={lag.max_partition_lag:>7,}  "
                    f"queue={collector.queue_size}/{collector.queue_capacity}  "
                    f"dropped={collector.dropped_spans:,}"
                )
        except KeyboardInterrupt:
            typer.echo("  stopped")


@cold_app.command("export")
def cold_export(
    max_windows: int | None = typer.Option(None, help="Stop after this many hourly windows."),
) -> None:
    """Export raw spans from ClickHouse into hive-partitioned Parquet.

    Exports complete hourly windows since the watermark. The watermark advances
    only after a file has been written, read back, and verified against the
    source -- a missed window is permanent, because the hot tier drops raw spans
    on a 48-hour TTL.
    """
    from telemetry_engine.coldtier.export import run_export
    from telemetry_engine.storage.client import client

    settings = get_settings()
    configure_logging(settings.log_level)

    with client(settings.clickhouse) as conn:
        result = run_export(conn, settings, max_windows=max_windows)

    for window in result.windows:
        if window.skipped:
            typer.echo(f"  skipped {window.window.start}: {window.reason}")
        elif window.ok:
            typer.echo(
                f"  exported {window.window.start} -> {window.written_rows:,} rows, "
                f"{window.bytes_on_disk / 1024 / 1024:.1f} MiB"
            )
        else:
            typer.secho(
                f"  FAILED {window.window.start}: source={window.source_rows:,} "
                f"written={window.written_rows:,} verified={window.verified_rows:,}",
                fg=typer.colors.RED,
            )

    typer.echo(f"  watermark: {result.watermark_before} -> {result.watermark_after}")
    typer.echo(f"  {result.rows:,} rows in {result.files} file(s)")
    if not result.ok:
        typer.secho(
            "  export halted on a failed window; watermark left behind it", fg=typer.colors.RED
        )
        raise typer.Exit(1)


@cold_app.command("status")
def cold_status() -> None:
    """Show lake contents and whether the exporter is keeping ahead of the TTL."""
    from telemetry_engine.coldtier.export import health
    from telemetry_engine.coldtier.query import stats
    from telemetry_engine.storage.client import client

    settings = get_settings()

    with client(settings.clickhouse) as conn:
        export_health = health(conn, settings)

    lake = stats(settings.coldtier.root)
    typer.echo(f"  root:       {settings.coldtier.root}")
    typer.echo(f"  files:      {lake.files} across {lake.partitions} partition(s)")
    typer.echo(f"  rows:       {lake.rows:,}")
    typer.echo(f"  size:       {lake.mib:.1f} MiB ({lake.bytes_per_row:.1f} bytes/row)")
    typer.echo(f"  export:     {export_health.describe()}")

    if export_health.at_risk:
        typer.secho(
            "  AT RISK: unexported data is approaching the hot-tier TTL and will be "
            "deleted from ClickHouse whether or not it was copied",
            fg=typer.colors.RED,
        )


@cold_app.command("verify")
def cold_verify() -> None:
    """Check the lake is readable, complete, and actually sorted.

    Sorting is what makes the coarse dt-only partitioning safe. An unsorted lake
    returns correct answers while reading far more data than it should, which
    surfaces as nothing at all.
    """
    from telemetry_engine.coldtier.query import ColdTierMismatchError, open_lake, verify_sorted

    settings = get_settings()
    root = settings.coldtier.root

    try:
        with open_lake(root) as conn:
            rows = conn.execute("SELECT count(*) FROM spans").fetchone()[0]
        typer.echo(f"  readable:   yes ({rows:,} rows)")
    except ColdTierMismatchError as exc:
        typer.secho(f"  MISMATCH: {exc}", fg=typer.colors.RED)
        raise typer.Exit(1) from exc

    problems = verify_sorted(root)
    if problems:
        for problem in problems:
            typer.secho(f"  UNSORTED: {problem}", fg=typer.colors.RED)
        raise typer.Exit(1)
    typer.echo("  sort order: verified")


@cold_app.command("query")
def cold_query(
    sql: str = typer.Argument("", help="SQL against the `spans` view. Omit for a summary."),
) -> None:
    """Query the lake with DuckDB."""
    from telemetry_engine.coldtier import query as coldquery

    settings = get_settings()
    statement = sql or coldquery.DAILY_VOLUME

    with coldquery.open_lake(settings.coldtier.root) as conn:
        cursor = conn.execute(statement)
        columns = [d[0] for d in cursor.description]
        rows = cursor.fetchall()

    typer.echo("  " + " | ".join(columns))
    for row in rows[:50]:
        typer.echo("  " + " | ".join(str(v) for v in row))
    if len(rows) > 50:
        typer.echo(f"  ... {len(rows) - 50} more rows")


@app.command("backpressure")
def backpressure(
    baseline: float = typer.Option(45.0, help="Seconds of sustained-rate baseline."),
    burst: float = typer.Option(45.0, help="Seconds of burst above capacity."),
    recovery: float = typer.Option(120.0, help="Seconds to observe recovery after the burst."),
    rate: int | None = typer.Option(None, help="Sustained spans/s."),
    burst_rate: int | None = typer.Option(None, help="Burst spans/s."),
    workers: int = typer.Option(8, help="Load generator processes."),
    interval: float = typer.Option(1.0, help="Health sampling interval."),
    out: str = typer.Option("docs/backpressure-run.json", help="Where to write results."),
) -> None:
    """Run the backpressure experiment and report whether it can be trusted.

    Drives a baseline, then a burst above the pipeline's comfortable capacity,
    then observes recovery -- while probing a real HTTP endpoint throughout,
    because the claim under test is about what an observed service experiences.

    Every run is graded against pre-registered validity checks. A run that fails
    any of them prints INVALID and its numbers should not be quoted.
    """
    from telemetry_engine.experiments.backpressure import (
        run_experiment,
        write_report,
    )

    settings = get_settings()
    configure_logging(settings.log_level)

    result = run_experiment(
        settings,
        baseline_s=baseline,
        burst_s=burst,
        recovery_s=recovery,
        sustained_rate=rate or settings.workload.sustained_spans_per_sec,
        burst_rate=burst_rate or settings.workload.burst_spans_per_sec,
        workers=workers,
        sample_interval_s=interval,
    )

    typer.echo("")
    typer.echo("  VALIDITY")
    for check in result.checks:
        mark = "PASS" if check.passed else "FAIL"
        color = None if check.passed else typer.colors.RED
        typer.secho(f"    [{mark}] {check.name}: {check.detail}", fg=color)
        if not check.passed:
            typer.secho(f"           would hide: {check.would_hide}", fg=typer.colors.YELLOW)

    baseline_lat = result.probe_latency(*result.baseline_window())
    burst_lat = result.probe_latency(*result.burst_window())
    recovery_time = result.recovery_seconds()

    typer.echo("")
    typer.echo("  RESULTS")
    typer.echo(
        f"    generator:         {result.generator_spans:,} spans "
        f"({result.generator_achieved_rate:,.0f}/s during burst)"
    )
    typer.echo(f"    peak consumer lag: {result.peak_lag:,} messages")
    typer.echo(
        "    recovery:          "
        + (
            f"{recovery_time:.1f}s after burst end"
            if recovery_time is not None
            else "DID NOT RECOVER in the observation window"
        )
    )
    typer.echo(f"    collector dropped: {result.collector_dropped:,}")
    typer.echo(f"    collector refused: {result.collector_refused:,}")
    typer.echo(f"    sdk-side loss:     {result.sdk_lost:,}")
    typer.echo(f"    unaccounted:       {result.unaccounted:,}")
    typer.echo("")
    typer.echo(
        f"    endpoint p50/p99 baseline: {baseline_lat['p50']:.1f} / {baseline_lat['p99']:.1f} ms"
    )
    typer.echo(f"    endpoint p50/p99 burst:    {burst_lat['p50']:.1f} / {burst_lat['p99']:.1f} ms")

    path = write_report(result, Path(out))
    typer.echo("")
    if result.valid:
        typer.secho(f"  RUN VALID - full results in {path}", fg=typer.colors.GREEN)
    else:
        typer.secho(f"  RUN INVALID - do not quote these numbers ({path})", fg=typer.colors.RED)
        raise typer.Exit(1)


@app.command()
def bootstrap() -> None:
    """Bring a freshly started stack to a usable state: topics, schema, allowlist."""
    stack_wait(timeout=90.0)
    topics_apply(dry_run=False)
    migrate(dry_run=False, wait=True)
    # Without this the rollups bucket everything into __other__: bounded, but
    # not useful. Static dimensions apply immediately; top-K needs traffic.
    dimensions_apply()


if __name__ == "__main__":  # pragma: no cover
    app()
