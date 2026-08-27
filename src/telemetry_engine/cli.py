"""Operator entrypoint for the telemetry engine.

Subcommands are added as each phase lands. Everything an operator does to this
pipeline should be reachable from here rather than from a pile of loose scripts.
"""

from __future__ import annotations

import json

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
