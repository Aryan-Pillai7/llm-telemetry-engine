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


@app.command()
def bootstrap() -> None:
    """Bring a freshly started stack to a usable state: topics, then schema."""
    topics_apply(dry_run=False)
    migrate(dry_run=False, wait=True)


if __name__ == "__main__":  # pragma: no cover
    app()
