"""Operator entrypoint for the telemetry engine.

Subcommands are added as each phase lands (migrate, emit, load, lag, export,
query). Everything an operator does to this pipeline should be reachable from
here rather than from a pile of loose scripts.
"""

from __future__ import annotations

import json

import typer

from telemetry_engine import __version__
from telemetry_engine.common.logging import configure_logging
from telemetry_engine.config import get_settings

app = typer.Typer(
    name="telemetry-engine",
    help="LLM & agent telemetry analytics engine.",
    no_args_is_help=True,
    add_completion=False,
)


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
    configure_logging(settings.log_level)
    typer.echo(json.dumps(json.loads(settings.model_dump_json()), indent=2, default=str))


if __name__ == "__main__":  # pragma: no cover
    app()
