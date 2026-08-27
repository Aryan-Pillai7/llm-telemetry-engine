#!/usr/bin/env python3
"""Cross-platform task runner: `python tasks.py <target>`.

Deliberately not a Makefile. The primary dev environment here is Windows, which
has no `make`, and a Makefile plus a PowerShell shim means two definitions of
every target that drift apart. This is stdlib-only, runs identically on Windows,
macOS, Linux, and in CI, and is the single source of truth for dev commands.

Every Python target runs inside the project venv at `.venv/`, created on demand
by `install`. Nothing here ever installs into the interpreter that launched it,
so the pipeline's dependency set cannot disturb anything else on the machine.
CI opts out with TE_NO_VENV=1, since a CI runner is already disposable.

    python tasks.py            # list targets
    python tasks.py install
    python tasks.py test
    python tasks.py up
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import venv
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parent
COMPOSE_FILE = ROOT / "deploy" / "docker-compose.yml"
VENV_DIR = ROOT / ".venv"

# Set by CI, where the runner is disposable and a venv only adds a layer.
USE_VENV = os.environ.get("TE_NO_VENV", "") != "1"

TARGETS: dict[str, Callable[[], int]] = {}


def target(help_text: str) -> Callable[[Callable[[], int]], Callable[[], int]]:
    def decorate(fn: Callable[[], int]) -> Callable[[], int]:
        fn.__doc__ = help_text
        TARGETS[fn.__name__.replace("_", "-")] = fn
        return fn

    return decorate


def venv_python() -> Path:
    """Path to the venv interpreter (Scripts/ on Windows, bin/ elsewhere)."""
    if os.name == "nt":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"


def python_exe() -> str:
    """The interpreter that Python targets run under.

    Falls back to the launching interpreter when the venv is absent (so `help`
    and the stack targets work on a fresh clone) or when explicitly disabled.
    """
    if USE_VENV and venv_python().exists():
        return str(venv_python())
    return sys.executable


def run(*cmd: str) -> int:
    """Run a command in the repo root, streaming output. Returns its exit code."""
    print(f"$ {' '.join(cmd)}", flush=True)
    exe = cmd[0] if Path(cmd[0]).exists() else shutil.which(cmd[0])
    if exe is None:
        print(f"error: '{cmd[0]}' not found on PATH", file=sys.stderr)
        return 127
    return subprocess.call([exe, *cmd[1:]], cwd=ROOT)


def py(*args: str) -> int:
    """Run a module/script under the project interpreter."""
    return run(python_exe(), *args)


def compose(*args: str) -> int:
    return run("docker", "compose", "-f", str(COMPOSE_FILE), *args)


def first_failure(*codes: int) -> int:
    return next((c for c in codes if c != 0), 0)


def require_venv() -> int:
    """Fail loudly rather than silently installing into the system interpreter."""
    if not USE_VENV or venv_python().exists():
        return 0
    print("error: no .venv found - run `python tasks.py install` first", file=sys.stderr)
    return 1


# --- Python-side targets -----------------------------------------------------


@target("Create .venv if needed, then install the package with dev extras into it")
def install() -> int:
    if USE_VENV and not venv_python().exists():
        print(f"creating venv at {VENV_DIR}", flush=True)
        venv.EnvBuilder(with_pip=True, upgrade_deps=True).create(VENV_DIR)
    return py("-m", "pip", "install", "-e", ".[dev]")


@target("Lint and check formatting")
def lint() -> int:
    if (rc := require_venv()) != 0:
        return rc
    return first_failure(
        py("-m", "ruff", "check", "."),
        py("-m", "ruff", "format", "--check", "."),
    )


@target("Auto-fix lint findings and format")
def fmt() -> int:
    if (rc := require_venv()) != 0:
        return rc
    return first_failure(
        py("-m", "ruff", "check", "--fix", "."),
        py("-m", "ruff", "format", "."),
    )


@target("Run unit tests (no docker stack required)")
def test() -> int:
    if (rc := require_venv()) != 0:
        return rc
    return py("-m", "pytest", "-m", "not integration")


@target("Run integration tests (requires `python tasks.py up`)")
def test_integration() -> int:
    if (rc := require_venv()) != 0:
        return rc
    return py("-m", "pytest", "-m", "integration")


@target("Type check with mypy")
def typecheck() -> int:
    if (rc := require_venv()) != 0:
        return rc
    return py("-m", "mypy")


@target("Find guards that are defined but never called")
def audit_guards() -> int:
    if (rc := require_venv()) != 0:
        return rc
    return py("scripts/find_inert_guards.py")


@target("Unit tests with a coverage report")
def coverage() -> int:
    if (rc := require_venv()) != 0:
        return rc
    return py(
        "-m",
        "pytest",
        "-m",
        "not integration",
        "--cov=telemetry_engine",
        "--cov-report=term-missing:skip-covered",
        "--cov-report=html",
    )


@target("Run every static check CI runs (lint, types, guard audit, unit tests)")
def ci() -> int:
    return first_failure(lint(), typecheck(), audit_guards(), test())


@target("Reconcile Redpanda topics and apply ClickHouse migrations")
def bootstrap() -> int:
    if (rc := require_venv()) != 0:
        return rc
    return py("-m", "telemetry_engine.cli", "bootstrap")


@target("Apply pending ClickHouse migrations")
def migrate() -> int:
    if (rc := require_venv()) != 0:
        return rc
    return py("-m", "telemetry_engine.cli", "migrate")


@target("Run the mock LLM inference endpoint on :8080")
def serve() -> int:
    if (rc := require_venv()) != 0:
        return rc
    return py("-m", "telemetry_engine.cli", "serve")


@target("Drive 30s of steady synthetic load at the configured rate")
def load() -> int:
    if (rc := require_venv()) != 0:
        return rc
    return py("-m", "telemetry_engine.cli", "load")


@target("Drive 60s of bursty load (sustained baseline with spikes above capacity)")
def load_burst() -> int:
    if (rc := require_venv()) != 0:
        return rc
    return py("-m", "telemetry_engine.cli", "load", "--profile", "burst", "--duration", "60")


@target("One command from nothing to a populated, queryable pipeline")
def demo() -> int:
    if (rc := require_venv()) != 0:
        return rc
    return py("scripts/demo.py")


# --- Stack targets -----------------------------------------------------------


@target("Start the stack and bootstrap topics + schema")
def up() -> int:
    rc = compose("up", "-d", "--wait")
    if rc != 0:
        return rc
    # A started stack is not a usable stack: the topic partition count and the
    # schema are architectural decisions, not broker defaults (see ADR-004/009).
    return bootstrap()


@target("Start the stack only, without bootstrapping")
def up_bare() -> int:
    return compose("up", "-d", "--wait")


@target("Stop the stack, keeping volumes")
def down() -> int:
    return compose("down")


@target("Stop the stack and delete its volumes (destroys all telemetry data)")
def nuke() -> int:
    return compose("down", "-v")


@target("Tail stack logs")
def logs() -> int:
    return compose("logs", "-f", "--tail", "100")


@target("Show stack container status")
def ps() -> int:
    return compose("ps")


def main(argv: list[str]) -> int:
    if not argv or argv[0] in {"-h", "--help", "help"}:
        width = max(len(name) for name in TARGETS)
        print(__doc__)
        print("Targets:")
        for name, fn in TARGETS.items():
            print(f"  {name:<{width}}  {fn.__doc__}")
        return 0

    name = argv[0]
    if name not in TARGETS:
        print(f"error: unknown target '{name}' (try: python tasks.py help)", file=sys.stderr)
        return 2
    return TARGETS[name]()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
