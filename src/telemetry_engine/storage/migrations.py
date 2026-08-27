"""Ordered, idempotent DDL migration runner.

`schemas/clickhouse/*.sql` is the source of truth for the hot tier. Files are
applied in filename order and recorded in `schema_migrations`, so a re-run is a
no-op and a fresh volume rebuilds the whole schema unattended.

Why not just mount the SQL into ClickHouse's docker-entrypoint-initdb.d? Because
that only runs on a *first* boot with an empty volume. Schema changes after that
would need a `nuke`, which throws away every measurement taken so far -- exactly
what you do not want mid-experiment.

Applied files are checksummed. Editing a file that already ran is flagged rather
than silently ignored: the intent was almost certainly a new migration.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

from clickhouse_connect.driver.client import Client

from telemetry_engine.common.logging import get_logger
from telemetry_engine.config import Settings, get_settings
from telemetry_engine.storage.client import client as ch_client

log = get_logger(__name__)

# Bookkeeping table. ReplacingMergeTree so a re-applied file updates its row
# rather than accumulating duplicates.
_MIGRATIONS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {db}.schema_migrations
(
    filename   String,
    checksum   String,
    applied_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(applied_at)
ORDER BY filename
"""

_COMMENT_RE = re.compile(r"^\s*--.*$", re.MULTILINE)


@dataclass(frozen=True)
class Migration:
    """One .sql file on disk."""

    path: Path

    @property
    def name(self) -> str:
        return self.path.name

    @property
    def sql(self) -> str:
        return self.path.read_text(encoding="utf-8")

    @property
    def checksum(self) -> str:
        # Normalize line endings so a Windows checkout and a Linux CI runner
        # agree on the checksum of an unchanged file.
        normalized = self.sql.replace("\r\n", "\n").encode("utf-8")
        return hashlib.sha256(normalized).hexdigest()[:16]

    def statements(self) -> list[str]:
        """Split the file into individual statements.

        ClickHouse's HTTP interface takes one statement per request. The split
        is deliberately simple -- strip line comments, split on semicolons --
        which is sufficient for DDL. It would mangle a semicolon inside a string
        literal, so migrations must not contain one.
        """
        stripped = _COMMENT_RE.sub("", self.sql)
        return [s.strip() for s in stripped.split(";") if s.strip()]


@dataclass(frozen=True)
class MigrationResult:
    """What `apply_all` did, for reporting."""

    applied: list[str]
    skipped: list[str]
    drifted: list[str]

    @property
    def changed(self) -> bool:
        return bool(self.applied)


def discover(schemas_dir: Path) -> list[Migration]:
    """Return migrations in filename order.

    The numeric prefix is the ordering contract: 000_ before 010_, and so on.
    """
    if not schemas_dir.is_dir():
        raise FileNotFoundError(f"schemas directory not found: {schemas_dir}")
    return [Migration(p) for p in sorted(schemas_dir.glob("*.sql"))]


def _applied_checksums(conn: Client, database: str) -> dict[str, str]:
    rows = conn.query(
        f"SELECT filename, checksum FROM {database}.schema_migrations FINAL"
    ).result_rows
    return {str(name): str(checksum) for name, checksum in rows}


def apply_all(settings: Settings | None = None, *, dry_run: bool = False) -> MigrationResult:
    """Apply every pending migration. Returns what was applied, skipped, drifted."""
    cfg = settings or get_settings()
    database = cfg.clickhouse.database
    migrations = discover(cfg.schemas_dir)

    applied: list[str] = []
    skipped: list[str] = []
    drifted: list[str] = []

    # Connect to the server default database: the first migration is the one
    # that creates ours.
    with ch_client(cfg.clickhouse, database="default") as conn:
        if dry_run:
            # No bookkeeping table may exist yet on a cold server, in which case
            # everything is pending by definition.
            try:
                already = _applied_checksums(conn, database)
            except Exception:
                already = {}
            for m in migrations:
                (skipped if already.get(m.name) == m.checksum else applied).append(m.name)
            return MigrationResult(applied=applied, skipped=skipped, drifted=drifted)

        # 000_database.sql creates the database; the bookkeeping table needs it
        # to exist, so the first file is applied before bookkeeping starts.
        bootstrap, rest = migrations[:1], migrations[1:]
        for m in bootstrap:
            for stmt in m.statements():
                conn.command(stmt)

        conn.command(_MIGRATIONS_TABLE_DDL.format(db=database))
        already = _applied_checksums(conn, database)

        for m in bootstrap:
            if already.get(m.name) != m.checksum:
                conn.insert(
                    f"{database}.schema_migrations",
                    [[m.name, m.checksum]],
                    column_names=["filename", "checksum"],
                )
                applied.append(m.name)
            else:
                skipped.append(m.name)

        for m in rest:
            prior = already.get(m.name)
            if prior == m.checksum:
                skipped.append(m.name)
                continue
            if prior is not None:
                # The file changed after it was applied. Re-running it may or
                # may not be safe, so surface it instead of guessing.
                drifted.append(m.name)
                log.warning(
                    "migration_drift",
                    filename=m.name,
                    applied_checksum=prior,
                    current_checksum=m.checksum,
                    hint="edit an applied migration only if it is idempotent; "
                    "otherwise add a new numbered file",
                )
                continue

            log.info("applying_migration", filename=m.name)
            for stmt in m.statements():
                conn.command(stmt)
            conn.insert(
                f"{database}.schema_migrations",
                [[m.name, m.checksum]],
                column_names=["filename", "checksum"],
            )
            applied.append(m.name)

    return MigrationResult(applied=applied, skipped=skipped, drifted=drifted)
