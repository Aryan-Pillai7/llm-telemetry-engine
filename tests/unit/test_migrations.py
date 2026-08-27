"""Migration discovery, checksumming, and statement splitting.

None of this needs a ClickHouse connection -- that is the point. The parts most
likely to go wrong (ordering, statement splitting, checksum stability across
platforms) are pure functions of what is on disk.
"""

from __future__ import annotations

import pytest

from telemetry_engine.config import Settings
from telemetry_engine.storage.migrations import Migration, discover


def _write(tmp_path, name: str, sql: str) -> Migration:
    p = tmp_path / name
    # Write bytes, not text: `write_text` on Windows translates newlines to
    # os.linesep, which would silently rewrite the very line endings a test is
    # trying to pin down.
    p.write_bytes(sql.encode("utf-8"))
    return Migration(p)


def test_discovery_is_ordered_by_filename(tmp_path) -> None:
    """The numeric prefix is the ordering contract."""
    for name in ["050_late.sql", "000_database.sql", "010_early.sql"]:
        (tmp_path / name).write_text("SELECT 1;", encoding="utf-8")
    assert [m.name for m in discover(tmp_path)] == [
        "000_database.sql",
        "010_early.sql",
        "050_late.sql",
    ]


def test_discovery_on_missing_directory_is_an_error(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        discover(tmp_path / "nope")


def test_statements_split_on_semicolons(tmp_path) -> None:
    m = _write(tmp_path, "001.sql", "CREATE DATABASE a;\n\nCREATE TABLE a.b (x Int8) ENGINE=Null;\n")
    assert m.statements() == [
        "CREATE DATABASE a",
        "CREATE TABLE a.b (x Int8) ENGINE=Null",
    ]


def test_comments_are_stripped(tmp_path) -> None:
    """ClickHouse tolerates comments, but stripping keeps statements clean."""
    m = _write(tmp_path, "001.sql", "-- explanatory header\nCREATE DATABASE a;\n-- trailing note\n")
    assert m.statements() == ["CREATE DATABASE a"]


def test_comment_only_file_yields_no_statements(tmp_path) -> None:
    m = _write(tmp_path, "001.sql", "-- nothing here yet\n")
    assert m.statements() == []


def test_trailing_semicolon_does_not_produce_empty_statement(tmp_path) -> None:
    m = _write(tmp_path, "001.sql", "SELECT 1;")
    assert m.statements() == ["SELECT 1"]


def test_checksum_ignores_line_endings(tmp_path) -> None:
    """A Windows checkout and a Linux CI runner must agree on an unchanged file.

    Without normalization, .gitattributes converting CRLF to LF would make every
    migration look edited on one platform and not the other. (Path.read_text
    already applies universal newlines, so this mostly guards against a future
    switch to binary reads.)
    """
    lf = _write(tmp_path, "lf.sql", "CREATE DATABASE a;\nCREATE DATABASE b;\n")
    crlf = _write(tmp_path, "crlf.sql", "CREATE DATABASE a;\r\nCREATE DATABASE b;\r\n")
    assert lf.checksum == crlf.checksum


def test_checksum_changes_when_sql_changes(tmp_path) -> None:
    before = _write(tmp_path, "a.sql", "CREATE DATABASE a;").checksum
    after = _write(tmp_path, "b.sql", "CREATE DATABASE b;").checksum
    assert before != after


# --- The shipped schema ------------------------------------------------------


def test_shipped_migrations_are_discoverable() -> None:
    settings = Settings()
    migrations = discover(settings.schemas_dir)
    assert migrations, "expected at least the database bootstrap migration"


def test_first_migration_creates_the_configured_database() -> None:
    """apply_all applies file [0] before bookkeeping exists; it must create the DB."""
    settings = Settings()
    first = discover(settings.schemas_dir)[0]
    statements = " ".join(first.statements()).lower()
    assert "create database" in statements
    assert settings.clickhouse.database in statements


def test_shipped_migrations_are_idempotent_ddl() -> None:
    """Every applied file must be safe to re-run.

    The runner records what it applied, but a file that fails halfway through
    has to be re-runnable without hand-editing the bookkeeping table.
    """
    settings = Settings()
    for migration in discover(settings.schemas_dir):
        for stmt in migration.statements():
            lowered = stmt.lower()
            if lowered.startswith("create"):
                assert "if not exists" in lowered, (
                    f"{migration.name}: '{stmt[:60]}...' must use IF NOT EXISTS"
                )


def test_shipped_migrations_contain_no_semicolons_in_literals() -> None:
    """The statement splitter is naive; guard the assumption it relies on."""
    settings = Settings()
    for migration in discover(settings.schemas_dir):
        sql = migration.sql
        # A semicolon inside quotes would be split incorrectly.
        for quote in ("'", '"'):
            segments = sql.split(quote)
            # Odd indices are inside quotes.
            assert all(";" not in seg for seg in segments[1::2]), (
                f"{migration.name}: semicolon inside a {quote}-quoted literal"
            )
