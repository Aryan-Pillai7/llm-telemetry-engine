"""Physical layout of the Parquet cold tier.

One module owns every path and sizing decision, so the exporter, the compactor,
and the DuckDB query layer cannot disagree about where data lives.

The layout (ADR-007):

    data/cold/dt=YYYY-MM-DD/spans-<start>-<end>.parquet

`dt` is the only partition key. `hour` rides along as a column, and rows are
sorted by `(tenant_id, ts)` inside each file. That combination is deliberate:

  - Partitioning on tenant would multiply file count by tenant cardinality. With
    a zipfian tail of mostly-idle tenants that produces thousands of tiny files,
    and small files are the classic way a Parquet lake becomes slower than the
    database it was supposed to relieve.
  - Sorting by tenant instead gives DuckDB what it actually needs: per-row-group
    min/max statistics on tenant_id, so a tenant-scoped query skips row groups
    without opening them. Sorting is what makes the coarse partitioning safe --
    if the sort is ever dropped, the layout still "works" while reading every
    byte on every query.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

# Only `dt`. See the module docstring.
PARTITION_KEYS: tuple[str, ...] = ("dt",)

# Rows are written in this order. tenant_id first because tenant-scoped queries
# dominate; ts second so a time filter prunes within a tenant.
SORT_KEYS: tuple[str, ...] = ("tenant_id", "ts")

# Rows per Parquet row group. This is the pruning granularity: too large and a
# tenant filter still reads most of the file, too small and statistics overhead
# and per-group decoding dominate. 128k rows is a few MiB compressed here.
ROW_GROUP_SIZE = 128_000

# ZSTD over snappy: the cold tier is written once and read rarely, so the extra
# compression time is paid once and the storage saving is permanent.
COMPRESSION = "zstd"

_PARTITION_RE = re.compile(r"^dt=(\d{4}-\d{2}-\d{2})$")
_FILE_RE = re.compile(r"^spans-(\d{8}T\d{6})-(\d{8}T\d{6})\.parquet$")

_STAMP = "%Y%m%dT%H%M%S"


@dataclass(frozen=True)
class ExportWindow:
    """A half-open time range [start, end) exported as one file."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start >= self.end:
            raise ValueError(f"empty export window: {self.start} >= {self.end}")
        if self.start.date() != (self.end.date()) and self.end != datetime.combine(
            self.end.date(), datetime.min.time()
        ):
            # A window that straddles midnight would belong to two partitions.
            raise ValueError(
                f"window {self.start} -> {self.end} crosses a date boundary; "
                "split it so each file belongs to exactly one dt= partition"
            )

    @property
    def partition_date(self) -> date:
        return self.start.date()

    @property
    def filename(self) -> str:
        """Deterministic name.

        Deterministic on purpose: re-exporting a window overwrites its file
        rather than adding a second copy of the same rows. A timestamped or
        random name would turn a retry into silent duplication, and duplicates
        in a lake are far harder to notice than missing rows.
        """
        return f"spans-{self.start.strftime(_STAMP)}-{self.end.strftime(_STAMP)}.parquet"


def partition_dir(root: Path, day: date) -> Path:
    return root / f"dt={day.isoformat()}"


def file_path(root: Path, window: ExportWindow) -> Path:
    return partition_dir(root, window.partition_date) / window.filename


def temp_path(final: Path) -> Path:
    """Staging path for an atomic write.

    Parquet is written here and renamed into place only after it has been read
    back and verified. A reader globbing the lake must never see a half-written
    file, and `.tmp` is excluded from the glob.
    """
    return final.with_suffix(".parquet.tmp")


def parse_partition(path: Path) -> date | None:
    """Extract the date from a `dt=YYYY-MM-DD` directory name."""
    match = _PARTITION_RE.match(path.name)
    return date.fromisoformat(match.group(1)) if match else None


def parse_window(path: Path) -> ExportWindow | None:
    """Recover the window a file covers from its name."""
    match = _FILE_RE.match(path.name)
    if not match:
        return None
    return ExportWindow(
        start=datetime.strptime(match.group(1), _STAMP),
        end=datetime.strptime(match.group(2), _STAMP),
    )


def parquet_files(root: Path) -> list[Path]:
    """Every committed Parquet file in the lake, in partition order.

    Excludes `.tmp` staging files, which are by definition not yet verified.
    """
    if not root.is_dir():
        return []
    return sorted(
        path
        for path in root.glob("dt=*/*.parquet")
        if not path.name.endswith(".tmp") and parse_partition(path.parent) is not None
    )


def glob_pattern(root: Path) -> str:
    """The glob DuckDB reads.

    Must match `parquet_files` exactly. A mismatch is the quiet failure mode
    here: DuckDB would return a smaller dataset with no error at all, and the
    query layer verifies the two agree rather than assuming it.
    """
    return str(root / "dt=*" / "*.parquet").replace("\\", "/")
