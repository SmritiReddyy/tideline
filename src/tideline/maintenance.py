"""Table maintenance: the unglamorous job that decides whether the lakehouse
is actually queryable.

A streaming MERGE writes new files on every micro-batch. At a ten-second
trigger that is 8,640 commits a day, each leaving behind small Parquet files.
Nothing breaks — the table stays correct — but query planning degrades steadily
as the engine opens thousands of files to read a few megabytes. This is the
"small file problem", and it is the difference between a lakehouse demo and a
lakehouse.

Three operations, in the order they should run:

1. **OPTIMIZE** — rewrite many small files into few large ones.
2. **Z-ORDER** — cluster co-accessed values into the same files so a filtered
   query can skip most of them entirely.
3. **VACUUM** — delete files no longer referenced by any retained version.
   This is the one that reclaims disk, and the one that destroys time travel
   beyond its retention window. Order matters: vacuuming before optimizing
   would leave the pre-compaction files behind for another cycle.
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from .cdc import TableSpec
from .config import Config

log = logging.getLogger(__name__)

# Delta refuses a VACUUM below 168 hours unless the retention check is disabled,
# because a shorter window can delete files an in-flight reader still needs.
DEFAULT_RETAIN_HOURS = 168


@dataclass
class FileStats:
    files: int
    bytes: int
    avg_file_bytes: float
    small_files: int  # below the target size, i.e. compaction candidates

    @property
    def megabytes(self) -> float:
        return self.bytes / 1024 / 1024


@dataclass
class MaintenanceResult:
    table: str
    before: FileStats
    after: FileStats
    optimize_seconds: float
    vacuum_seconds: float
    zordered_by: tuple[str, ...]

    @property
    def file_reduction(self) -> float:
        return 1 - (self.after.files / self.before.files) if self.before.files else 0.0

    def to_dict(self) -> dict:
        data = asdict(self)
        data["file_reduction_pct"] = round(self.file_reduction * 100, 1)
        return data


def file_stats(table_path: str, small_file_threshold_mb: int = 16) -> FileStats:
    """Count the Parquet files actually backing the table.

    Deliberately reads the filesystem rather than Delta's metadata: the point
    is to measure what the query engine will have to open, including files
    still on disk that the latest version no longer references.
    """
    root = Path(table_path)
    if not root.exists():
        return FileStats(0, 0, 0.0, 0)

    files = [p for p in root.rglob("*.parquet") if "_delta_log" not in p.parts]
    sizes = [p.stat().st_size for p in files]
    total = sum(sizes)
    threshold = small_file_threshold_mb * 1024 * 1024
    return FileStats(
        files=len(files),
        bytes=total,
        avg_file_bytes=(total / len(files)) if files else 0.0,
        small_files=sum(1 for s in sizes if s < threshold),
    )


def optimize_table(
    spark: SparkSession,
    table_path: str,
    spec: TableSpec,
    *,
    target_file_mb: int = 128,
) -> float:
    """Compact, and Z-order if the spec names clustering columns."""
    # Delta's bin-packing target. 128 MB is the usual sweet spot: large enough
    # that per-file overhead disappears, small enough to stay parallelisable.
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", target_file_mb * 1024 * 1024)

    table = DeltaTable.forPath(spark, table_path)
    started = time.monotonic()

    if spec.zorder_by:
        table.optimize().executeZOrderBy(*spec.zorder_by)
    else:
        table.optimize().executeCompaction()

    return time.monotonic() - started


def vacuum_table(
    spark: SparkSession,
    table_path: str,
    *,
    retain_hours: int = DEFAULT_RETAIN_HOURS,
) -> float:
    """Delete unreferenced files older than the retention window.

    `retain_hours` is the time-travel horizon. Shrinking it reclaims disk and
    destroys the ability to query older versions — a real trade, not a knob to
    turn casually.
    """
    table = DeltaTable.forPath(spark, table_path)
    started = time.monotonic()
    table.vacuum(retentionHours=retain_hours)
    return time.monotonic() - started


def maintain_table(
    spark: SparkSession,
    config: Config,
    spec: TableSpec,
    *,
    retain_hours: int = DEFAULT_RETAIN_HOURS,
    target_file_mb: int = 128,
    skip_vacuum: bool = False,
) -> MaintenanceResult:
    table_path = config.table_path(spec.name)
    if not DeltaTable.isDeltaTable(spark, table_path):
        raise FileNotFoundError(f"No Delta table at {table_path}. Run the stream first.")

    before = file_stats(table_path)
    optimize_seconds = optimize_table(spark, table_path, spec, target_file_mb=target_file_mb)
    vacuum_seconds = (
        0.0 if skip_vacuum else vacuum_table(spark, table_path, retain_hours=retain_hours)
    )
    after = file_stats(table_path)

    result = MaintenanceResult(
        table=spec.name,
        before=before,
        after=after,
        optimize_seconds=round(optimize_seconds, 3),
        vacuum_seconds=round(vacuum_seconds, 3),
        zordered_by=spec.zorder_by,
    )
    log.info(
        "%s: %s files (%.1f MiB) -> %s files (%.1f MiB), %.0f%% fewer, optimize %.2fs vacuum %.2fs",
        spec.name,
        before.files,
        before.megabytes,
        after.files,
        after.megabytes,
        result.file_reduction * 100,
        optimize_seconds,
        vacuum_seconds,
    )
    return result


def maintain_all(
    spark: SparkSession,
    config: Config,
    specs: list[TableSpec],
    **kwargs,
) -> list[MaintenanceResult]:
    results = []
    for spec in specs:
        try:
            results.append(maintain_table(spark, config, spec, **kwargs))
        except FileNotFoundError as exc:
            log.warning("skipping %s: %s", spec.name, exc)
    return results


def describe_history(spark: SparkSession, table_path: str, limit: int = 20):
    """Delta's commit log — the audit trail for who changed what, when."""
    return DeltaTable.forPath(spark, table_path).history(limit)
