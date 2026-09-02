#!/usr/bin/env python
"""Measure what compaction actually buys, end to end.

Builds a fragmented lakehouse the way the stream does — many small commits —
benchmarks a set of representative analytical queries, runs OPTIMIZE + Z-ORDER
+ VACUUM, then benchmarks the identical queries again.

Queries run through DuckDB's `delta_scan`, not Spark. That is deliberate: Spark
would add seconds of job-submission overhead per query and drown the signal.
DuckDB reads the same Delta log and the same Parquet files, so what is being
measured is the file layout, which is exactly the thing compaction changes.

    python scripts/benchmark.py --batches 20 --runs 7
"""

from __future__ import annotations

import argparse
import json
import shutil
import statistics
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

import duckdb  # noqa: E402

from tideline.cdc import TABLE_SPECS  # noqa: E402
from tideline.config import Config, get_config  # noqa: E402
from tideline.maintenance import file_stats, maintain_all  # noqa: E402
from tideline.simulate import run_simulation  # noqa: E402
from tideline.spark import build_spark  # noqa: E402

# Representative of what a real consumer asks of a CDC lakehouse: point lookups,
# filtered scans on the clustering key, joins, and aggregates.
QUERIES: dict[str, str] = {
    "point lookup by key": """
        select * from orders where order_id = 42
    """,
    "filter on z-order column": """
        select order_id, status, total_amount
        from orders
        where customer_id between 100 and 140
    """,
    "aggregate by status": """
        select status, count(*) as orders, sum(total_amount) as revenue
        from orders
        where not _deleted
        group by status
        order by revenue desc
    """,
    "join orders to line items": """
        select o.status, count(*) as lines, sum(i.quantity * i.unit_price) as value
        from orders o
        join order_items i on o.order_id = i.order_id
        where not o._deleted and not i._deleted
        group by o.status
    """,
    "inventory below reorder level": """
        select product_id, warehouse_id, quantity, reorder_level
        from inventory
        where quantity < reorder_level
        order by quantity
    """,
    "full scan count": """
        select count(*) from order_items
    """,
}


def _connect(config: Config) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    for spec in TABLE_SPECS:
        path = config.table_path(spec.name)
        if Path(path).exists():
            con.execute(f"CREATE OR REPLACE VIEW {spec.name} AS SELECT * FROM delta_scan('{path}')")
    return con


def benchmark(config: Config, runs: int) -> dict[str, float]:
    """Median latency per query, in milliseconds."""
    results: dict[str, float] = {}
    for label, sql in QUERIES.items():
        # A fresh connection per query: DuckDB caches the Delta log, and reusing
        # one connection would measure the cache rather than the file layout.
        con = _connect(config)
        try:
            con.execute(sql).fetchall()  # warm
            timings = []
            for _ in range(runs):
                started = time.perf_counter()
                con.execute(sql).fetchall()
                timings.append((time.perf_counter() - started) * 1000)
            results[label] = statistics.median(timings)
        finally:
            con.close()
    return results


def total_files(config: Config) -> tuple[int, float]:
    files = 0
    megabytes = 0.0
    for spec in TABLE_SPECS:
        stats = file_stats(config.table_path(spec.name))
        files += stats.files
        megabytes += stats.megabytes
    return files, megabytes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batches", type=int, default=20)
    parser.add_argument("--ticks-per-batch", type=int, default=40)
    parser.add_argument("--runs", type=int, default=7, help="Timed runs per query.")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    config = get_config()
    lakehouse = Path(config.lakehouse_root)

    # Start clean so the numbers describe this run and nothing else.
    if lakehouse.exists():
        shutil.rmtree(lakehouse)
    checkpoints = Path(config.checkpoint_root)
    if checkpoints.exists():
        shutil.rmtree(checkpoints)

    spark = build_spark(config, app_name="tideline-benchmark", with_kafka=False)
    try:
        print(f"Simulating {args.batches} micro-batches of CDC traffic...", file=sys.stderr)
        stats = run_simulation(
            spark,
            config,
            list(TABLE_SPECS),
            batches=args.batches,
            ticks_per_batch=args.ticks_per_batch,
            evolve_at_batch=args.batches // 2,
        )

        files_before, mib_before = total_files(config)
        print(f"Benchmarking fragmented layout ({files_before} files)...", file=sys.stderr)
        before = benchmark(config, args.runs)

        print("Compacting...", file=sys.stderr)
        maintenance = maintain_all(spark, config, list(TABLE_SPECS), retain_hours=0)

        files_after, mib_after = total_files(config)
        print(f"Benchmarking compacted layout ({files_after} files)...", file=sys.stderr)
        after = benchmark(config, args.runs)
    finally:
        spark.stop()

    # --- report ----------------------------------------------------------
    print("\nTideline — compaction benchmark")
    print("=" * 78)
    print(f"\nCDC events simulated      {stats.events:>10,}")
    print(f"Delta commits             {stats.batches:>10,}")
    print(f"Rows merged               {stats.merged:>10,}")
    print(f"Collapsed by dedup        {stats.collapsed:>10,}")

    print(f"\n{'':<34}{'before':>12}{'after':>12}{'change':>12}")
    print("-" * 78)
    print(
        f"{'parquet files':<34}{files_before:>12,}{files_after:>12,}"
        f"{-(1 - files_after / max(files_before, 1)) * 100:>11.0f}%"
    )
    print(
        f"{'on-disk size (MiB)':<34}{mib_before:>12.2f}{mib_after:>12.2f}"
        f"{-(1 - mib_after / max(mib_before, 1e-9)) * 100:>11.0f}%"
    )

    print(f"\n{'query':<34}{'before ms':>12}{'after ms':>12}{'speedup':>12}")
    print("-" * 78)
    total_before = total_after = 0.0
    for label in QUERIES:
        b, a = before[label], after[label]
        total_before += b
        total_after += a
        print(f"{label:<34}{b:>12.2f}{a:>12.2f}{b / a:>11.2f}x")
    print("-" * 78)
    print(
        f"{'TOTAL':<34}{total_before:>12.2f}{total_after:>12.2f}"
        f"{total_before / total_after:>11.2f}x"
    )
    print("=" * 78)

    report = {
        "events": stats.events,
        "delta_commits": stats.batches,
        "rows_merged": stats.merged,
        "collapsed_by_dedup": stats.collapsed,
        "simulation_seconds": round(stats.seconds, 2),
        "files_before": files_before,
        "files_after": files_after,
        "mib_before": round(mib_before, 3),
        "mib_after": round(mib_after, 3),
        "query_ms_before": {k: round(v, 3) for k, v in before.items()},
        "query_ms_after": {k: round(v, 3) for k, v in after.items()},
        "total_speedup": round(total_before / total_after, 3),
        "per_table": [m.to_dict() for m in maintenance],
    }
    out = REPO_ROOT / "data" / "benchmark.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, default=str))
    print(f"\nReport written to {out}")

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
