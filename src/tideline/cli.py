"""Command line entry point: `tideline <command>`.

Every stage of the pipeline is reachable from here, so a failed Airflow task or
a broken demo can be reproduced by hand one command at a time.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from . import connector as connector_api
from .cdc import TABLE_SPECS, get_spec
from .config import get_config
from .generator import WorkloadGenerator
from .maintenance import DEFAULT_RETAIN_HOURS, file_stats, maintain_all
from .simulate import run_simulation
from .spark import build_spark
from .stream import print_summary, run_streams

REPO_ROOT = Path(__file__).resolve().parents[2]
CONNECTOR_CONFIG = REPO_ROOT / "connectors" / "postgres-source.json"


def _logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )


def _specs(names: list[str] | None):
    return [get_spec(n) for n in names] if names else list(TABLE_SPECS)


# ------------------------------------------------------------------ commands


def cmd_seed(args, config) -> int:
    generator = WorkloadGenerator(config, seed=args.seed)
    if not generator.wait_for_database(timeout_seconds=args.wait):
        print("error: could not connect to Postgres", file=sys.stderr)
        return 1
    counts = generator.seed(customers=args.customers, products=args.products)
    print(json.dumps(counts, indent=2))
    return 0


def cmd_workload(args, config) -> int:
    generator = WorkloadGenerator(config, seed=args.seed)
    if not generator.wait_for_database(timeout_seconds=args.wait):
        print("error: could not connect to Postgres", file=sys.stderr)
        return 1
    stats = generator.run(
        duration_seconds=args.duration,
        operations=args.operations,
        rate_per_second=args.rate,
    )
    print(
        json.dumps(
            {
                "inserts": stats.inserts,
                "updates": stats.updates,
                "deletes": stats.deletes,
                "total_row_changes": stats.total,
                "table_counts": generator.counts(),
            },
            indent=2,
        )
    )
    return 0


def cmd_evolve(args, config) -> int:
    generator = WorkloadGenerator(config)
    changed = generator.evolve_schema(column=args.column)
    print(
        f"Added column {args.column!r} to shop.customers and touched rows."
        if changed
        else f"Column {args.column!r} already exists; nothing to do."
    )
    return 0


def cmd_register_connector(args, _config) -> int:
    if not connector_api.wait_for_connect(args.connect_url, timeout_seconds=args.wait):
        print(f"error: Kafka Connect not reachable at {args.connect_url}", file=sys.stderr)
        return 1

    connector_api.register(CONNECTOR_CONFIG, args.connect_url)
    name = json.loads(CONNECTOR_CONFIG.read_text())["name"]
    try:
        state = connector_api.wait_until_running(name, args.connect_url, timeout_seconds=args.wait)
    except (RuntimeError, TimeoutError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(state, indent=2))
    return 0


def cmd_connector_status(args, _config) -> int:
    names = connector_api.list_connectors(args.connect_url)
    if not names:
        print("No connectors registered.")
        return 0
    for name in names:
        print(json.dumps(connector_api.status(name, args.connect_url), indent=2))
    return 0


def cmd_delete_connector(args, _config) -> int:
    name = json.loads(CONNECTOR_CONFIG.read_text())["name"]
    connector_api.delete(name, args.connect_url)
    print(f"Deleted connector {name}.")
    return 0


def cmd_stream(args, config) -> int:
    specs = _specs(args.tables)
    spark = build_spark(config, app_name="tideline-stream")
    try:
        handles = run_streams(
            spark,
            config,
            specs,
            once=args.once,
            starting_offsets=args.starting_offsets,
            timeout_seconds=args.timeout,
        )
        print_summary(handles)
    finally:
        spark.stop()
    return 0


def cmd_simulate(args, config) -> int:
    """Run the CDC path offline: synthetic Debezium events, real merge logic."""
    specs = _specs(args.tables)
    spark = build_spark(config, app_name="tideline-simulate", with_kafka=False)
    try:
        stats = run_simulation(
            spark,
            config,
            specs,
            batches=args.batches,
            ticks_per_batch=args.ticks_per_batch,
            customers=args.customers,
            products=args.products,
            evolve_at_batch=args.evolve_at_batch,
            seed=args.seed,
        )
    finally:
        spark.stop()

    print(
        json.dumps(
            {
                "events": stats.events,
                "delta_commits": stats.batches,
                "rows_merged": stats.merged,
                "collapsed_by_dedup": stats.collapsed,
                "seconds": round(stats.seconds, 2),
                "events_per_second": round(stats.events_per_second, 1),
                "per_table": stats.per_table,
            },
            indent=2,
        )
    )
    return 0


def cmd_maintain(args, config) -> int:
    specs = _specs(args.tables)
    spark = build_spark(config, app_name="tideline-maintenance", with_kafka=False)
    try:
        results = maintain_all(
            spark,
            config,
            specs,
            retain_hours=args.retain_hours,
            target_file_mb=args.target_file_mb,
            skip_vacuum=args.skip_vacuum,
        )
    finally:
        spark.stop()

    if not results:
        print("No Delta tables found. Run the stream first.")
        return 0

    print(f"\n{'table':<16}{'files before':>14}{'files after':>13}{'reduction':>11}{'MiB':>9}")
    print("-" * 63)
    for r in results:
        print(
            f"{r.table:<16}{r.before.files:>14,}{r.after.files:>13,}"
            f"{r.file_reduction * 100:>10.0f}%{r.after.megabytes:>9.1f}"
        )
    print("-" * 63)
    if args.json:
        print(json.dumps([r.to_dict() for r in results], indent=2, default=str))
    return 0


def cmd_stats(args, config) -> int:
    """Row counts and file layout per Delta table — no Spark needed for files."""
    rows: list[dict] = []
    for spec in _specs(args.tables):
        path = config.table_path(spec.name)
        stats = file_stats(path)
        rows.append(
            {
                "table": spec.name,
                "path": path,
                "files": stats.files,
                "small_files": stats.small_files,
                "megabytes": round(stats.megabytes, 2),
                "avg_file_kib": round(stats.avg_file_bytes / 1024, 1),
            }
        )

    print(f"\n{'table':<16}{'files':>8}{'small':>8}{'MiB':>10}{'avg KiB':>10}")
    print("-" * 52)
    for r in rows:
        print(
            f"{r['table']:<16}{r['files']:>8,}{r['small_files']:>8,}"
            f"{r['megabytes']:>10.2f}{r['avg_file_kib']:>10.1f}"
        )
    print("-" * 52)
    if args.json:
        print(json.dumps(rows, indent=2))
    return 0


def cmd_query(args, config) -> int:
    """Run a SQL file or literal against the Delta tables via DuckDB."""
    import duckdb  # noqa: PLC0415

    sql = Path(args.file).read_text() if args.file else args.sql
    if not sql:
        print("error: pass --sql or --file", file=sys.stderr)
        return 2

    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    for spec in TABLE_SPECS:
        path = config.table_path(spec.name)
        if Path(path).exists():
            con.execute(f"CREATE OR REPLACE VIEW {spec.name} AS SELECT * FROM delta_scan('{path}')")

    # Run statements one at a time. `execute()` on a multi-statement string
    # runs them all but returns only the last result set, which silently hides
    # every query in a file but its final one.
    statements = [s.strip() for s in _split_statements(sql) if s.strip()]
    failures = 0

    for number, statement in enumerate(statements, start=1):
        if len(statements) > 1:
            first_line = next(
                (line for line in statement.splitlines() if not line.strip().startswith("--")),
                statement,
            )
            print(f"\n--- [{number}/{len(statements)}] {first_line.strip()[:70]}")
        try:
            result = con.execute(statement)
            columns = [d[0] for d in result.description]
            # Fetching has to be inside the guard too: DuckDB defers plenty of
            # errors (type conversion, missing optional deps) until the rows
            # are actually pulled.
            rows = result.fetchall()
        except Exception as exc:  # noqa: BLE001 - report and continue to the next query
            failures += 1
            print(f"  ERROR: {exc}", file=sys.stderr)
            continue

        print(" | ".join(columns))
        print("-" * (sum(len(c) for c in columns) + 3 * len(columns)))
        for row in rows[: args.limit]:
            print(" | ".join("NULL" if v is None else str(v) for v in row))
        print(f"({len(rows)} rows)")

    return 1 if failures else 0


def _split_statements(sql: str) -> list[str]:
    """Split on the semicolons that actually end a statement.

    Quote tracking alone is not enough: an apostrophe inside a `--` comment
    ("the connector's snapshot") looks like an opening quote and swallows every
    semicolon after it, silently merging the rest of the file into one
    statement. Comments are therefore skipped rather than scanned.
    """
    statements: list[str] = []
    current: list[str] = []
    in_string = False
    in_line_comment = False
    in_block_comment = False
    index = 0

    while index < len(sql):
        char = sql[index]
        pair = sql[index : index + 2]

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
        elif in_block_comment:
            if pair == "*/":
                in_block_comment = False
                current.append(pair)
                index += 2
                continue
        elif in_string:
            # '' is an escaped quote inside a literal, not a close-then-open.
            if pair == "''":
                current.append(pair)
                index += 2
                continue
            if char == "'":
                in_string = False
        else:
            if pair == "--":
                in_line_comment = True
            elif pair == "/*":
                in_block_comment = True
            elif char == "'":
                in_string = True
            elif char == ";":
                statements.append("".join(current))
                current = []
                index += 1
                continue

        current.append(char)
        index += 1

    statements.append("".join(current))
    return statements


def cmd_info(_args, config) -> int:
    print(
        json.dumps(
            {
                "postgres": f"{config.pg_host}:{config.pg_port}/{config.pg_database}",
                "bootstrap_servers": config.bootstrap_servers,
                "lakehouse_root": config.lakehouse_root,
                "checkpoint_root": config.checkpoint_root,
                "tables": {
                    s.name: {
                        "topic": config.topic_for(s.name),
                        "path": config.table_path(s.name),
                        "primary_key": list(s.primary_key),
                        "zorder_by": list(s.zorder_by),
                    }
                    for s in TABLE_SPECS
                },
            },
            indent=2,
        )
    )
    return 0


# ------------------------------------------------------------------- parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tideline", description="CDC streaming lakehouse")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_tables(p):
        p.add_argument("--tables", nargs="*", help="Subset of tables; default all.")

    def add_connect_url(p):
        p.add_argument("--connect-url", default=connector_api.DEFAULT_CONNECT_URL)
        p.add_argument("--wait", type=float, default=120, help="Seconds to wait for readiness.")

    p = sub.add_parser("seed", help="Populate the source database")
    p.add_argument("--customers", type=int, default=500)
    p.add_argument("--products", type=int, default=200)
    p.add_argument("--seed", type=int, default=1337)
    p.add_argument("--wait", type=float, default=60)
    p.set_defaults(func=cmd_seed)

    p = sub.add_parser("workload", help="Drive insert/update/delete traffic")
    p.add_argument("--duration", type=float, default=60, help="Seconds to run.")
    p.add_argument("--operations", type=int, help="Stop after N operations instead.")
    p.add_argument("--rate", type=float, default=20.0, help="Operations per second.")
    p.add_argument("--seed", type=int, default=1337)
    p.add_argument("--wait", type=float, default=60)
    p.set_defaults(func=cmd_workload)

    p = sub.add_parser("evolve", help="Add a column to shop.customers mid-stream")
    p.add_argument("--column", default="loyalty_tier")
    p.set_defaults(func=cmd_evolve)

    p = sub.add_parser("register-connector", help="Register the Debezium connector")
    add_connect_url(p)
    p.set_defaults(func=cmd_register_connector)

    p = sub.add_parser("connector-status", help="Show connector and task state")
    add_connect_url(p)
    p.set_defaults(func=cmd_connector_status)

    p = sub.add_parser("delete-connector", help="Remove the connector")
    add_connect_url(p)
    p.set_defaults(func=cmd_delete_connector)

    p = sub.add_parser("stream", help="Run the Kafka -> Delta streaming job")
    add_tables(p)
    p.add_argument(
        "--once",
        action="store_true",
        help="Drain what is currently in the topics, then stop (Trigger.AvailableNow).",
    )
    p.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    p.add_argument("--timeout", type=float, help="Stop after N seconds.")
    p.set_defaults(func=cmd_stream)

    p = sub.add_parser(
        "simulate",
        help="Run the CDC path offline with synthetic Debezium events (no Docker).",
    )
    add_tables(p)
    p.add_argument("--batches", type=int, default=20, help="Micro-batches to simulate.")
    p.add_argument("--ticks-per-batch", type=int, default=50)
    p.add_argument("--customers", type=int, default=500)
    p.add_argument("--products", type=int, default=200)
    p.add_argument(
        "--evolve-at-batch",
        type=int,
        help="Add a column to customers at this batch, to demonstrate schema evolution.",
    )
    p.add_argument("--seed", type=int, default=1337)
    p.set_defaults(func=cmd_simulate)

    p = sub.add_parser("maintain", help="Compact, Z-order and vacuum the Delta tables")
    add_tables(p)
    p.add_argument("--retain-hours", type=int, default=DEFAULT_RETAIN_HOURS)
    p.add_argument("--target-file-mb", type=int, default=128)
    p.add_argument("--skip-vacuum", action="store_true")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_maintain)

    p = sub.add_parser("stats", help="File layout of each Delta table")
    add_tables(p)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_stats)

    p = sub.add_parser("query", help="Query the lakehouse with DuckDB")
    p.add_argument("--sql")
    p.add_argument("--file")
    p.add_argument("--limit", type=int, default=50)
    p.set_defaults(func=cmd_query)

    p = sub.add_parser("info", help="Print resolved configuration")
    p.set_defaults(func=cmd_info)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _logging(args.verbose)
    try:
        return args.func(args, get_config())
    except (FileNotFoundError, ValueError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
