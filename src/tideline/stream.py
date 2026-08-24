"""Spark Structured Streaming job: Kafka topics -> Delta tables.

One query per table rather than one query fanning out to all of them. It costs
a little more driver memory and buys three things worth more than that:

* **Independent checkpoints.** A schema problem on `customers` does not stall
  `orders`.
* **Independent restart.** One table can be replayed from an earlier offset
  without touching the others.
* **Per-table backpressure.** `maxOffsetsPerTrigger` is tuned per topic instead
  of being a single number that suits none of them.

Exactly-once, concretely: Structured Streaming records the Kafka offsets it
processed in the checkpoint *before* committing the batch, and the Delta MERGE
is guarded on `_lsn`. A crash between the two replays the batch, and the replay
is a no-op because every event in it loses the LSN comparison. That combination
— checkpointed offsets plus an idempotent sink — is what makes the end-to-end
guarantee, not either half alone.
"""

from __future__ import annotations

import contextlib
import json
import logging
import signal
import sys
import threading
import time
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from .cdc import BatchStats, TableSpec, build_change_stream, process_batch
from .config import Config

log = logging.getLogger(__name__)


@dataclass
class StreamHandle:
    spec: TableSpec
    query: StreamingQuery
    stats: BatchStats


def read_kafka_topic(
    spark: SparkSession,
    config: Config,
    spec: TableSpec,
    *,
    starting_offsets: str = "earliest",
) -> DataFrame:
    """Streaming read of one Debezium topic.

    `startingOffsets` only applies the first time a checkpoint is created; on
    restart the checkpoint wins. That is the desired behaviour — it is what
    stops a redeploy from silently reprocessing the whole topic.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", config.topic_for(spec.name))
        .option("startingOffsets", starting_offsets)
        # Bound the batch so the initial snapshot does not arrive as one
        # enormous micro-batch that blows the driver's heap.
        .option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
        # A topic deleted and recreated, or a retention expiry, would otherwise
        # kill the query. Log it and carry on from the earliest available offset.
        .option("failOnDataLoss", "false")
        .load()
    )


def start_table_stream(
    spark: SparkSession,
    config: Config,
    spec: TableSpec,
    *,
    starting_offsets: str = "earliest",
    once: bool = False,
) -> StreamHandle:
    """Launch the streaming query for one table."""
    stats = BatchStats()
    table_path = config.table_path(spec.name)
    checkpoint = config.checkpoint_path(spec.name)

    source = read_kafka_topic(spark, config, spec, starting_offsets=starting_offsets)
    changes = build_change_stream(source)

    def handle_batch(batch: DataFrame, batch_id: int) -> None:
        started = time.monotonic()
        summary = process_batch(spark, batch, spec, table_path)
        elapsed = time.monotonic() - started
        stats.record(summary)
        if summary.get("events"):
            log.info(
                "%s batch %s: %s events -> %s rows merged in %.2fs (%s collapsed)",
                spec.name,
                batch_id,
                summary["events"],
                summary.get("merged", 0),
                elapsed,
                summary.get("collapsed", 0),
            )

    writer = (
        changes.writeStream.queryName(f"tideline-{spec.name}")
        .foreachBatch(handle_batch)
        .option("checkpointLocation", checkpoint)
        .outputMode("update")
    )

    # `availableNow` drains everything currently in the topic and stops. It is
    # what makes the pipeline testable and schedulable, rather than only ever
    # being a process you have to remember to kill.
    trigger = {"availableNow": True} if once else {"processingTime": config.trigger_interval}
    query = writer.trigger(**trigger).start()

    log.info(
        "started stream %s: %s -> %s (checkpoint %s)",
        spec.name,
        config.topic_for(spec.name),
        table_path,
        checkpoint,
    )
    return StreamHandle(spec=spec, query=query, stats=stats)


def run_streams(
    spark: SparkSession,
    config: Config,
    specs: list[TableSpec],
    *,
    once: bool = False,
    starting_offsets: str = "earliest",
    timeout_seconds: float | None = None,
) -> list[StreamHandle]:
    """Start every table's stream and wait for them.

    Handles SIGINT/SIGTERM so a Ctrl-C or a container stop drains cleanly
    instead of leaving a half-written checkpoint behind.
    """
    handles = [
        start_table_stream(spark, config, spec, starting_offsets=starting_offsets, once=once)
        for spec in specs
    ]

    stopping = threading.Event()

    def _shutdown(signum, _frame):
        log.info("signal %s received; stopping streams", signum)
        stopping.set()
        for handle in handles:
            try:
                handle.query.stop()
            except Exception as exc:  # noqa: BLE001 - best effort on the way out
                log.warning("error stopping %s: %s", handle.spec.name, exc)

    for sig in (signal.SIGINT, signal.SIGTERM):
        # Raises ValueError off the main thread (e.g. under an Airflow worker),
        # where the caller owns signal handling instead.
        with contextlib.suppress(ValueError):
            signal.signal(sig, _shutdown)

    deadline = time.monotonic() + timeout_seconds if timeout_seconds else None
    try:
        while not stopping.is_set():
            active = [h for h in handles if h.query.isActive]
            if not active:
                break
            if deadline and time.monotonic() > deadline:
                log.info("timeout reached; stopping streams")
                _shutdown("timeout", None)
                break
            for handle in active:
                exception = handle.query.exception()
                if exception:
                    log.error("stream %s failed: %s", handle.spec.name, exception)
                    _shutdown("failure", None)
                    raise RuntimeError(f"stream {handle.spec.name} failed") from None
            time.sleep(1)
    finally:
        for handle in handles:
            if handle.query.isActive:
                handle.query.stop()

    return handles


def summarise(handles: list[StreamHandle]) -> dict:
    """Throughput summary, for the CLI and the metrics doc."""
    return {
        "tables": {
            h.spec.name: {
                "batches": h.stats.batches,
                "events": h.stats.events,
                "merged": h.stats.merged,
                "collapsed": h.stats.collapsed,
                "last_progress": _last_progress(h.query),
            }
            for h in handles
        },
        "total_events": sum(h.stats.events for h in handles),
        "total_merged": sum(h.stats.merged for h in handles),
    }


def _last_progress(query: StreamingQuery) -> dict | None:
    try:
        progress = query.lastProgress
    except Exception:  # noqa: BLE001 - the query may already be torn down
        return None
    if not progress:
        return None
    return {
        "batchId": progress.get("batchId"),
        "inputRowsPerSecond": progress.get("inputRowsPerSecond"),
        "processedRowsPerSecond": progress.get("processedRowsPerSecond"),
        "durationMs": progress.get("durationMs"),
    }


def print_summary(handles: list[StreamHandle]) -> None:
    summary = summarise(handles)
    print(json.dumps(summary, indent=2, default=str), file=sys.stderr)
