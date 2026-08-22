"""Debezium change-event envelope handling.

Debezium wraps every row change in an envelope:

    {
      "before": {...} | null,      # row state before the change
      "after":  {...} | null,      # row state after the change
      "source": {"lsn": 123, "table": "orders", "ts_ms": ..., "snapshot": ...},
      "op":     "c" | "u" | "d" | "r",
      "ts_ms":  1699999999999
    }

`op` is create / update / delete / read (the last being a snapshot row emitted
during the connector's initial backfill).

**The envelope is consumed raw, not unwrapped.** It is common to configure
Debezium's `ExtractNewRecordState` transform so Kafka carries only the `after`
image. That throws away three things this pipeline needs: the `before` image
for deletes, the transaction LSN needed to order changes correctly, and the
distinction between a snapshot read and a live insert. Handling the full
envelope costs a little parsing and buys correctness.

`after` and `before` are deliberately kept as raw JSON strings here rather than
being parsed against a fixed struct. A fixed struct would silently drop any
column added to the source table mid-stream, which is exactly the scenario the
schema-evolution path has to survive. The payload schema is instead inferred
per micro-batch in `cdc.py`.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

# Operation codes as Debezium emits them.
OP_CREATE = "c"
OP_UPDATE = "u"
OP_DELETE = "d"
OP_READ = "r"  # snapshot row
OP_TRUNCATE = "t"

# Ops that mean "this row exists with these values".
UPSERT_OPS = (OP_CREATE, OP_UPDATE, OP_READ)

# Columns this pipeline adds to every target table. Prefixed so they cannot
# collide with a source column, and carried into the Delta table so the merge
# can reason about ordering and so consumers can audit lineage.
CDC_METADATA_COLUMNS = (
    "_op",
    "_lsn",
    "_source_ts_ms",
    "_event_ts_ms",
    "_ingested_at",
    "_is_snapshot",
    "_deleted",
)

# `source.lsn` is a Postgres log sequence number: monotonically increasing
# within a database, and the only trustworthy ordering key for CDC. Wall-clock
# timestamps are not — two changes can share a millisecond, and a connector
# restart can emit them out of order.
SOURCE_SCHEMA = StructType(
    [
        StructField("version", StringType(), True),
        StructField("connector", StringType(), True),
        StructField("name", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("snapshot", StringType(), True),
        StructField("db", StringType(), True),
        StructField("schema", StringType(), True),
        StructField("table", StringType(), True),
        StructField("txId", LongType(), True),
        StructField("lsn", LongType(), True),
    ]
)

ENVELOPE_SCHEMA = StructType(
    [
        # Kept as strings; parsed later against a per-batch inferred schema.
        StructField("before", StringType(), True),
        StructField("after", StringType(), True),
        StructField("source", SOURCE_SCHEMA, True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
    ]
)


def _envelope_json_column(value_column: str = "value") -> Column:
    """Reparse `before`/`after` as strings regardless of the writer's shape.

    `from_json` with a `StringType` field would fail on a nested object, so the
    two payload fields are pulled out with `get_json_object`, which returns the
    raw JSON text of a subtree.
    """
    return F.col(value_column)


def parse_envelope(
    df: DataFrame,
    *,
    value_column: str = "value",
    key_column: str = "key",
) -> DataFrame:
    """Turn raw Kafka records into one normalised change row each.

    Emits the envelope metadata as typed columns and leaves the row image in
    `_after_json` / `_before_json` for schema-flexible parsing downstream.

    Tombstones — the null-valued record Kafka uses to mark a key deletable —
    are dropped. The preceding delete event already carries everything needed;
    the tombstone exists for log compaction, not for consumers.
    """
    raw = df.withColumn("_value_str", F.col(value_column).cast("string"))

    # Drop tombstones and any empty payload.
    raw = raw.filter(F.col("_value_str").isNotNull() & (F.length(F.col("_value_str")) > 0))

    envelope = raw.withColumn(
        "_envelope",
        F.from_json(
            F.col("_value_str"),
            StructType(
                [
                    StructField("source", SOURCE_SCHEMA, True),
                    StructField("op", StringType(), True),
                    StructField("ts_ms", LongType(), True),
                ]
            ),
        ),
    )

    parsed = envelope.select(
        F.col(key_column).cast("string").alias("_key"),
        F.get_json_object(F.col("_value_str"), "$.after").alias("_after_json"),
        F.get_json_object(F.col("_value_str"), "$.before").alias("_before_json"),
        F.col("_envelope.op").alias("_op"),
        F.coalesce(F.col("_envelope.source.lsn"), F.lit(0)).cast("long").alias("_lsn"),
        F.col("_envelope.source.ts_ms").alias("_source_ts_ms"),
        F.col("_envelope.ts_ms").alias("_event_ts_ms"),
        F.col("_envelope.source.table").alias("_source_table"),
        # Debezium marks snapshot rows "true"/"last"/"first"; anything non-null
        # and not "false" means this row came from the initial backfill.
        (
            F.col("_envelope.source.snapshot").isNotNull()
            & (F.col("_envelope.source.snapshot") != F.lit("false"))
        ).alias("_is_snapshot"),
    )

    # A delete carries its row image in `before`, not `after`. Collapsing the
    # two here means every downstream stage sees one "row image" column and
    # does not have to branch on the operation.
    return parsed.withColumn(
        "_row_json",
        F.when(F.col("_op") == F.lit(OP_DELETE), F.col("_before_json")).otherwise(
            F.col("_after_json")
        ),
    ).filter(F.col("_row_json").isNotNull())


def is_delete(op_column: str = "_op") -> Column:
    return F.col(op_column) == F.lit(OP_DELETE)


def is_upsert(op_column: str = "_op") -> Column:
    return F.col(op_column).isin(list(UPSERT_OPS))
