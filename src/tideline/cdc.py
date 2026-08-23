"""The core of Tideline: turning a stream of change events into a table.

Three problems have to be solved together, and getting any one of them wrong
produces a lakehouse table that silently disagrees with the source database.

**1. Several changes to one row inside one micro-batch.**
A row inserted and then updated twice in the same second arrives as three
events in the same batch. Handing all three to a Delta `MERGE` raises
`multiple source rows matched`, because MERGE requires at most one source row
per target row. The batch must therefore be collapsed to the *latest* change
per key first.

**2. Events arriving out of order.**
A connector restart, a partition rebalance, or a retry can deliver an older
change after a newer one. Applying it blindly would resurrect stale values. The
merge is guarded on the Postgres LSN, so an older change is a no-op.

**3. Schema changes mid-stream.**
A column added to the source table appears in later events and not earlier
ones. Parsing against a fixed struct would silently drop it. The row image is
kept as JSON and its schema inferred per batch, then Delta's schema evolution
widens the target table.

Deletes are applied as **soft deletes** by default — the row stays with
`_deleted = true` — because a hard delete makes "what did this table look like
last Tuesday" unanswerable even with time travel, and most analytical consumers
would rather filter than lose the record. `hard_delete=True` switches to
removing the row.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

from .envelope import OP_DELETE, parse_envelope

log = logging.getLogger(__name__)


@dataclass
class TableSpec:
    """How one source table maps onto one Delta table."""

    name: str
    primary_key: tuple[str, ...]
    # Column the Delta table is partitioned on. Optional: over-partitioning a
    # streaming table is the fastest route to the small-file problem.
    partition_by: tuple[str, ...] = ()
    # Columns worth Z-ordering on during compaction — the ones queries filter by.
    zorder_by: tuple[str, ...] = ()
    hard_delete: bool = False
    # Explicit payload schema. Leave unset to infer per batch, which is what
    # makes schema evolution work without a redeploy.
    schema: StructType | None = None
    comment: str = ""

    @property
    def topic(self) -> str:
        """Debezium publishes one topic per table: <server>.<schema>.<table>."""
        return self.name

    def merge_condition(self, target: str = "t", source: str = "s") -> str:
        return " AND ".join(f"{target}.`{c}` = {source}.`{c}`" for c in self.primary_key)


def infer_payload_schema(spark: SparkSession, batch: DataFrame) -> StructType | None:
    """Infer the row-image schema from this micro-batch's JSON.

    This is what lets a column added to the source table appear in the Delta
    table on the next batch, with no redeploy and no fixed schema to maintain.
    The cost is one extra pass over the batch's JSON; the alternative is
    silently dropping the new column.
    """
    payloads = batch.select(F.col("_row_json").alias("value")).filter(F.col("value").isNotNull())
    # PySpark's JSON reader takes a path or an RDD of strings, not a DataFrame.
    json_rdd = payloads.rdd.map(lambda row: row[0])
    if json_rdd.isEmpty():
        return None

    schema = spark.read.json(json_rdd).schema
    # A batch of unparseable JSON yields a lone `_corrupt_record` column; treat
    # that as "no usable schema" rather than writing a garbage table.
    if not schema.fields or all(f.name == "_corrupt_record" for f in schema.fields):
        return None
    return schema


def flatten_changes(
    spark: SparkSession,
    batch: DataFrame,
    spec: TableSpec,
) -> DataFrame | None:
    """Expand the JSON row image into real columns and attach CDC metadata."""
    schema = spec.schema or infer_payload_schema(spark, batch)
    if schema is None:
        return None

    parsed = batch.withColumn("_row", F.from_json(F.col("_row_json"), schema))

    payload_columns = [
        F.col(f"_row.`{f.name}`").alias(f.name)
        for f in schema.fields
        # A source column colliding with a metadata column would be shadowed
        # silently; drop it loudly instead.
        if not f.name.startswith("_")
    ]

    return parsed.select(
        *payload_columns,
        F.col("_op"),
        F.col("_lsn"),
        F.col("_source_ts_ms"),
        F.col("_event_ts_ms"),
        F.col("_is_snapshot"),
        (F.col("_op") == F.lit(OP_DELETE)).alias("_deleted"),
        F.current_timestamp().alias("_ingested_at"),
    )


def deduplicate_batch(changes: DataFrame, spec: TableSpec) -> DataFrame:
    """Collapse to the latest change per primary key.

    Ordering is by LSN first — the only monotonic ordering Postgres gives us —
    then by event timestamp, then by operation so a delete wins a tie against
    an update at the same position. Without this a MERGE against a batch
    containing two changes to one row fails outright.
    """
    ordering = [
        F.col("_lsn").desc(),
        F.col("_event_ts_ms").desc_nulls_last(),
        # Final tie-break: a delete beats an update at the same position.
        # Ordering on `_op` would not do this — 'd' sorts *before* 'u', so
        # descending would pick the update and resurrect a deleted row.
        # `_deleted` (true > false) states the intent directly.
        F.col("_deleted").desc(),
    ]
    window = Window.partitionBy(*[F.col(c) for c in spec.primary_key]).orderBy(*ordering)

    return (
        changes.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
    )


def merge_into_delta(
    spark: SparkSession,
    changes: DataFrame,
    spec: TableSpec,
    table_path: str,
) -> None:
    """Apply one deduplicated batch to the Delta table.

    Creates the table on first sight, then merges. `_lsn` guards every update:
    an event older than what the table already holds is skipped rather than
    overwriting newer state.
    """
    if not DeltaTable.isDeltaTable(spark, table_path):
        log.info("creating Delta table for %s at %s", spec.name, table_path)
        writer = changes.write.format("delta").mode("overwrite")
        if spec.partition_by:
            writer = writer.partitionBy(*spec.partition_by)
        writer.save(table_path)
        return

    target = DeltaTable.forPath(spark, table_path)
    condition = spec.merge_condition()

    # Only apply a change strictly newer than what is stored. This is the
    # out-of-order guard, and it is also what makes replaying the stream from
    # an earlier offset harmless: replayed events lose the comparison.
    newer = "s._lsn > t._lsn"

    merge = target.alias("t").merge(changes.alias("s"), condition)

    if spec.hard_delete:
        merge = merge.whenMatchedDelete(condition=f"s._deleted = true AND {newer}")
    merge = merge.whenMatchedUpdateAll(condition=newer)

    # A delete for a row that was never seen is dropped rather than inserted as
    # a tombstone — inserting it would invent a row the source never had.
    merge = merge.whenNotMatchedInsertAll(condition="s._deleted = false")

    merge.execute()


def process_batch(
    spark: SparkSession,
    batch: DataFrame,
    spec: TableSpec,
    table_path: str,
) -> dict:
    """Full path for one micro-batch: parse, flatten, dedupe, merge.

    Returns a small summary so the stream can log throughput without a second
    pass over the data.
    """
    # Caching matters here: the batch is read once to infer the schema and
    # again to merge. Without it the Kafka source would be consumed twice.
    batch = batch.cache()
    try:
        raw_count = batch.count()
        if raw_count == 0:
            return {"table": spec.name, "events": 0, "merged": 0}

        changes = flatten_changes(spark, batch, spec)
        if changes is None:
            return {"table": spec.name, "events": raw_count, "merged": 0}

        deduped = deduplicate_batch(changes, spec).cache()
        try:
            merged = deduped.count()
            merge_into_delta(spark, deduped, spec, table_path)
        finally:
            deduped.unpersist()

        return {
            "table": spec.name,
            "events": raw_count,
            "merged": merged,
            "collapsed": raw_count - merged,
        }
    finally:
        batch.unpersist()


def build_change_stream(
    kafka_df: DataFrame,
    *,
    value_column: str = "value",
    key_column: str = "key",
) -> DataFrame:
    """Kafka records -> normalised change rows. Thin wrapper for readability."""
    return parse_envelope(kafka_df, value_column=value_column, key_column=key_column)


# ---------------------------------------------------------------- table specs

# The OLTP schema Tideline captures. Partitioning is chosen conservatively:
# a streaming table partitioned too finely produces thousands of tiny files an
# hour, which is the exact problem the compaction job exists to clean up.
TABLE_SPECS: tuple[TableSpec, ...] = (
    TableSpec(
        name="orders",
        primary_key=("order_id",),
        partition_by=("status",),
        zorder_by=("customer_id", "order_id"),
        comment="Order headers. Status changes drive most of the update traffic.",
    ),
    TableSpec(
        name="order_items",
        primary_key=("order_item_id",),
        zorder_by=("order_id", "product_id"),
        comment="Order lines. Insert-heavy.",
    ),
    TableSpec(
        name="inventory",
        primary_key=("product_id", "warehouse_id"),
        zorder_by=("product_id",),
        comment="Stock levels. Update-heavy; the composite key exercises multi-column merges.",
    ),
    TableSpec(
        name="customers",
        primary_key=("customer_id",),
        zorder_by=("customer_id",),
        comment="Customer records. The table the schema-evolution demo alters.",
    ),
)

SPECS_BY_NAME: dict[str, TableSpec] = {s.name: s for s in TABLE_SPECS}


def get_spec(name: str) -> TableSpec:
    try:
        return SPECS_BY_NAME[name]
    except KeyError:
        known = ", ".join(sorted(SPECS_BY_NAME))
        raise KeyError(f"Unknown table {name!r}. Known: {known}") from None


@dataclass
class BatchStats:
    """Rolling counters the stream reports as it runs."""

    batches: int = 0
    events: int = 0
    merged: int = 0
    collapsed: int = 0
    per_table: dict = field(default_factory=dict)

    def record(self, summary: dict) -> None:
        self.batches += 1
        self.events += summary.get("events", 0)
        self.merged += summary.get("merged", 0)
        self.collapsed += summary.get("collapsed", 0)
        table = summary.get("table", "unknown")
        entry = self.per_table.setdefault(table, {"events": 0, "merged": 0})
        entry["events"] += summary.get("events", 0)
        entry["merged"] += summary.get("merged", 0)
