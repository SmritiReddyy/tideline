"""Correctness tests for the CDC merge path.

Each test targets a specific way a CDC pipeline silently corrupts a table:
duplicate keys, resurrected deletes, stale overwrites from out-of-order events,
dropped columns after a schema change. These are the failures that do not throw
— the table just quietly stops matching the source.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from tests.conftest import debezium_event
from tideline.cdc import (
    TableSpec,
    build_change_stream,
    deduplicate_batch,
    flatten_changes,
    get_spec,
    process_batch,
)

ORDERS = TableSpec(name="orders", primary_key=("order_id",), zorder_by=("customer_id",))
ORDERS_HARD = TableSpec(name="orders", primary_key=("order_id",), hard_delete=True)
INVENTORY = TableSpec(name="inventory", primary_key=("product_id", "warehouse_id"))


def _order(order_id: int, status: str, amount: float = 100.0, customer_id: int = 1) -> dict:
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "status": status,
        "amount": amount,
    }


def _read(spark, path):
    return spark.read.format("delta").load(path)


def _rows(spark, path, *, include_deleted: bool = False):
    df = _read(spark, path)
    if not include_deleted and "_deleted" in df.columns:
        df = df.filter(~F.col("_deleted"))
    return {r["order_id"]: r.asDict() for r in df.collect()}


# ------------------------------------------------------------------ envelope


def test_parses_all_operation_types(spark, make_batch):
    batch = make_batch(
        [
            ("1", debezium_event("r", lsn=1, after=_order(1, "new"), snapshot="true")),
            ("2", debezium_event("c", lsn=2, after=_order(2, "new"))),
            ("3", debezium_event("u", lsn=3, after=_order(3, "paid"), before=_order(3, "new"))),
            ("4", debezium_event("d", lsn=4, before=_order(4, "paid"), after=None)),
        ]
    )
    parsed = build_change_stream(batch).orderBy("_lsn").collect()

    assert [r["_op"] for r in parsed] == ["r", "c", "u", "d"]
    assert [r["_lsn"] for r in parsed] == [1, 2, 3, 4]
    assert [r["_is_snapshot"] for r in parsed] == [True, False, False, False]
    # A delete carries its row image in `before`; the parser must find it there.
    assert '"order_id":4' in parsed[3]["_row_json"].replace(" ", "")


def test_tombstones_are_dropped(spark, make_batch):
    """Kafka's null-valued tombstone exists for log compaction, not consumers.

    Letting one through would produce a row with every column null.
    """
    batch = make_batch(
        [
            ("1", debezium_event("d", lsn=1, before=_order(1, "paid"))),
            ("1", None),  # the tombstone Debezium emits straight after
        ]
    )
    parsed = build_change_stream(batch).collect()
    assert len(parsed) == 1
    assert parsed[0]["_op"] == "d"
