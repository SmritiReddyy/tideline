"""Tests for the compaction / Z-order / vacuum path.

The small-file problem is easy to talk about and easy to get wrong, so these
build a genuinely fragmented table — one commit per micro-batch, exactly as the
stream produces — and assert the maintenance job actually fixes it.
"""

from __future__ import annotations

from delta.tables import DeltaTable

from tests.conftest import debezium_event
from tideline.cdc import TableSpec, build_change_stream, process_batch
from tideline.config import Config
from tideline.maintenance import (
    describe_history,
    file_stats,
    maintain_all,
    maintain_table,
    optimize_table,
)

ORDERS = TableSpec(name="orders", primary_key=("order_id",), zorder_by=("customer_id",))
NO_ZORDER = TableSpec(name="plain", primary_key=("order_id",))


def _config(lakehouse: str) -> Config:
    return Config(
        pg_host="localhost",
        pg_port=5432,
        pg_database="tideline",
        pg_user="t",
        pg_password="t",
        bootstrap_servers="localhost:19092",
        topic_prefix="tideline",
        pg_schema="shop",
        lakehouse_root=lakehouse,
        checkpoint_root=f"{lakehouse}/_checkpoints",
        driver_memory="1g",
        shuffle_partitions=2,
        max_offsets_per_trigger=1000,
        trigger_interval="5 seconds",
    )


def _fragment(spark, make_batch, spec, path, batches: int = 12, per_batch: int = 5) -> None:
    """Write many small commits, the way a 10-second trigger would."""
    lsn = 0
    for b in range(batches):
        events = []
        for i in range(per_batch):
            lsn += 1
            order_id = b * per_batch + i
            events.append(
                (
                    str(order_id),
                    debezium_event(
                        "c",
                        lsn=lsn,
                        after={
                            "order_id": order_id,
                            "customer_id": order_id % 7,
                            "status": "new",
                            "amount": 10.0 * order_id,
                        },
                    ),
                )
            )
        process_batch(spark, build_change_stream(make_batch(events)), spec, path)


def test_streaming_writes_produce_many_small_files(spark, make_batch, lakehouse):
    """Establishes the problem before testing the fix."""
    path = f"{lakehouse}/orders"
    _fragment(spark, make_batch, ORDERS, path, batches=10)

    stats = file_stats(path)
    assert stats.files >= 10, "expected fragmentation from per-batch commits"
    assert stats.small_files == stats.files, "all files should be well under 16 MiB"


def test_optimize_reduces_file_count(spark, make_batch, lakehouse):
    path = f"{lakehouse}/orders_opt"
    _fragment(spark, make_batch, ORDERS, path, batches=12)

    before = file_stats(path)
    rows_before = spark.read.format("delta").load(path).count()

    optimize_table(spark, path, ORDERS)
    after_optimize = spark.read.format("delta").load(path)

    # Compaction must not change what the table contains.
    assert after_optimize.count() == rows_before

    # OPTIMIZE leaves the old files on disk until VACUUM; the *active* file
    # count is what the query engine reads, so measure that.
    active = len(after_optimize.inputFiles())
    assert active < before.files, f"active files {active} not below {before.files}"


def test_full_maintenance_cycle_reclaims_files(spark, make_batch, lakehouse):
    """OPTIMIZE then VACUUM: the second is what actually reclaims disk."""
    path = f"{lakehouse}/orders_full"
    config = _config(lakehouse)
    spec = TableSpec(name="orders_full", primary_key=("order_id",), zorder_by=("customer_id",))

    _fragment(spark, make_batch, spec, path, batches=14)
    rows_before = spark.read.format("delta").load(path).count()

    # retain_hours=0 vacuums everything unreferenced immediately. Only safe
    # because nothing else is reading; in production this is the time-travel
    # horizon and must not be zero.
    result = maintain_table(spark, config, spec, retain_hours=0)

    assert result.before.files > result.after.files
    assert result.file_reduction > 0
    assert spark.read.format("delta").load(path).count() == rows_before
    assert result.zordered_by == ("customer_id",)


def test_zorder_and_plain_compaction_both_work(spark, make_batch, lakehouse):
    """A table with no Z-order columns must still compact."""
    path = f"{lakehouse}/plain"
    _fragment(spark, make_batch, NO_ZORDER, path, batches=8)

    rows_before = spark.read.format("delta").load(path).count()
    optimize_table(spark, path, NO_ZORDER)

    df = spark.read.format("delta").load(path)
    assert df.count() == rows_before
    assert len(df.inputFiles()) < 8


def test_maintenance_skips_missing_tables(spark, lakehouse):
    """A table that has never received data should be skipped, not fatal."""
    config = _config(lakehouse)
    results = maintain_all(spark, config, [TableSpec(name="never_streamed", primary_key=("id",))])
    assert results == []


def test_history_is_preserved_through_optimize(spark, make_batch, lakehouse):
    """Compaction adds a commit; it must not erase the ones before it."""
    path = f"{lakehouse}/orders_hist"
    _fragment(spark, make_batch, ORDERS, path, batches=5)

    versions_before = describe_history(spark, path).count()
    optimize_table(spark, path, ORDERS)
    versions_after = describe_history(spark, path).count()

    assert versions_after == versions_before + 1
    operations = [r["operation"] for r in DeltaTable.forPath(spark, path).history().collect()]
    assert "OPTIMIZE" in operations


def test_file_stats_on_missing_path_is_empty(tmp_path):
    stats = file_stats(str(tmp_path / "nope"))
    assert stats.files == 0
    assert stats.bytes == 0
