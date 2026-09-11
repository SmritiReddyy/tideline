"""Tests for configuration, topic naming, and the offline simulator.

The simulator is the thing CI relies on to exercise the CDC path without
Docker, so it needs to be verified to actually produce Debezium-shaped events —
otherwise the CDC tests would be validating against a fiction.
"""

from __future__ import annotations

import json

import pytest

from tideline.cdc import TABLE_SPECS, TableSpec
from tideline.config import Config
from tideline.connector import load_connector_config
from tideline.envelope import ENVELOPE_SCHEMA, SOURCE_SCHEMA
from tideline.simulate import ChangeEventSimulator, run_simulation

REPO_ROOT = __import__("pathlib").Path(__file__).resolve().parents[1]


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


# ------------------------------------------------------------------- config


def test_topic_names_match_debezium_convention(lakehouse):
    """Debezium publishes to <server>.<schema>.<table>. A mismatch here means
    the stream subscribes to a topic that will never receive anything."""
    config = _config(lakehouse)
    assert config.topic_for("orders") == "tideline.shop.orders"
    assert config.topic_for("inventory") == "tideline.shop.inventory"


def test_paths_are_derived_per_table(lakehouse):
    config = _config(lakehouse)
    assert config.table_path("orders").endswith("/orders")
    assert config.checkpoint_path("orders") != config.table_path("orders")


def test_config_reads_environment(monkeypatch):
    monkeypatch.setenv("TIDELINE_PG_HOST", "db.internal")
    monkeypatch.setenv("TIDELINE_TOPIC_PREFIX", "prod")
    monkeypatch.setenv("TIDELINE_SHUFFLE_PARTITIONS", "64")
    config = Config.from_env()
    assert config.pg_host == "db.internal"
    assert config.topic_for("orders") == "prod.shop.orders"
    assert config.shuffle_partitions == 64


# ---------------------------------------------------------------- connector


def test_connector_config_strips_documentation_keys():
    """The committed JSON carries `//` keys explaining each setting. Kafka
    Connect would reject them, so they must not survive the load."""
    definition = load_connector_config(REPO_ROOT / "connectors" / "postgres-source.json")
    assert definition["name"] == "tideline-postgres-source"
    assert not any(k.startswith("//") for k in definition["config"])


def test_connector_config_declares_the_settings_the_pipeline_depends_on():
    config = load_connector_config(REPO_ROOT / "connectors" / "postgres-source.json")["config"]

    # pgoutput needs no server-side extension, unlike wal2json.
    assert config["plugin.name"] == "pgoutput"
    # Snapshot then stream; 'never' would skip every pre-existing row.
    assert config["snapshot.mode"] == "initial"
    # The pipeline reads before-images and the LSN, so the envelope must stay intact.
    assert "transforms" not in config
    # Silently skipping a bad event means silent divergence from the source.
    assert config["errors.tolerance"] == "none"
    # Topic prefix must line up with what Config builds.
    assert config["topic.prefix"] == "tideline"


# ---------------------------------------------------------------- envelope


def test_envelope_schema_carries_the_ordering_key():
    """`source.lsn` is the only trustworthy ordering key; losing it from the
    schema would silently disable the out-of-order guard."""
    assert "lsn" in SOURCE_SCHEMA.fieldNames()
    assert "op" in ENVELOPE_SCHEMA.fieldNames()


# --------------------------------------------------------------- simulator


def test_simulator_emits_valid_debezium_envelopes(lakehouse):
    simulator = ChangeEventSimulator(_config(lakehouse))
    simulator.snapshot_customers(5)
    events = simulator.place_order()

    for table_events in events.values():
        for _key, value in table_events:
            payload = json.loads(value)
            assert set(payload) == {"before", "after", "source", "op", "ts_ms"}
            assert payload["op"] in ("c", "u", "d", "r")
            assert payload["source"]["lsn"] > 0
            assert payload["source"]["table"] in {s.name for s in TABLE_SPECS}


def test_simulator_lsn_is_globally_unique_and_ordered_per_table(lakehouse):
    """The two properties the merge actually depends on.

    A Postgres LSN is unique across the whole database, and Debezium preserves
    that order *within* each topic. Across topics the interleaving is arbitrary
    — a single transaction inserting an order and its line items emits into two
    topics, and nothing orders those against each other. The merge only ever
    compares LSNs within one table, so per-table monotonicity plus global
    uniqueness is exactly the right contract to assert.
    """
    simulator = ChangeEventSimulator(_config(lakehouse))
    simulator.snapshot_customers(10)

    per_table: dict[str, list[int]] = {}
    for _ in range(50):
        for table, table_events in simulator.tick().items():
            for _key, value in table_events:
                per_table.setdefault(table, []).append(json.loads(value)["source"]["lsn"])

    all_lsns = [lsn for lsns in per_table.values() for lsn in lsns]
    assert len(set(all_lsns)) == len(all_lsns), "LSNs must be globally unique"

    for table, lsns in per_table.items():
        assert lsns == sorted(lsns), f"{table} LSNs are out of order within the topic"


def test_simulator_deletes_target_existing_rows(lakehouse):
    """A delete for a key that was never inserted would make the pipeline look
    correct while testing nothing."""
    simulator = ChangeEventSimulator(_config(lakehouse))
    simulator.snapshot_customers(5)
    for _ in range(30):
        simulator.place_order()

    deletes = 0
    for _ in range(200):
        events = simulator.cancel_order()
        for _key, value in events.get("orders", []):
            payload = json.loads(value)
            assert payload["op"] == "d"
            assert payload["before"] is not None
            assert payload["after"] is None
            deletes += 1
    assert deletes > 0


def test_simulator_evolution_adds_the_column_to_later_events(lakehouse):
    simulator = ChangeEventSimulator(_config(lakehouse))
    simulator.snapshot_customers(20)

    before = simulator.edit_customer()["customers"][0][1]
    assert "loyalty_tier" not in json.loads(before)["after"]

    simulator.evolve()
    after = simulator.edit_customer()["customers"][0][1]
    assert "loyalty_tier" in json.loads(after)["after"]


def test_simulation_writes_every_table(spark, lakehouse):
    """End-to-end smoke test of the offline path CI depends on."""
    config = _config(lakehouse)
    stats = run_simulation(
        spark,
        config,
        list(TABLE_SPECS),
        batches=3,
        ticks_per_batch=15,
        customers=40,
        products=20,
        warehouses=2,
        evolve_at_batch=1,
    )

    assert stats.events > 0
    assert stats.batches > 0

    for spec in TABLE_SPECS:
        count = spark.read.format("delta").load(config.table_path(spec.name)).count()
        assert count > 0, f"{spec.name} received no rows"

    # The evolved column must have reached the Delta table.
    customers = spark.read.format("delta").load(config.table_path("customers"))
    assert "loyalty_tier" in customers.columns


def test_simulation_dedup_actually_collapses_events(spark, lakehouse):
    """`place_order` writes an order then immediately updates it, so a batch
    always contains multiple changes per key. If nothing collapses, the dedup
    is not running."""
    config = _config(lakehouse)
    stats = run_simulation(
        spark,
        config,
        [s for s in TABLE_SPECS if s.name == "orders"],
        batches=3,
        ticks_per_batch=25,
        customers=30,
        products=10,
        warehouses=1,
    )
    assert stats.collapsed > 0


# -------------------------------------------------------------------- specs


def test_every_spec_has_a_usable_merge_condition():
    for spec in TABLE_SPECS:
        condition = spec.merge_condition()
        for column in spec.primary_key:
            assert f"t.`{column}` = s.`{column}`" in condition


def test_merge_condition_handles_composite_keys():
    spec = TableSpec(name="x", primary_key=("a", "b"))
    assert spec.merge_condition() == "t.`a` = s.`a` AND t.`b` = s.`b`"


@pytest.mark.parametrize("name", ["orders", "order_items", "inventory", "customers"])
def test_specs_declare_zorder_columns(name):
    """Compaction without clustering columns leaves most of the win on the table."""
    spec = next(s for s in TABLE_SPECS if s.name == name)
    assert spec.zorder_by, f"{name} has no Z-order columns"


# ------------------------------------------------------- SQL file splitting


def test_split_statements_ignores_apostrophes_in_comments():
    """A `--` comment containing "connector's" used to swallow every following
    semicolon, silently merging the rest of a .sql file into one statement."""
    from tideline.cli import _split_statements

    sql = "-- the connector's snapshot\nselect 1;\nselect 2;"
    assert len([s for s in _split_statements(sql) if s.strip()]) == 2


@pytest.mark.parametrize(
    ("label", "sql"),
    [
        ("semicolon inside a literal", "select 'a;b' as x;\nselect 2;"),
        ("escaped quote", "select 'it''s' as x;\nselect 2;"),
        ("block comment", "/* it's fine; really */ select 1;\nselect 2;"),
        ("trailing semicolon", "select 1;\nselect 2;\n"),
    ],
)
def test_split_statements_edge_cases(label, sql):
    from tideline.cli import _split_statements

    assert len([s for s in _split_statements(sql) if s.strip()]) == 2, label


def test_analytics_sql_has_no_reserved_word_aliases():
    """`value`, `rows` and `lines` are reserved in DuckDB; using one as a bare
    alias makes the whole query file fail to parse."""
    import re

    sql = (REPO_ROOT / "query" / "queries" / "analytics.sql").read_text()
    offenders = re.findall(r"\bas (value|rows|lines)\b", sql, flags=re.IGNORECASE)
    assert not offenders, f"reserved words used as aliases: {offenders}"


def test_evolve_emits_events_carrying_the_new_column(lakehouse):
    """An ALTER on its own writes nothing to the WAL for existing rows, so the
    new column would never reach Kafka. `evolve()` must produce the follow-up
    updates itself rather than waiting for the 5%-weighted random customer edit
    to fire — which at small batch counts it may never do."""
    simulator = ChangeEventSimulator(_config(lakehouse))
    simulator.snapshot_customers(60)

    events = simulator.evolve(touch_rows=25)
    customer_events = events["customers"]
    assert len(customer_events) == 25

    for _key, value in customer_events:
        payload = json.loads(value)
        assert payload["op"] == "u"
        assert payload["after"]["loyalty_tier"] in ("bronze", "silver", "gold")
        # The before-image must predate the column, or the change is not a change.
        assert "loyalty_tier" not in payload["before"]


def test_evolve_touch_count_is_capped_by_population(lakehouse):
    simulator = ChangeEventSimulator(_config(lakehouse))
    simulator.snapshot_customers(5)
    assert len(simulator.evolve(touch_rows=40)["customers"]) == 5
