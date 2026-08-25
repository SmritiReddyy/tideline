"""Offline simulation: Debezium-shaped events without Postgres or Kafka.

The full stack needs four containers. That is the right way to run this, but it
makes the pipeline awkward to test in CI and impossible to demonstrate on a
machine without Docker.

This module generates change events **in the exact envelope Debezium emits** and
feeds them through the real `process_batch` path — the same parsing, the same
deduplication, the same LSN-guarded MERGE, the same Delta tables. Only the
transport is different. Anything proven here about correctness holds for the
containerised pipeline; what it cannot exercise is Debezium's WAL decoding and
Kafka's delivery semantics, which is stated plainly rather than glossed over.

It also produces a realistically fragmented table, which is what makes the
compaction and query-performance numbers in docs/METRICS.md measurable.
"""

from __future__ import annotations

import json
import logging
import random
import time
from dataclasses import dataclass, field

from pyspark.sql import SparkSession

from .cdc import TableSpec, build_change_stream, process_batch
from .config import Config
from .generator import COUNTRIES, STATUS_FLOW

log = logging.getLogger(__name__)


@dataclass
class SimulationStats:
    events: int = 0
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    batches: int = 0
    merged: int = 0
    collapsed: int = 0
    seconds: float = 0.0
    per_table: dict = field(default_factory=dict)

    @property
    def events_per_second(self) -> float:
        return self.events / self.seconds if self.seconds else 0.0


class ChangeEventSimulator:
    """Produces Debezium envelopes for the Tideline schema."""

    def __init__(self, config: Config, seed: int = 1337) -> None:
        self.config = config
        self.rng = random.Random(seed)
        self.lsn = 0
        self.base_ts = 1_700_000_000_000

        # Mirror of the source database's state, so updates and deletes target
        # rows that actually exist — an update for a key never inserted would
        # make the pipeline look correct while testing nothing.
        self.orders: dict[int, dict] = {}
        self.customers: dict[int, dict] = {}
        self.inventory: dict[tuple[int, int], dict] = {}
        self.next_order_id = 1
        self.next_item_id = 1
        self.evolved = False

    # ------------------------------------------------------------ envelope

    def _envelope(
        self,
        table: str,
        op: str,
        after: dict | None,
        before: dict | None = None,
        snapshot: str | None = None,
    ) -> tuple[str, str]:
        self.lsn += 1
        key = json.dumps({"id": (after or before or {}).get("_key", self.lsn)})
        payload = {
            "before": before,
            "after": after,
            "source": {
                "version": "2.7.3.Final",
                "connector": "postgresql",
                "name": self.config.topic_prefix,
                "ts_ms": self.base_ts + self.lsn,
                "snapshot": snapshot,
                "db": self.config.pg_database,
                "schema": self.config.pg_schema,
                "table": table,
                "txId": self.lsn,
                "lsn": self.lsn,
            },
            "op": op,
            "ts_ms": self.base_ts + self.lsn + 1,
        }
        return key, json.dumps(payload)

    # -------------------------------------------------------------- events

    def snapshot_customers(self, n: int) -> list[tuple[str, str]]:
        events = []
        for customer_id in range(1, n + 1):
            row = {
                "customer_id": customer_id,
                "email": f"user{customer_id}@example.com",
                "full_name": f"customer {customer_id}",
                "country": self.rng.choice(COUNTRIES),
            }
            self.customers[customer_id] = row
            events.append(
                self._envelope(
                    "customers",
                    "r",
                    row,
                    snapshot="last" if customer_id == n else "true",
                )
            )
        return events

    def snapshot_inventory(self, products: int, warehouses: int) -> list[tuple[str, str]]:
        events = []
        for product_id in range(1, products + 1):
            for warehouse_id in range(1, warehouses + 1):
                row = {
                    "product_id": product_id,
                    "warehouse_id": warehouse_id,
                    "quantity": self.rng.randint(0, 500),
                    "reorder_level": self.rng.choice((5, 10, 25)),
                }
                self.inventory[(product_id, warehouse_id)] = row
                events.append(self._envelope("inventory", "r", row, snapshot="true"))
        return events

    def place_order(self) -> dict[str, list[tuple[str, str]]]:
        if not self.customers:
            return {}
        order_id = self.next_order_id
        self.next_order_id += 1
        customer_id = self.rng.choice(list(self.customers))

        order = {
            "order_id": order_id,
            "customer_id": customer_id,
            "status": "pending",
            "total_amount": 0.0,
            "currency": "USD",
        }
        events = {"orders": [self._envelope("orders", "c", order)], "order_items": []}

        total = 0.0
        for _ in range(self.rng.choices([1, 2, 3, 4], weights=[60, 22, 12, 6])[0]):
            quantity = self.rng.randint(1, 5)
            unit_price = round(self.rng.uniform(5, 400), 2)
            total += quantity * unit_price
            item = {
                "order_item_id": self.next_item_id,
                "order_id": order_id,
                "product_id": self.rng.randint(1, 200),
                "quantity": quantity,
                "unit_price": unit_price,
            }
            self.next_item_id += 1
            events["order_items"].append(self._envelope("order_items", "c", item))

        # Immediate follow-up update on the row just inserted: this is what
        # produces two changes for one key inside a single micro-batch.
        before = dict(order)
        order = {**order, "total_amount": round(total, 2)}
        self.orders[order_id] = order
        events["orders"].append(self._envelope("orders", "u", order, before))
        return events

    def advance_status(self) -> dict[str, list[tuple[str, str]]]:
        candidates = [
            o
            for o in self.orders.values()
            if o["status"] in STATUS_FLOW and o["status"] != STATUS_FLOW[-1]
        ]
        if not candidates:
            return {}
        order = self.rng.choice(candidates)
        before = dict(order)
        order["status"] = STATUS_FLOW[STATUS_FLOW.index(order["status"]) + 1]
        return {"orders": [self._envelope("orders", "u", dict(order), before)]}

    def adjust_inventory(self) -> dict[str, list[tuple[str, str]]]:
        if not self.inventory:
            return {}
        key = self.rng.choice(list(self.inventory))
        row = self.inventory[key]
        before = dict(row)
        row["quantity"] = max(0, row["quantity"] + self.rng.randint(-20, 30))
        return {"inventory": [self._envelope("inventory", "u", dict(row), before)]}

    def cancel_order(self) -> dict[str, list[tuple[str, str]]]:
        pending = [o for o in self.orders.values() if o["status"] == "pending"]
        if not pending:
            return {}
        order = self.rng.choice(pending)
        del self.orders[order["order_id"]]
        return {"orders": [self._envelope("orders", "d", None, dict(order))]}

    def edit_customer(self) -> dict[str, list[tuple[str, str]]]:
        if not self.customers:
            return {}
        customer_id = self.rng.choice(list(self.customers))
        row = self.customers[customer_id]
        before = dict(row)
        row["country"] = self.rng.choice(COUNTRIES)
        if self.evolved:
            row["loyalty_tier"] = self.rng.choice(("bronze", "silver", "gold"))
        return {"customers": [self._envelope("customers", "u", dict(row), before)]}

    def evolve(self, column: str = "loyalty_tier") -> None:
        """Simulate `ALTER TABLE shop.customers ADD COLUMN ...` mid-stream.

        From here on, customer events carry the extra field — exactly what
        Debezium emits once the WAL relation message changes.
        """
        self.evolved = True
        self.column = column
        log.info("schema evolved: customers now carry %r", column)

    def tick(self) -> dict[str, list[tuple[str, str]]]:
        action = self.rng.choices(
            ("order", "status", "inventory", "cancel", "customer"),
            weights=(30, 40, 20, 5, 5),
            k=1,
        )[0]
        return {
            "order": self.place_order,
            "status": self.advance_status,
            "inventory": self.adjust_inventory,
            "cancel": self.cancel_order,
            "customer": self.edit_customer,
        }[action]()


def run_simulation(
    spark: SparkSession,
    config: Config,
    specs: list[TableSpec],
    *,
    batches: int = 20,
    ticks_per_batch: int = 50,
    customers: int = 500,
    products: int = 200,
    warehouses: int = 3,
    evolve_at_batch: int | None = None,
    seed: int = 1337,
) -> SimulationStats:
    """Drive the full CDC path offline, one micro-batch at a time.

    Each batch is a separate `process_batch` call and therefore a separate Delta
    commit, which reproduces the small-file fragmentation a real stream causes.
    """
    simulator = ChangeEventSimulator(config, seed=seed)
    specs_by_name = {s.name: s for s in specs}
    stats = SimulationStats()
    started = time.monotonic()

    def flush(table_events: dict[str, list[tuple[str, str]]]) -> None:
        for table, events in table_events.items():
            spec = specs_by_name.get(table)
            if not spec or not events:
                continue
            frame = spark.createDataFrame(events, "key string, value string")
            summary = process_batch(
                spark, build_change_stream(frame), spec, config.table_path(table)
            )
            stats.batches += 1
            stats.merged += summary.get("merged", 0)
            stats.collapsed += summary.get("collapsed", 0)
            entry = stats.per_table.setdefault(table, {"events": 0, "merged": 0})
            entry["events"] += summary.get("events", 0)
            entry["merged"] += summary.get("merged", 0)

    # Initial snapshot, mirroring the connector's `snapshot.mode: initial`.
    log.info("simulating connector snapshot")
    snapshot = {
        "customers": simulator.snapshot_customers(customers),
        "inventory": simulator.snapshot_inventory(products, warehouses),
    }
    stats.events += sum(len(v) for v in snapshot.values())
    flush(snapshot)

    # Streaming phase.
    for batch_number in range(batches):
        if evolve_at_batch is not None and batch_number == evolve_at_batch:
            simulator.evolve()

        batch: dict[str, list[tuple[str, str]]] = {}
        for _ in range(ticks_per_batch):
            for table, events in simulator.tick().items():
                batch.setdefault(table, []).extend(events)

        stats.events += sum(len(v) for v in batch.values())
        flush(batch)
        if (batch_number + 1) % 5 == 0:
            log.info("batch %s/%s, %s events so far", batch_number + 1, batches, stats.events)

    stats.seconds = time.monotonic() - started
    log.info(
        "simulation complete: %s events in %.1fs (%.0f events/s) across %s commits",
        stats.events,
        stats.seconds,
        stats.events_per_second,
        stats.batches,
    )
    return stats
