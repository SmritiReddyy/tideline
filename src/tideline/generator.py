"""OLTP workload generator.

Drives realistic traffic against the source Postgres so there is something for
Debezium to capture. The mix matters more than the volume: a pipeline that only
ever sees inserts will happily pass tests and then corrupt the table the first
time a row is updated twice in one micro-batch.

Default mix, per tick:
  * new orders with line items      — inserts across three tables
  * order status transitions        — the update-heavy path
  * inventory adjustments           — updates on a composite key
  * occasional order cancellations  — deletes, with cascade to line items
  * occasional customer edits       — updates on the table the ALTER hits
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass

import psycopg

from .config import Config

log = logging.getLogger(__name__)

COUNTRIES = ("US", "GB", "DE", "FR", "BR", "JP", "CA", "AU", "IN", "NL")
STATUS_FLOW = ("pending", "paid", "packed", "shipped", "delivered")
FIRST_NAMES = ("ada", "alan", "grace", "linus", "barbara", "edsger", "ken", "margaret")
LAST_NAMES = ("lovelace", "turing", "hopper", "torvalds", "liskov", "dijkstra", "thompson")


@dataclass
class WorkloadStats:
    inserts: int = 0
    updates: int = 0
    deletes: int = 0

    @property
    def total(self) -> int:
        return self.inserts + self.updates + self.deletes


class WorkloadGenerator:
    def __init__(self, config: Config, seed: int = 1337) -> None:
        self.config = config
        self.rng = random.Random(seed)
        self.stats = WorkloadStats()

    def connect(self) -> psycopg.Connection:
        return psycopg.connect(self.config.pg_dsn, autocommit=True)

    # ------------------------------------------------------------- seeding

    def seed(self, customers: int = 500, products: int = 200, warehouses: int = 3) -> dict:
        """Populate the baseline state the connector will snapshot."""
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM shop.customers")
            if cur.fetchone()[0] > 0:
                log.info("source database already seeded")
                return self.counts()

            cur.executemany(
                "INSERT INTO shop.customers (email, full_name, country) VALUES (%s, %s, %s)",
                [
                    (
                        f"user{i}@example.com",
                        f"{self.rng.choice(FIRST_NAMES)} {self.rng.choice(LAST_NAMES)}",
                        self.rng.choice(COUNTRIES),
                    )
                    for i in range(customers)
                ],
            )

            cur.executemany(
                "INSERT INTO shop.inventory (product_id, warehouse_id, quantity, reorder_level) "
                "VALUES (%s, %s, %s, %s)",
                [
                    (p, w, self.rng.randint(0, 500), self.rng.choice((5, 10, 25)))
                    for p in range(1, products + 1)
                    for w in range(1, warehouses + 1)
                ],
            )
            self.stats.inserts += customers + products * warehouses

        log.info("seeded %s customers, %s inventory rows", customers, products * warehouses)
        return self.counts()

    # ----------------------------------------------------------- operations

    def _place_order(self, cur) -> None:
        cur.execute("SELECT customer_id FROM shop.customers ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if not row:
            return
        customer_id = row[0]

        cur.execute(
            "INSERT INTO shop.orders (customer_id, status, total_amount, currency) "
            "VALUES (%s, 'pending', 0, 'USD') RETURNING order_id",
            (customer_id,),
        )
        order_id = cur.fetchone()[0]
        self.stats.inserts += 1

        total = 0.0
        for _ in range(self.rng.choices([1, 2, 3, 4], weights=[60, 22, 12, 6])[0]):
            quantity = self.rng.randint(1, 5)
            unit_price = round(self.rng.uniform(5, 400), 2)
            total += quantity * unit_price
            cur.execute(
                "INSERT INTO shop.order_items (order_id, product_id, quantity, unit_price) "
                "VALUES (%s, %s, %s, %s)",
                (order_id, self.rng.randint(1, 200), quantity, unit_price),
            )
            self.stats.inserts += 1

        # A second write to the same row moments after the insert — this is the
        # pattern that produces two changes for one key inside one micro-batch.
        cur.execute(
            "UPDATE shop.orders SET total_amount = %s, updated_at = now() WHERE order_id = %s",
            (round(total, 2), order_id),
        )
        self.stats.updates += 1

    def _advance_status(self, cur) -> None:
        cur.execute(
            "SELECT order_id, status FROM shop.orders "
            "WHERE status <> 'delivered' AND status <> 'cancelled' "
            "ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return
        order_id, status = row
        try:
            nxt = STATUS_FLOW[STATUS_FLOW.index(status) + 1]
        except (ValueError, IndexError):
            return
        cur.execute(
            "UPDATE shop.orders SET status = %s, updated_at = now() WHERE order_id = %s",
            (nxt, order_id),
        )
        self.stats.updates += 1

    def _adjust_inventory(self, cur) -> None:
        cur.execute(
            "UPDATE shop.inventory SET quantity = GREATEST(0, quantity + %s), updated_at = now() "
            "WHERE (product_id, warehouse_id) = ("
            "  SELECT product_id, warehouse_id FROM shop.inventory ORDER BY random() LIMIT 1)",
            (self.rng.randint(-20, 30),),
        )
        self.stats.updates += 1

    def _cancel_order(self, cur) -> None:
        """Delete an order. ON DELETE CASCADE also removes its line items, so
        one statement produces deletes on two topics."""
        cur.execute(
            "DELETE FROM shop.orders WHERE order_id = ("
            "  SELECT order_id FROM shop.orders WHERE status = 'pending' "
            "  ORDER BY random() LIMIT 1)"
        )
        if cur.rowcount:
            self.stats.deletes += cur.rowcount

    def _edit_customer(self, cur) -> None:
        cur.execute(
            "UPDATE shop.customers SET country = %s, updated_at = now() "
            "WHERE customer_id = ("
            "  SELECT customer_id FROM shop.customers ORDER BY random() LIMIT 1)",
            (self.rng.choice(COUNTRIES),),
        )
        self.stats.updates += 1

    # -------------------------------------------------------------- driving

    def tick(self, cur) -> None:
        """One unit of workload, weighted to look like a real shop."""
        action = self.rng.choices(
            ("order", "status", "inventory", "cancel", "customer"),
            weights=(30, 40, 20, 5, 5),
            k=1,
        )[0]
        {
            "order": self._place_order,
            "status": self._advance_status,
            "inventory": self._adjust_inventory,
            "cancel": self._cancel_order,
            "customer": self._edit_customer,
        }[action](cur)

    def run(
        self,
        *,
        duration_seconds: float | None = 60,
        operations: int | None = None,
        rate_per_second: float = 20.0,
    ) -> WorkloadStats:
        """Drive traffic for a duration or a fixed number of operations."""
        interval = 1.0 / rate_per_second if rate_per_second > 0 else 0.0
        deadline = time.monotonic() + duration_seconds if duration_seconds else None
        performed = 0

        with self.connect() as conn, conn.cursor() as cur:
            while True:
                if operations is not None and performed >= operations:
                    break
                if deadline and time.monotonic() >= deadline:
                    break

                self.tick(cur)
                performed += 1

                if performed % 200 == 0:
                    log.info("%s ticks, %s row changes", performed, self.stats.total)
                if interval:
                    time.sleep(interval)

        log.info(
            "workload complete: %s inserts, %s updates, %s deletes",
            self.stats.inserts,
            self.stats.updates,
            self.stats.deletes,
        )
        return self.stats

    # ------------------------------------------------------ schema evolution

    def evolve_schema(self, column: str = "loyalty_tier") -> bool:
        """Add a column to `shop.customers` while the stream is running.

        This is the schema-evolution demo. Debezium picks the new column up from
        the WAL's relation message automatically, later change events carry it,
        and the Spark job's per-batch schema inference plus Delta's schema
        evolution widen the target table — with no redeploy and no downtime.
        """
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema='shop' AND table_name='customers' AND column_name=%s",
                (column,),
            )
            if cur.fetchone():
                log.info("column %s already exists", column)
                return False

            cur.execute(f"ALTER TABLE shop.customers ADD COLUMN {column} TEXT")
            # Touch some rows so change events actually carrying the new column
            # are produced; an ALTER alone emits nothing to the WAL for existing rows.
            cur.execute(
                "UPDATE shop.customers SET "
                f"{column} = (ARRAY['bronze','silver','gold'])[1 + floor(random()*3)], "
                "updated_at = now() "
                "WHERE customer_id IN (SELECT customer_id FROM shop.customers "
                "                      ORDER BY random() LIMIT 100)"
            )
            self.stats.updates += cur.rowcount

        log.info("added column %s and touched %s rows", column, 100)
        return True

    # -------------------------------------------------------------- reporting

    def counts(self) -> dict:
        with self.connect() as conn, conn.cursor() as cur:
            counts = {}
            for table in ("customers", "orders", "order_items", "inventory"):
                cur.execute(f"SELECT count(*) FROM shop.{table}")
                counts[table] = cur.fetchone()[0]
            return counts

    def wait_for_database(self, timeout_seconds: float = 60) -> bool:
        """Block until Postgres accepts connections, for container startup."""
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            try:
                with self.connect() as conn, conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return True
            except psycopg.OperationalError:
                time.sleep(1)
        return False
