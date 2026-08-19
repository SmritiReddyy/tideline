-- Tideline source OLTP schema.
--
-- Deliberately small and ordinary — the interesting part of this project is
-- what happens downstream of the WAL, not the schema itself. What it does need
-- is a mix of workloads: an insert-heavy table, an update-heavy table, one with
-- a composite primary key, and one that gets an ALTER mid-stream.

CREATE SCHEMA IF NOT EXISTS shop;

-- ---------------------------------------------------------------- customers
-- The table the schema-evolution demo alters.
CREATE TABLE shop.customers (
    customer_id   BIGSERIAL PRIMARY KEY,
    email         TEXT        NOT NULL UNIQUE,
    full_name     TEXT        NOT NULL,
    country       TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------------- orders
-- Update-heavy: every order walks through a status lifecycle, so this is where
-- most of the CDC update traffic comes from.
CREATE TABLE shop.orders (
    order_id      BIGSERIAL PRIMARY KEY,
    customer_id   BIGINT      NOT NULL REFERENCES shop.customers(customer_id),
    status        TEXT        NOT NULL DEFAULT 'pending',
    total_amount  NUMERIC(12,2) NOT NULL DEFAULT 0,
    currency      TEXT        NOT NULL DEFAULT 'USD',
    placed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON shop.orders (customer_id);
CREATE INDEX ON shop.orders (status);

-- -------------------------------------------------------------- order_items
-- Insert-heavy, rarely updated.
CREATE TABLE shop.order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id      BIGINT      NOT NULL REFERENCES shop.orders(order_id) ON DELETE CASCADE,
    product_id    BIGINT      NOT NULL,
    quantity      INTEGER     NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(12,2) NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON shop.order_items (order_id);

-- ---------------------------------------------------------------- inventory
-- Composite primary key, which is what exercises multi-column MERGE
-- conditions downstream. Almost every event on this table is an update.
CREATE TABLE shop.inventory (
    product_id    BIGINT      NOT NULL,
    warehouse_id  INTEGER     NOT NULL,
    quantity      INTEGER     NOT NULL DEFAULT 0,
    reorder_level INTEGER     NOT NULL DEFAULT 10,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id, warehouse_id)
);

-- ------------------------------------------------------------------ replica
-- REPLICA IDENTITY FULL makes Postgres write the *entire* pre-image of a row
-- into the WAL on UPDATE and DELETE, not just the primary key. Without it,
-- Debezium's `before` field on a delete contains only the key columns — which
-- is enough to delete a row but not enough to audit what was deleted, and not
-- enough to reconstruct state if the delete needs reversing.
--
-- The cost is real: a wider WAL and more replication traffic. On these tables
-- it is worth it; on a very hot, very wide table it might not be.
ALTER TABLE shop.customers   REPLICA IDENTITY FULL;
ALTER TABLE shop.orders      REPLICA IDENTITY FULL;
ALTER TABLE shop.order_items REPLICA IDENTITY FULL;
ALTER TABLE shop.inventory   REPLICA IDENTITY FULL;

-- A dedicated publication rather than FOR ALL TABLES, so adding a table to the
-- database does not silently start streaming it.
CREATE PUBLICATION tideline_pub FOR TABLE
    shop.customers,
    shop.orders,
    shop.order_items,
    shop.inventory;

-- Replication user. Debezium needs REPLICATION plus read access.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
        CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA shop TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA shop TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA shop GRANT SELECT ON TABLES TO debezium;
