-- Representative analytical queries against the Tideline lakehouse.
--
-- Run them with:
--     tideline query --file query/queries/analytics.sql
-- which registers each Delta table as a DuckDB view via delta_scan().
--
-- Two conventions apply throughout:
--
--   * `where not _deleted` — deletes are soft by default, so a row deleted in
--     the source is still present with `_deleted = true`. Forgetting this
--     filter is the single easiest way to report numbers that quietly include
--     cancelled orders.
--
--   * `_lsn` is the source-database log position. It is the correct way to ask
--     "how fresh is this table?" — more reliable than a wall-clock timestamp,
--     which can move backwards across a connector restart.

-- ---------------------------------------------------------------------------
-- 1. Live order book by status
-- ---------------------------------------------------------------------------
select
    status,
    count(*)                        as orders,
    round(sum(total_amount), 2)     as revenue,
    round(avg(total_amount), 2)     as avg_order_value
from orders
where not _deleted
group by status
order by revenue desc;

-- ---------------------------------------------------------------------------
-- 2. Order detail joined to its line items
-- ---------------------------------------------------------------------------
select
    o.order_id,
    o.status,
    o.total_amount,
    count(i.order_item_id)                          as line_count,
    round(sum(i.quantity * i.unit_price), 2)        as computed_total,
    -- Should be ~0. A persistent gap means line items and the order header
    -- fell out of sync, which is exactly the class of bug CDC introduces when
    -- two topics are merged independently.
    round(o.total_amount - sum(i.quantity * i.unit_price), 2) as variance
from orders o
join order_items i on o.order_id = i.order_id
where not o._deleted and not i._deleted
group by o.order_id, o.status, o.total_amount
having abs(variance) > 0.01
order by abs(variance) desc
limit 20;

-- ---------------------------------------------------------------------------
-- 3. Inventory needing replenishment
-- ---------------------------------------------------------------------------
select
    product_id,
    warehouse_id,
    quantity,
    reorder_level,
    reorder_level - quantity as shortfall
from inventory
where not _deleted
  and quantity < reorder_level
order by shortfall desc
limit 25;

-- ---------------------------------------------------------------------------
-- 4. Pipeline freshness, per table
--
-- `_lsn` is the highest source log position this table has absorbed, and
-- `_ingested_at` is when Spark wrote it. The gap between the two is the
-- end-to-end lag.
-- ---------------------------------------------------------------------------
select 'orders' as table_name, max(_lsn) as max_lsn, max(_ingested_at) as last_write
from orders
union all
select 'order_items', max(_lsn), max(_ingested_at) from order_items
union all
select 'inventory',   max(_lsn), max(_ingested_at) from inventory
union all
select 'customers',   max(_lsn), max(_ingested_at) from customers
order by table_name;

-- ---------------------------------------------------------------------------
-- 5. CDC operation mix
--
-- 'r' rows came from the connector's initial snapshot; 'c'/'u'/'d' are live
-- changes. A table still showing only 'r' means streaming never started.
-- ---------------------------------------------------------------------------
select
    _op,
    count(*)                as event_rows,
    sum(case when _deleted then 1 else 0 end) as deleted_rows
from orders
group by _op
order by event_rows desc;

-- ---------------------------------------------------------------------------
-- 6. Schema evolution: rows carrying the column added mid-stream
-- ---------------------------------------------------------------------------
select
    coalesce(loyalty_tier, '(added after this row was written)') as loyalty_tier,
    count(*) as customers
from customers
where not _deleted
group by 1
order by customers desc;
