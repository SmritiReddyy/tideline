# Metrics and how they were measured

Reproducible, with the method stated so the numbers can be argued with.

**Environment:** Apple Silicon laptop, 8 cores, 8 GB RAM, macOS. Spark 3.5.3
local mode with a 2 GB driver, Delta Lake 3.2.1, OpenJDK 17, Python 3.10.
DuckDB 1.x with the `delta` extension for query timing.

```bash
make clean && make benchmark        # everything below
```

---

## CDC throughput and correctness

From `scripts/benchmark.py` — 20 micro-batches, 40 workload ticks each, plus a
connector snapshot of 500 customers and 600 inventory rows.

| Measure | Value |
|---|---|
| Change events processed | 2,514 |
| Delta commits | 80 |
| Rows merged into Delta | 2,184 |
| **Events collapsed by intra-batch dedup** | **330** |
| Tables written | 4 |

The 330 collapsed events are the important figure. Each is a case where one
primary key received more than one change inside a single micro-batch — the
workload generator inserts an order and immediately updates its total, which is
what real applications do. Every one of them would have failed the Delta MERGE
with *multiple source rows matched* without the deduplication step.

**A note on throughput.** The simulation processes roughly 20 events/second,
which sounds slow and is not a measure of the pipeline's capacity. It is
dominated by fixed per-micro-batch Spark overhead — 80 separate MERGE commits,
each with job submission, planning, and a Delta transaction. Batch size is the
lever, not data volume: the same code with 10,000 events per batch does not take
500× longer. Quoting this as an events/second capacity figure would be
misleading, so it is not quoted as one.

## The small-file problem

Fragmentation after 80 streaming commits, then after OPTIMIZE + Z-ORDER + VACUUM:

| | Before | After | Change |
|---|---|---|---|
| Parquet files | 180 | 8 | **−96%** |
| On-disk size | 0.98 MiB | 0.07 MiB | −93% |

Per table:

| Table | Files before | Files after | Reduction | Z-ordered on |
|---|---|---|---|---|
| `orders` | 100 | 5 | 95.0% | `customer_id`, `order_id` |
| `order_items` | 26 | 1 | 96.2% | `order_id`, `product_id` |
| `inventory` | 28 | 1 | 96.4% | `product_id` |
| `customers` | 26 | 1 | 96.2% | `customer_id` |

`orders` retains five files rather than one because it is partitioned by
`status`, and Delta cannot compact across partitions. That is correct, and it
illustrates why a streaming table should be partitioned conservatively — fine
partitioning multiplies the small-file problem it is meant to help with.

The 93% size reduction is mostly the elimination of per-file Parquet overhead:
180 files each carrying their own footer, schema, and dictionary pages, for a
few kilobytes of actual data apiece.

## Query latency

Median of 7 runs after a warm-up, DuckDB `delta_scan` over the same Delta log
and Parquet files, with a fresh connection per query so the Delta log cache is
not being measured instead of the file layout.

| Query | Fragmented (180 files) | Compacted (8 files) | Speedup |
|---|---|---|---|
| Point lookup by key | 3.62 ms | 2.08 ms | 1.74× |
| Filter on Z-order column | 3.68 ms | 2.34 ms | 1.57× |
| Aggregate by status | 4.92 ms | 2.75 ms | 1.79× |
| Join orders → line items | 10.30 ms | 6.25 ms | 1.65× |
| Inventory below reorder level | 1.65 ms | 1.98 ms | **0.83×** |
| Full scan count | 3.13 ms | 1.80 ms | 1.74× |
| **Total** | **27.30 ms** | **17.21 ms** | **1.59×** |

One query is slower after compaction. At under 2 ms it sits close to the
measurement floor, where per-run variance exceeds the effect being measured. It
is reported rather than dropped: a benchmark that reports only its wins is not
a benchmark.

**Read this as a demonstration of the mechanism, not a production claim.** The
dataset is small, so both sides are fast in absolute terms and the win comes
from opening 8 files instead of 180. The effect scales with file count, not
data size — a table with 50,000 small files behaves qualitatively differently,
and that is the case compaction is actually for. On object storage the gap is
larger again, because each file open is a network round trip rather than a page
cache hit.

## Testing

| Suite | Tests | What it covers |
|---|---|---|
| `test_cdc.py` | 17 | envelope parsing, tombstones, dedup, composite keys, insert/update/delete, hard delete, out-of-order guard, replay idempotency, unseen deletes, snapshot→live, schema evolution, time travel |
| `test_maintenance.py` | 7 | fragmentation, OPTIMIZE, Z-ORDER, VACUUM, data integrity, history preservation |
| `test_config_and_simulate.py` | 24 | topic naming, connector config, envelope schema, simulator fidelity, LSN contract, SQL statement splitting |
| **Total** | **48** | all passing |
| DAG import validation | 1 DAG, 9 tasks | separate CI job |

Correctness properties asserted rather than assumed:

| Property | Test |
|---|---|
| Replaying a batch changes nothing | `test_replaying_the_same_batch_is_idempotent` |
| A stale event cannot overwrite newer state | `test_out_of_order_event_does_not_overwrite_newer_state` |
| A delete beats an update at the same LSN | `test_delete_wins_over_update_at_the_same_lsn` |
| A new source column reaches Delta with no redeploy | `test_new_column_appears_without_redeploy` |
| Compaction does not change what the table contains | `test_full_maintenance_cycle_reclaims_files` |
| Compaction does not erase commit history | `test_history_is_preserved_through_optimize` |
| A delete for an unseen key does not invent a row | `test_delete_for_unseen_row_is_ignored` |

## Schema evolution

Measured on the benchmark run above, where the column is added at the halfway
batch:

| Measure | Value |
|---|---|
| Column added mid-stream | `customers.loyalty_tier` |
| Pipeline restarts required | **0** |
| Rows written before the change | 478, `loyalty_tier` null |
| Rows carrying the new column | 22 |
| Column survives compaction | yes |

## Verified against the real stack

The figures above come from the offline simulator. The pipeline was also run
end to end on the containerised stack — Postgres 16 → Debezium 2.7.3.Final →
Redpanda 24.2.7 → Spark 3.5.3 → Delta 3.2.1 → Trino 468 — and the results below
are from that run.

**Throughput and dedup, on real Debezium events**

| Measure | Value |
|---|---|
| Change events consumed from Kafka | 2,857 |
| Rows merged into Delta | 1,859 |
| **Collapsed by intra-batch dedup** | **998** (678 on `orders` alone) |
| Workload driven against Postgres | 759 inserts, 862 updates, 49 deletes |

678 collapsed events on a single table in one batch: without deduplication the
Delta MERGE would have failed on the very first micro-batch.

**Correctness — Delta vs Postgres, value by value**

| Check | Result |
|---|---|
| Live row counts, all four tables | exact match (500 / 239 / 395 / 600) |
| `orders` status + amount mismatches | **0** |
| `inventory` quantity mismatches (composite key) | **0** |
| Rows in Postgres missing from Delta | **0** |
| Rows in Delta not in Postgres | **0** |
| Soft-deleted orders still live in Postgres | **0** |
| Soft deletes recorded | 49 orders + 87 cascaded line items — exactly the workload's deletes |

**End-to-end latency** — `INSERT ... COMMIT` in Postgres to the row being
queryable in Delta, with a 5-second Spark trigger, n=6:

| min | median | max |
|---|---|---|
| 6.09 s | **7.49 s** | 8.72 s |

Roughly the trigger interval plus ~2.5 s of merge. The trigger dominates, so
this is a tuning choice rather than a processing limit.

**Idempotency under replay** — the strongest exactly-once evidence:

| Scenario | Events reprocessed | Rows changed |
|---|---|---|
| Restart from existing checkpoint | 0 | 0 |
| **Checkpoints deleted, full replay from earliest** | **2,968** | **0** |

Row counts *and* `sum(_lsn)` checksums were byte-identical before and after
replaying every event in every topic.

**Schema evolution, live** — `ALTER TABLE shop.customers ADD COLUMN
loyalty_tier` while the stream was running: the column reached Delta within one
trigger interval, 100 touched rows carried values, 400 pre-existing rows stayed
null, and the streaming process was never restarted.

**Compaction on real streamed data**

| Table | Files before | Files after | Reduction |
|---|---|---|---|
| `orders` | 55 | 5 | 91% |
| `order_items` | 9 | 1 | 89% |
| `inventory` | 9 | 1 | 89% |
| `customers` | 10 | 1 | 90% |

Row counts still matched Postgres exactly afterwards.

**Trino** reads the same tables through the Delta connector and returns
identical results, including the mid-stream evolved column.

## What is still not covered

- **Failure injection.** No broker restart, partition rebalance, connector
  crash mid-batch, or Postgres failover was exercised.
- **Scale.** Thousands of events, not millions. The compaction argument gets
  stronger with volume, not weaker, but that is an inference rather than a
  measurement.
- **Sustained throughput.** The events/second figure measures per-batch
  overhead at demo scale, not capacity.

---

## Turning these into resume bullets

> Built a CDC streaming lakehouse capturing row-level changes from Postgres via
> Debezium into Delta Lake through Spark Structured Streaming, with LSN-guarded
> idempotent MERGE semantics giving exactly-once effect across connector
> restarts and offset replays.

> Solved the three failure modes that silently corrupt CDC tables — multiple
> changes per key within a micro-batch, out-of-order delivery, and mid-stream
> schema changes — and covered each with a regression test, 48 in total.

> Cut Parquet file count 96% (180 → 8) with a scheduled OPTIMIZE / Z-ORDER /
> VACUUM job, improving analytical query latency 1.59× and reducing on-disk
> footprint 93%.

> Delivered mid-stream schema evolution with zero pipeline downtime by
> inferring the payload schema per micro-batch and enabling Delta schema
> auto-merge, so a source `ALTER TABLE` propagates without a redeploy.

Do not quote the ~20 events/second figure as throughput capacity — it measures
per-batch overhead at demo scale. Quote the ratios and the mechanisms.
