# Tideline — CDC Streaming Lakehouse

Row-level changes captured from a Postgres write-ahead log, streamed through
Kafka, and merged into Delta Lake tables that support upserts, deletes, schema
evolution and time travel.

The pattern behind "why does the warehouse always lag production by hours?"

```bash
git clone <this repo> && cd tideline
make setup && make demo      # no Docker needed
```

---

## The pipeline

```
┌────────────┐   WAL    ┌──────────┐  change  ┌──────────┐
│ PostgreSQL │ ───────► │ Debezium │  events  │ Redpanda │   one topic per table
│ (OLTP)     │ logical  │ connector│ ───────► │ (Kafka)  │
└────────────┘ decoding └──────────┘          └────┬─────┘
                                                   │
                                   ┌───────────────▼────────────────┐
                                   │ Spark Structured Streaming     │
                                   │  parse envelope                │
                                   │  dedupe to latest per key      │
                                   │  MERGE, guarded on LSN         │
                                   └───────────────┬────────────────┘
                                                   │
                    ┌──────────────────────────────▼──────────────────┐
                    │ Delta Lake — upserts, soft deletes, time travel │
                    └──────┬───────────────────────────────┬──────────┘
                           │                               │
              ┌────────────▼───────────┐      ┌────────────▼───────────┐
              │ OPTIMIZE / Z-ORDER /   │      │ Query: Trino or DuckDB │
              │ VACUUM (hourly, Airflow)│     └────────────────────────┘
              └────────────────────────┘
```

---

## The three problems this actually solves

A CDC pipeline that only handles inserts looks correct and is not. These are
the failures that do not throw — the table just quietly stops matching the
source — and each has a test that would fail without the corresponding fix.

**1. Several changes to one row inside one micro-batch.** An order inserted and
then updated twice in the same second arrives as three events in one batch.
Handing all three to a Delta `MERGE` raises *multiple source rows matched*. The
batch is collapsed to the latest change per key first, ordered by LSN.
→ `test_multiple_changes_to_one_key_collapse_to_the_latest`

**2. Events arriving out of order.** A connector restart or a partition
rebalance can deliver an older change after a newer one, resurrecting stale
values. Every update is guarded on `s._lsn > t._lsn`, so an older event is a
no-op — which also makes replaying from an earlier Kafka offset harmless.
→ `test_out_of_order_event_does_not_overwrite_newer_state`,
`test_replaying_the_same_batch_is_idempotent`

**3. Schema changes mid-stream.** A column added to the source appears in later
events and not earlier ones. Parsing against a fixed struct would silently drop
it. The row image is kept as JSON, its schema inferred per micro-batch, and
Delta's schema evolution widens the target table — no redeploy, no downtime.
→ `test_new_column_appears_without_redeploy`

---

## Verified on the real stack

The full pipeline has been run end to end on the containerised stack —
Postgres 16 → Debezium 2.7.3 → Redpanda 24.2.7 → Spark 3.5.3 → Delta 3.2.1 →
Trino 468 — not just through the offline simulator.

| Check | Result |
|---|---|
| Change events consumed from Kafka | 2,857 |
| **Collapsed by intra-batch dedup** | **998** (678 on `orders` alone) |
| Delta vs Postgres, value-by-value | **0 mismatches**, 0 missing, 0 phantom rows |
| Soft deletes | 49 orders + 87 cascaded line items — exactly the workload's deletes |
| **End-to-end latency** (commit → queryable, 5s trigger) | **median 7.49 s** (6.09–8.72) |
| **Full replay of all 2,968 events** | **0 rows changed** — identical LSN checksums |
| Live `ALTER TABLE ADD COLUMN` | reached Delta in one trigger, **0 restarts** |
| Compaction on streamed data | 83 files → 8 (up to 91% per table), counts still exact |
| Trino reading the same tables | identical results, evolved column included |

The 678 collapsed events on one table in one batch is the load-bearing number:
without deduplication the Delta MERGE would have failed on the first
micro-batch.

## Benchmark results

From `make benchmark`, which fragments a lakehouse the way the stream does,
measures it, compacts it, and measures again. Regenerate any time.

**CDC throughput and correctness**

| | |
|---|---|
| Change events processed | 2,514 |
| Delta commits (micro-batches) | 80 |
| Rows merged | 2,184 |
| **Events collapsed by intra-batch dedup** | **330** |

That last number is the load-bearing one: 330 events were multiple changes to
a key within a single batch. Without deduplication every one of them would
have failed the MERGE.

**The small-file problem, and the fix**

| | Before | After | Change |
|---|---|---|---|
| Parquet files | 180 | 8 | **−96%** |
| On-disk size | 0.98 MiB | 0.07 MiB | −93% |

**Query latency** — median of 7 runs, DuckDB `delta_scan` over the same files:

| Query | Fragmented | Compacted | Speedup |
|---|---|---|---|
| Point lookup by key | 3.62 ms | 2.08 ms | 1.74× |
| Filter on Z-order column | 3.68 ms | 2.34 ms | 1.57× |
| Aggregate by status | 4.92 ms | 2.75 ms | 1.79× |
| Join orders → line items | 10.30 ms | 6.25 ms | 1.65× |
| Inventory below reorder level | 1.65 ms | 1.98 ms | 0.83× |
| Full scan count | 3.13 ms | 1.80 ms | 1.74× |
| **Total** | **27.30 ms** | **17.21 ms** | **1.59×** |

One query got *slower*. At ~2 ms the measurement is close to the noise floor,
and that row is left in rather than dropped — a benchmark that only reports its
wins is not a benchmark. The signal is the aggregate: 1.59× from opening 8
files instead of 180.

**Testing**

| | |
|---|---|
| Tests | **48**, all passing |
| — CDC correctness | 17 (envelope, dedup, merge, ordering, evolution, time travel) |
| — Table maintenance | 7 (fragmentation, OPTIMIZE, Z-ORDER, VACUUM, history) |
| — Config, simulator, SQL tooling | 24 |
| Airflow DAGs import-validated | 1 (9 tasks) |

---

## Engineering decisions worth explaining

**The Debezium envelope is consumed raw, not unwrapped.** It is common to
configure `ExtractNewRecordState` so Kafka carries only the `after` image. That
discards three things this pipeline needs: the `before` image for deletes, the
transaction LSN for ordering, and the flag distinguishing a snapshot read from
a live insert. Parsing the full envelope costs a little and buys correctness.

**Exactly-once is two mechanisms, not one.** Structured Streaming checkpoints
the Kafka offsets it processed before committing a batch, and the Delta MERGE
is LSN-guarded. A crash between the two replays the batch, and the replay is a
no-op because every event in it loses the LSN comparison. Checkpointing alone
gives at-least-once; the idempotent sink is what closes it.

**Deletes are soft by default.** The row stays with `_deleted = true`. A hard
delete makes "what did this look like last Tuesday" unanswerable even with time
travel, and most analytical consumers would rather filter than lose the record.
`hard_delete=True` per table switches behaviour.

**One streaming query per table, not one fanning out.** Costs a little driver
memory; buys independent checkpoints, independent restart, and per-topic
backpressure. A schema problem on `customers` does not stall `orders`.

**Redpanda instead of Kafka.** One Go binary, no JVM, no ZooKeeper or KRaft
quorum, and it speaks the Kafka protocol so Debezium and Spark are unmodified.
The equivalent Kafka setup wants ~3 GB before a single message is produced,
which does not leave room for Spark on an 8 GB laptop.

**`REPLICA IDENTITY FULL` on every captured table.** Without it Postgres writes
only the primary key into the WAL on update and delete, so Debezium's `before`
image is just the key — enough to delete a row, not enough to audit what was
deleted. The cost is a wider WAL; on these tables it is worth paying.

**Delta over Iceberg.** Simplest to run locally, first-class Spark support,
and `MERGE` semantics that map directly onto CDC. The choice is confined to
`cdc.py`'s merge function and `maintenance.py` — swapping to Iceberg means
rewriting those two, not the pipeline. Pick whichever appears in the job
descriptions you are targeting.

---

## Running it

### Offline — no Docker, no containers

The CDC path runs against synthetic Debezium envelopes generated in-process:
same parsing, same dedup, same LSN-guarded merge, same Delta tables. Only the
transport differs.

```bash
make setup        # venv + deps (checks for Java 17)
make demo         # simulate → stats → compact → stats
make benchmark    # the before/after numbers above
make query        # run the analytical query set
```

This cannot exercise Debezium's WAL decoding or Kafka's delivery semantics —
for that, run the real stack below. (It has been: see the table above.)

### The real stack

```bash
make up           # Postgres + Redpanda + Debezium, then registers the connector
make seed         # populate the source database
make workload &   # drive insert/update/delete traffic
make stream       # Kafka → Delta, continuously (Ctrl-C to stop)
```

Then, in another shell:

```bash
make evolve       # ALTER TABLE shop.customers ADD COLUMN, mid-stream
make stats        # watch the file count climb
make maintain     # compact it back down
make query
```

- Redpanda console: http://localhost:8085 — read the raw Debezium envelopes
- Trino (opt-in, `make up-query`): http://localhost:8086, tables under
  `delta.tideline`. `make up-query` also attaches them to the catalog.

### Requirements

- **Java 17.** Spark 3.5 rejects newer JDKs with module access errors.
  `brew install openjdk@17`, or pass `JAVA_HOME=/path/to/jdk17`.
- **Python 3.10 or 3.11.** Spark 3.5's Python workers do not support 3.12+.
- Docker only for the real stack.

---

## Repository layout

```
tideline/
├── docker/
│   ├── docker-compose.yml       Postgres + Redpanda + Debezium (+ Trino profile)
│   └── postgres/init/           schema, REPLICA IDENTITY, publication, repl user
├── connectors/
│   └── postgres-source.json     Debezium config, annotated inline
├── src/tideline/
│   ├── envelope.py              Debezium envelope parsing
│   ├── cdc.py                   dedup + LSN-guarded MERGE  ← the core
│   ├── stream.py                Structured Streaming job
│   ├── maintenance.py           OPTIMIZE / Z-ORDER / VACUUM
│   ├── simulate.py              offline event source
│   ├── generator.py             OLTP workload against Postgres
│   ├── connector.py             Kafka Connect REST client
│   └── spark.py, config.py, cli.py
├── query/
│   ├── trino/catalog/           Trino Delta catalog
│   └── queries/analytics.sql    representative queries
├── airflow/dags/                hourly compaction DAG
├── scripts/benchmark.py         the before/after measurement
└── tests/                       48 tests
```

Further reading: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) ·
[docs/METRICS.md](docs/METRICS.md) · [docs/RUNBOOK.md](docs/RUNBOOK.md)

---

## Relationship to Lodestar

[Lodestar](../lodestar) is the batch counterpart: an Airflow + dbt + BigQuery
warehouse on a daily schedule. Tideline is the streaming path to the same kind
of destination, and the maintenance DAG here is designed to run in the same
Airflow deployment. Between them they cover both halves of the job: the nightly
dimensional warehouse, and the minutes-fresh operational mirror.
