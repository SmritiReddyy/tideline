# Architecture

## The change-event path, stage by stage

### 1. Postgres → WAL

Logical decoding is enabled with `wal_level=logical`. Without it the WAL carries
only physical page changes, which cannot be decoded back into rows — CDC is
impossible, not merely harder.

Three settings in `docker-compose.yml` matter beyond that:

- `REPLICA IDENTITY FULL` (set per table in the init SQL) makes Postgres write
  the entire pre-image on update and delete. The default writes only the
  primary key, leaving Debezium's `before` field nearly empty.
- A named `PUBLICATION` listing exactly four tables, rather than
  `FOR ALL TABLES`, so adding a table to the database does not silently start
  streaming it.
- `max_slot_wal_keep_size=1GB`. A replication slot pins WAL until its consumer
  acknowledges it; if Debezium stops, the disk fills. This caps the damage at
  the cost of the slot going invalid, which is the better failure.

### 2. Debezium → Kafka

One topic per table, named `<topic.prefix>.<schema>.<table>`.

`plugin.name=pgoutput` uses Postgres' built-in logical decoding output — no
server-side extension, unlike `wal2json` or `decoderbufs`.

`snapshot.mode=initial` backfills existing rows as `op='r'` before streaming
live changes. `never` would silently skip everything already in the table.

`heartbeat.interval.ms=10000` exists for a specific failure: on a low-traffic
table, Debezium has nothing to acknowledge, so the replication slot never
advances and WAL accumulates indefinitely. Heartbeats keep the acknowledged
position moving.

**No `transforms`.** `ExtractNewRecordState` would flatten the envelope to just
the `after` image, discarding the before-image, the LSN, and the snapshot flag.
All three are load-bearing downstream.

### 3. Kafka → Spark

One `readStream` per topic, `maxOffsetsPerTrigger` bounded so the initial
snapshot does not arrive as one enormous micro-batch.

`foreachBatch` rather than a native Delta sink, because the merge is not a
simple append — it needs deduplication and a conditional MERGE, neither of
which a declarative sink expresses.

### 4. Spark → Delta

Per micro-batch:

```
parse envelope
  → infer row schema from this batch's JSON
  → expand to columns + CDC metadata
  → dedupe: row_number() over (partition by PK order by _lsn desc) = 1
  → MERGE:
        matched   AND s._lsn > t._lsn         → update all
        matched   AND s._deleted AND newer    → delete   (hard-delete tables)
        unmatched AND NOT s._deleted          → insert
```

Every target table carries `_op`, `_lsn`, `_source_ts_ms`, `_event_ts_ms`,
`_ingested_at`, `_is_snapshot`, `_deleted`. The underscore prefix keeps them
from colliding with source columns; source columns beginning with `_` are
dropped rather than allowed to shadow them silently.

## Ordering: why LSN and not a timestamp

A Postgres LSN is a write-ahead-log position: strictly increasing within a
database, and preserved in order within each Kafka topic. Wall-clock timestamps
are not usable for this — two changes can share a millisecond, and a connector
restart can emit them out of order.

The guarantee is per-table, not global. A transaction inserting an order and
its line items emits into two topics, and nothing orders those against each
other. That is fine, because the merge only ever compares LSNs within one
table. `test_simulator_lsn_is_globally_unique_and_ordered_per_table` asserts
exactly that contract and no more.

## Exactly-once, precisely

Two mechanisms, and neither is sufficient alone:

1. **Checkpointed offsets.** Structured Streaming records which Kafka offsets a
   batch covered, in the checkpoint, before committing.
2. **An idempotent sink.** The MERGE only applies changes with `s._lsn > t._lsn`.

A crash between them replays the batch. The replay is a no-op because every
event in it now loses the LSN comparison. Checkpointing alone gives
at-least-once delivery; the LSN guard is what turns that into exactly-once
*effect*.

The same property makes an operational replay safe: resetting the consumer to
an earlier offset re-reads events that have already been applied, and they land
as no-ops rather than corruption.

## Schema evolution

The payload is deliberately *not* parsed against a fixed struct. Instead:

1. `envelope.py` extracts `after`/`before` as raw JSON strings with
   `get_json_object`.
2. `cdc.infer_payload_schema` infers the schema from this batch's JSON.
3. `from_json` expands it into columns.
4. `spark.databricks.delta.schema.autoMerge.enabled=true` lets the MERGE widen
   the target table.

A column added to the source appears in the Delta table on the next batch.
Rows written before the change keep `null` for it — which is correct, since the
value genuinely did not exist then.

The cost is one extra pass over the batch's JSON to infer the schema. `TableSpec.schema`
accepts an explicit schema to skip inference where stability matters more than
flexibility.

## Soft deletes

Default. The row remains with `_deleted = true`.

A hard delete removes the row from the current version. Delta's time travel can
still reach older versions, but only within the VACUUM retention window — so
"what did this table look like last month" becomes unanswerable the moment
retention lapses. Soft deletes keep the record and push the decision to the
consumer, which is why every query in `query/queries/analytics.sql` filters
`where not _deleted`.

Per-table `hard_delete=True` switches behaviour where regulation or volume
demands it.

## Table maintenance

A ten-second trigger is 8,640 commits a day, each leaving small Parquet files.
Nothing breaks; query planning degrades steadily.

Order matters:

1. **OPTIMIZE** bin-packs small files toward 128 MB.
2. **Z-ORDER** clusters co-accessed values so filtered queries skip files.
3. **VACUUM** deletes files no longer referenced within the retention window.

Vacuuming before optimizing would leave the pre-compaction files for another
cycle. Retention is the time-travel horizon — the tests use `retain_hours=0`
because nothing else is reading; production must not.

The `orders` table stays at several files after compaction because it is
partitioned by `status`, and Delta cannot merge across partitions. That is
correct behaviour, and a reminder that partitioning a streaming table finely is
the fastest route to the problem compaction exists to solve.

## Deployment shape

The streaming job is **not** orchestrated. Structured Streaming manages its own
lifecycle; wrapping it in a scheduler adds a second thing that can stop it. It
runs as a long-lived process — `spark-submit` on a cluster, or a container with
a restart policy.

What *is* scheduled is maintenance: `airflow/dags/tideline_maintenance.py`,
hourly compaction and daily vacuum, in the same Airflow deployment that runs
Lodestar's ELT DAG. Compaction is per-table so lock contention on one table
does not block the others.
