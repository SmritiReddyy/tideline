# Runbook

## Routine operations

### Start everything

```bash
make up          # containers + connector registration
make seed        # populate the source database
make stream      # Kafka -> Delta, continuously
```

### Check the pipeline is moving

```bash
make connector-status                       # Debezium connector + task state
tideline query --sql "select max(_lsn), max(_ingested_at) from orders"
make stats                                  # file counts climbing = data arriving
```

The Redpanda console at http://localhost:8085 shows topic offsets and lets you
read raw change events — the fastest way to answer "is this Debezium's problem
or Spark's?".

### Replay a table from the beginning

Safe: the LSN guard makes re-applied events no-ops.

```bash
# stop the stream, then drop its checkpoint
rm -rf data/checkpoints/orders
tideline stream --tables orders --starting-offsets earliest --once
```

### Rebuild a table from scratch

```bash
rm -rf data/lakehouse/orders data/checkpoints/orders
tideline stream --tables orders --starting-offsets earliest --once
```

Only works while the topic still holds the full history. Topics are compacted,
not infinite — for a table older than the retention window, re-snapshot instead
(below).

### Force a fresh snapshot from Postgres

```bash
tideline delete-connector
docker compose -f docker/docker-compose.yml exec postgres \
  psql -U tideline -c "SELECT pg_drop_replication_slot('tideline_slot');"
tideline register-connector
```

Dropping the slot discards the connector's position, so `snapshot.mode=initial`
re-reads every row. Expensive; the last resort.

---

## Failure playbook

### Connector is FAILED

```bash
make connector-status                  # the trace is in the task, not the connector
make logs
```

| Symptom in the trace | Cause | Fix |
|---|---|---|
| `must be superuser or replication role` | user lacks REPLICATION | check the init SQL ran |
| `publication "tideline_pub" does not exist` | init SQL did not run | `make down && make up` — init scripts only run on an empty volume |
| `replication slot "tideline_slot" is active` | a previous connector still holds it | drop the slot (above) |
| `wal_level must be logical` | Postgres started without the flag | check `command:` in docker-compose |

### Postgres disk filling up

Almost always an abandoned replication slot pinning WAL.

```sql
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained
FROM pg_replication_slots;
```

If `active` is false and `retained` is large, the consumer is gone:

```sql
SELECT pg_drop_replication_slot('tideline_slot');
```

`max_slot_wal_keep_size=1GB` caps this — the slot goes invalid instead of the
disk filling, which forces a re-snapshot but keeps the database up.

### Spark: "multiple source rows matched"

The deduplication step did not run, or `TableSpec.primary_key` does not match
the table's real key. Two changes to one key reached the MERGE.

```bash
pytest tests/test_cdc.py -k dedup -q
```

Check `primary_key` in `cdc.TABLE_SPECS` against the source schema — a
composite key declared as a single column produces exactly this.

### Rows are missing after a delete

Expected. Deletes are soft by default; the row is present with
`_deleted = true`. Every query needs `where not _deleted`.

```sql
select _op, _deleted, count(*) from orders group by 1, 2;
```

### A new source column is not appearing

1. Confirm Debezium is emitting it — read the topic in the Redpanda console.
2. Confirm `spark.databricks.delta.schema.autoMerge.enabled` is `true`
   (`spark.py` sets it).
3. Confirm the table's `TableSpec.schema` is `None`. An explicit schema pins the
   columns and disables inference — that is what it is for, but it also stops
   evolution.

New columns only appear on rows written *after* the change. Existing rows keep
`null`, which is correct.

### PYTHON_VERSION_MISMATCH

Spark launched a worker with a different Python than the driver.
`spark._ensure_worker_python()` pins both, so this means something set
`PYSPARK_PYTHON` externally. Unset it and let the code choose.

### Trino will not start, or every query says "No factory for location"

Three separate traps, each with an error that does not point at the fix. All
are already handled in the committed config; this is why.

| Symptom | Cause | Fix |
|---|---|---|
| `Defunct property 'delta.metadata.cache-size'` | removed in Trino 460+ | drop the line; `delta.metadata.cache-ttl` still works |
| `No factory for location: file:///...` | no filesystem implementation selected | `fs.hadoop.enabled=true` in `catalog/delta.properties` |
| `Configuration property 'fs.native-local.enabled' was not used` | that property does not exist in 468, at catalog *or* node level | use `fs.hadoop.enabled` instead |
| `register_table procedure is disabled` | disabled by default | `delta.register-table-procedure.enabled=true` |

Trino reads tables Spark created, so they must be attached to the catalog
before they are queryable. `make up-query` does this; by hand it is:

```sql
CREATE SCHEMA IF NOT EXISTS delta.tideline WITH (location='file:///data/lakehouse');
CALL delta.system.register_table(
  schema_name => 'tideline', table_name => 'orders',
  table_location => 'file:///data/lakehouse/orders');
```

### Spark: "Failed to find data source: kafka"

The Kafka jar was not resolved. Note that `configure_spark_with_delta_pip`
*overwrites* `spark.jars.packages` — setting the Kafka coordinates with
`.config(...)` on the builder appears to work and is silently discarded. It has
to go through that helper's `extra_packages` argument, which `spark.py` does.

### Spark fails with module access errors on startup

Java version. Spark 3.5 needs 8, 11 or 17:

```bash
java -version
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

### OPTIMIZE fails with a concurrent modification

The streaming job holds the Delta write lock. The DAG retries with exponential
backoff, which usually resolves it. If it does not, compaction is running too
often relative to the trigger interval — lengthen the schedule.

---

## Monitoring

**Freshness per table**

```sql
select 'orders' as t, max(_lsn) as lsn, max(_ingested_at) as last_write from orders
union all select 'inventory', max(_lsn), max(_ingested_at) from inventory;
```

**Fragmentation** — the leading indicator of query slowdown:

```bash
tideline stats
```

Rising `small` counts between compaction runs are normal. A count that keeps
climbing across runs means compaction is failing or scheduled too infrequently.

**Streaming progress** — the Spark UI at http://localhost:4040 while the job
runs, or `lastProgress` in the job's own log output:
`inputRowsPerSecond` below `processedRowsPerSecond` means keeping up.

---

## Operational cautions

- **VACUUM is irreversible and shortens time travel.** `retain_hours` is the
  horizon. The tests use 0 because nothing else reads them; production must not.
- **Only one writer per Delta table.** Trino's catalog sets
  `delta.enable-non-concurrent-writes=false` for this reason. Spark is the
  writer; everything else reads.
- **The Trino file metastore is single-node only.** Fine for this stack,
  unsupported for production — point at Hive Metastore, Glue, or Unity there.
- **`docker compose down -v` deletes the Postgres volume**, so init scripts
  re-run on the next `up`. That is usually what you want after a schema change,
  and never what you want if the data mattered.
