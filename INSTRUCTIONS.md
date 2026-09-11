# Running Tideline

There are two ways to run this. The offline path needs no Docker and exercises
all the CDC logic. The real stack adds Debezium and Kafka.

## Requirements

- **Java 17.** Spark 3.5 rejects newer JDKs with confusing module errors.
  `brew install openjdk@17`, or pass `JAVA_HOME=/path/to/jdk17`.
- **Python 3.10 or 3.11.** Spark's Python workers don't support 3.12.
- **Docker**, only for the real stack.

`make setup` checks for Java 17 and tells you what to do if it's missing.

## Offline — no containers

Synthetic Debezium events go through the real parsing, deduplication and merge
code into real Delta tables. Only the transport is different.

```bash
make setup       # venv + dependencies
make demo        # simulate → stats → compact → stats
make benchmark   # fragment, measure, compact, measure again
make query       # run the analytical queries
```

This can't exercise Debezium's WAL decoding or Kafka's delivery semantics. For
that, use the real stack.

## The real stack

```bash
make up          # Postgres + Redpanda + Debezium, registers the connector
make seed        # populate the source database
make workload    # drive insert/update/delete traffic for 60s
make stream      # Kafka → Delta, continuously (Ctrl-C to stop)
```

In another terminal, while `make stream` is running:

```bash
make stats       # watch the file count climb
make evolve      # ALTER TABLE mid-stream, no restart needed
make maintain    # compact it back down
make query
```

- Redpanda console: http://localhost:8085 — read the raw Debezium envelopes
- Trino (optional): `make up-query`, then http://localhost:8086

`make down` stops everything and deletes the volumes.

### Memory

The stack fits in about 3.3 GB: Postgres 512 MB, Redpanda 1.5 GB, Connect 1 GB,
console 256 MB. Spark runs on the host and wants another 2 GB. If Docker
Desktop is set below ~4 GB, raise it in Settings → Resources, or stop other
containers first.

Trino adds another 2 GB, which is why it's behind an opt-in profile.

## Demos worth running

**Schema evolution.** With the stream running:

```bash
make evolve
```

Adds `loyalty_tier` to `shop.customers` and touches 100 rows. The column
appears in Delta within one trigger interval, rows written before it stay null,
and nothing restarts.

**Replay safety.** Stop the stream, delete its checkpoints, and re-read every
event from the beginning:

```bash
rm -rf data/checkpoints
tideline stream --once --starting-offsets earliest
```

The tables come out identical. That's the merge being idempotent, not luck.

**Compaction.** `make benchmark` builds a fragmented lakehouse the way the
stream does, measures query latency, compacts, and measures again.

## Driving it by hand

Every stage has a CLI command, so a failed scheduled task can be reproduced one
step at a time:

```bash
tideline info                          # resolved configuration
tideline seed                          # populate Postgres
tideline workload --duration 60 --rate 20
tideline register-connector            # register Debezium
tideline connector-status              # connector and task state
tideline stream --once                 # drain the topics, then stop
tideline stats                         # Delta file layout
tideline maintain --retain-hours 168   # compact and vacuum
tideline query --file query/queries/analytics.sql
tideline simulate --batches 20         # offline CDC path
```

## Orchestration

The streaming job is deliberately **not** orchestrated — Structured Streaming
manages its own lifecycle, and wrapping it in a scheduler just adds a second
thing that can stop it. Run it as a long-lived process.

What *is* scheduled is maintenance. `airflow/dags/tideline_maintenance.py` runs
hourly compaction and a daily vacuum, and is designed to live in the same
Airflow deployment as [Lodestar's](https://github.com/SmritiReddyy/lodestar)
ELT DAG.

## Trino gotchas

Three config traps, all already handled in the committed files. Worth knowing
because none of the error messages point at the fix:

| Error | Fix |
|---|---|
| `Defunct property 'delta.metadata.cache-size'` | removed in Trino 460+; drop the line |
| `No factory for location: file:///...` | `fs.hadoop.enabled=true` in the catalog |
| `Configuration property 'fs.native-local.enabled' was not used` | that property doesn't exist in 468 |
| `register_table procedure is disabled` | `delta.register-table-procedure.enabled=true` |

Spark creates the tables; Trino only attaches to them. `make up-query` does the
attaching, or by hand:

```sql
CREATE SCHEMA IF NOT EXISTS delta.tideline WITH (location='file:///data/lakehouse');
CALL delta.system.register_table(
  schema_name => 'tideline', table_name => 'orders',
  table_location => 'file:///data/lakehouse/orders');
```

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `TIDELINE_PG_HOST` | `localhost` | Source database |
| `TIDELINE_BOOTSTRAP_SERVERS` | `localhost:19092` | Kafka/Redpanda |
| `TIDELINE_TOPIC_PREFIX` | `tideline` | Debezium logical server name |
| `TIDELINE_PG_SCHEMA` | `shop` | Captured schema |
| `TIDELINE_LAKEHOUSE_ROOT` | `data/lakehouse` | Delta tables |
| `TIDELINE_CHECKPOINT_ROOT` | `data/checkpoints` | Stream checkpoints |
| `TIDELINE_DRIVER_MEMORY` | `2g` | Spark driver |
| `TIDELINE_TRIGGER_INTERVAL` | `10 seconds` | Micro-batch cadence |
| `TIDELINE_MAX_OFFSETS_PER_TRIGGER` | `20000` | Batch size cap |

Topic names are derived as `<prefix>.<schema>.<table>`, matching Debezium's
convention, so the stream can't subscribe to a topic that will never exist.

## Repository layout

```
tideline/
├── docker/                   Postgres, Redpanda, Debezium (+ Trino profile)
├── connectors/               Debezium connector config, annotated inline
├── src/tideline/
│   ├── envelope.py           Debezium envelope parsing
│   ├── cdc.py                dedupe + LSN-guarded MERGE  ← the core
│   ├── stream.py             Structured Streaming job
│   ├── maintenance.py        OPTIMIZE / Z-ORDER / VACUUM
│   ├── simulate.py           offline event source
│   ├── generator.py          OLTP workload against Postgres
│   └── connector.py, spark.py, config.py, cli.py
├── query/                    Trino catalog + analytical queries
├── airflow/dags/             hourly compaction DAG
├── scripts/benchmark.py      before/after compaction measurement
└── tests/                    48 tests
```

## Troubleshooting

The [runbook](docs/RUNBOOK.md) covers the failure modes in detail. The three
most common:

- **Rows "missing" after a delete** — they're not; deletes are soft. Every
  query needs `where not _deleted`.
- **`multiple source rows matched`** — `TableSpec.primary_key` doesn't match
  the table's real key. A composite key declared as one column does this.
- **`Failed to find data source: kafka`** — the Kafka jar didn't resolve. Note
  `configure_spark_with_delta_pip` overwrites `spark.jars.packages`, so it has
  to go through that helper's `extra_packages`.

## CI

Four jobs per pull request: lint, the full test suite, compose and connector
config validation, and Airflow DAG import. The test job also runs an end-to-end
simulation including a mid-stream schema change and a compaction cycle.

Airflow and PySpark pin conflicting dependencies, so DAG validation runs in its
own job rather than fighting the resolver.
