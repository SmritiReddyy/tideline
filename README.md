# Tideline

A streaming pipeline that keeps a lakehouse in sync with a production Postgres
database, row by row, seconds behind.

Debezium reads the write-ahead log, Kafka carries the changes, Spark merges
them into Delta Lake tables that handle upserts, deletes, schema changes and
time travel.

It's the answer to "why does our warehouse always lag production by hours?"

```bash
git clone https://github.com/SmritiReddyy/tideline && cd tideline
make setup && make demo     # no Docker needed for this part
```

---

## The shape of it

```
Postgres ──WAL──> Debezium ──> Redpanda (Kafka) ──> Spark Structured Streaming
                                                              ↓
                                            parse envelope, dedupe, MERGE
                                                              ↓
                                                         Delta Lake
                                                         ↙         ↘
                                      compaction job          Trino / DuckDB
```

## The interesting part

A CDC pipeline that only handles inserts looks correct and isn't. Three things
break it quietly — the table just stops matching the source, with no error:

**Several changes to one row in one batch.** An order gets inserted and updated
twice in the same second. Hand all three to a Delta `MERGE` and it refuses —
*multiple source rows matched*. So each batch collapses to the latest change
per key first. On a real run, **678 events on one table in one batch** needed
this.

**Events arriving out of order.** A connector restart can deliver an old change
after a new one and quietly resurrect stale data. Every update is gated on the
Postgres log position, so an older event does nothing. The same guard is what
makes replaying from an old Kafka offset harmless.

**Schema changes mid-stream.** Someone adds a column. Parsing against a fixed
schema would silently drop it, so the row is kept as JSON and its shape is
worked out per batch. The column shows up in Delta on the next micro-batch,
with no redeploy.

Each has a test that fails if you remove the fix.

## What came out of it

Run against the real stack — Postgres, Debezium, Redpanda, Spark, Delta, Trino:

- **Zero mismatches** against Postgres, checked value by value, not just row counts
- **7.5 seconds** median from `COMMIT` to queryable in the lakehouse
- Replayed all **2,968 events** back through it: nothing changed
- Added a column mid-stream: reached Delta in one trigger, **no restart**
- Compaction took **180 files down to 8**, queries 1.59× faster
- **48 tests**, all passing

Full numbers and how they were measured: [docs/METRICS.md](docs/METRICS.md).

## A few decisions I'd defend in an interview

**The Debezium envelope is kept whole.** Most setups unwrap it so Kafka carries
just the new row. That throws away the before-image, the log position, and the
snapshot flag — all three of which this pipeline needs.

**Exactly-once is two mechanisms, not one.** Spark checkpoints Kafka offsets,
and the merge is idempotent. A crash between them replays the batch, and the
replay does nothing because every event loses the ordering comparison.
Checkpointing alone would only get you at-least-once.

**Deletes are soft.** The row stays with `_deleted = true`. A hard delete makes
"what did this look like last Tuesday" unanswerable, and most people would
rather filter than lose the record.

**Redpanda instead of Kafka.** One Go binary, no JVM, no ZooKeeper. Same
protocol, so Debezium and Spark don't know the difference — and it leaves
enough RAM to actually run Spark next to it.

More in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Running it

[INSTRUCTIONS.md](INSTRUCTIONS.md) has setup, the container stack, the demos,
and the Trino gotchas.

Quick version:

```bash
make setup        # venv + deps (checks you have Java 17)
make demo         # CDC path with synthetic events, no Docker
make up           # the real stack: Postgres + Redpanda + Debezium
make stream       # Kafka → Delta
make benchmark    # the compaction numbers above
```

Needs **Java 17** (Spark 3.5 rejects newer) and **Python 3.10 or 3.11**
(Spark's workers don't support 3.12). Docker only for the real stack.

## Docs

| | |
|---|---|
| [INSTRUCTIONS.md](INSTRUCTIONS.md) | Setup, running, the demos |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | How it works and why |
| [docs/METRICS.md](docs/METRICS.md) | Every number, and how it was measured |
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | What to do when it breaks |

Its batch counterpart is [Lodestar](https://github.com/SmritiReddyy/lodestar) —
the nightly warehouse to this one's live mirror.
