"""Shared fixtures.

One SparkSession for the whole session — starting a JVM per test would make the
suite unusably slow.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from collections.abc import Iterator

import pytest

# Must be set before the JVM starts.
os.environ.setdefault("JAVA_HOME", "/opt/homebrew/opt/openjdk@17")

from tideline.spark import build_local_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> Iterator:
    warehouse = tempfile.mkdtemp(prefix="tideline-warehouse-")
    session = build_local_spark(warehouse_dir=warehouse)
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def lakehouse(tmp_path) -> str:
    path = tmp_path / "lakehouse"
    path.mkdir()
    return str(path)


# --------------------------------------------------------------- CDC helpers


def debezium_event(
    op: str,
    *,
    lsn: int,
    after: dict | None = None,
    before: dict | None = None,
    table: str = "orders",
    ts_ms: int = 1_700_000_000_000,
    snapshot: str | None = None,
) -> str:
    """Build a Debezium envelope exactly as the connector emits one.

    Tests feed these through the real parsing path rather than constructing
    DataFrames directly, so the envelope handling is covered too.
    """
    payload = {
        "before": before,
        "after": after,
        "source": {
            "version": "2.7.3.Final",
            "connector": "postgresql",
            "name": "tideline",
            "ts_ms": ts_ms,
            "snapshot": snapshot,
            "db": "tideline",
            "schema": "shop",
            "table": table,
            "txId": lsn,
            "lsn": lsn,
        },
        "op": op,
        "ts_ms": ts_ms + 5,
    }
    return json.dumps(payload)


@pytest.fixture
def make_batch(spark):
    """Turn a list of (key, envelope-json) pairs into a Kafka-shaped DataFrame."""

    def _make(records: list[tuple[str, str | None]]):
        rows = [(k, v) for k, v in records]
        return spark.createDataFrame(rows, "key string, value string")

    return _make
