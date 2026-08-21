"""SparkSession construction, in one place.

Every Spark tuning decision in the project lives here so they can be reviewed
together rather than scattered across job files.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from .config import Config

# Kafka source jar. Must match the running PySpark version exactly or the
# connector fails at query-start with a confusing NoSuchMethodError.
KAFKA_PACKAGE_TEMPLATE = "org.apache.spark:spark-sql-kafka-0-10_2.12:{version}"


def _ensure_worker_python() -> None:
    """Pin the worker interpreter to the one running the driver.

    Spark launches Python workers from whatever `python3` is first on PATH. If
    that differs from the driver even by a minor version, every task fails with
    PYTHON_VERSION_MISMATCH. Setting both variables makes the job independent of
    the ambient PATH.
    """
    executable = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", executable)


def build_spark(
    config: Config,
    app_name: str = "tideline",
    *,
    with_kafka: bool = True,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    _ensure_worker_python()

    import pyspark

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("TIDELINE_SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", config.driver_memory)
        # --- Delta Lake ---
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Lets a MERGE widen the target table when the source gains a column.
        # This is the switch that makes mid-stream schema evolution work.
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Compaction rewrites files; without retention checks disabled a VACUUM
        # below the default 7-day horizon refuses to run.
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        # --- shuffle ---
        # The default 200 partitions is wildly wrong for a laptop-sized batch:
        # it produces 200 tiny files per write and dominates the runtime.
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # --- streaming ---
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        # Quieter, smaller local runs.
        .config("spark.ui.showConsoleProgress", "false")
    )

    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    # The Kafka jar has to go through `extra_packages`, not `.config(...)`:
    # `configure_spark_with_delta_pip` *sets* `spark.jars.packages` to Delta's
    # own coordinates, silently discarding anything already configured there.
    # Setting it on the builder appears to work and then fails at query start
    # with "Failed to find data source: kafka".
    extra_packages = (
        [KAFKA_PACKAGE_TEMPLATE.format(version=pyspark.__version__)] if with_kafka else []
    )

    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("TIDELINE_LOG_LEVEL", "WARN"))
    return spark


def build_local_spark(
    app_name: str = "tideline-test",
    warehouse_dir: str | Path | None = None,
) -> SparkSession:
    """Minimal session for tests: Delta, no Kafka, no package download."""
    _ensure_worker_python()

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if warehouse_dir:
        builder = builder.config("spark.sql.warehouse.dir", str(warehouse_dir))

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
