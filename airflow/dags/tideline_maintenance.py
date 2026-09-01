"""Scheduled Delta table maintenance.

The streaming job runs continuously and is not orchestrated — Structured
Streaming manages its own lifecycle, and wrapping it in a scheduler would just
add a second thing that can stop it. What *does* need scheduling is the
maintenance that keeps the tables queryable, and that is what this DAG owns.

It is deliberately the same Airflow deployment that runs the Lodestar ELT DAG.
A streaming pipeline and a batch warehouse both need a scheduler, and running
two is how organisations end up with two on-call rotations.

Cadence: hourly compaction, daily vacuum. Compaction is cheap and its benefit
decays quickly as new small files land. Vacuum is expensive, irreversibly
deletes files, and shortens the time-travel window — so it runs once a day and
retains a week.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ROOT = Path(os.environ.get("TIDELINE_HOME", "/opt/tideline"))

BASH_PREAMBLE = f"""
set -euo pipefail
cd {PROJECT_ROOT}
export PATH="${{TIDELINE_BIN_PATH:-{PROJECT_ROOT}/.venv/bin}}:$PATH"
export JAVA_HOME="${{JAVA_HOME:-/opt/java/openjdk}}"
"""

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    # OPTIMIZE takes the Delta write lock. If the streaming job holds it, the
    # retry is the fix; failing the whole DAG would be an overreaction.
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="tideline_maintenance",
    description="Hourly compaction and Z-ordering of the Delta lakehouse.",
    default_args=DEFAULT_ARGS,
    schedule="0 * * * *",
    start_date=datetime(2024, 1, 1),
    # Maintenance is not a per-interval unit of work: running yesterday's
    # missed compaction is pointless, since today's already covers it.
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    tags=["tideline", "delta", "maintenance", "streaming"],
    doc_md=__doc__,
    params={
        "target_file_mb": Param(default=128, type="integer"),
    },
) as dag:

    start = EmptyOperator(task_id="start")

    before = BashOperator(
        task_id="file_stats_before",
        bash_command=BASH_PREAMBLE + """
        tideline stats --json
        """,
        doc_md="Record the fragmentation level so the effect is measurable.",
    )

    # Compaction runs per table rather than as one task, so a lock contention
    # failure on `orders` does not prevent `inventory` from being compacted.
    compact_tasks = []
    for table in ("orders", "order_items", "inventory", "customers"):
        compact_tasks.append(
            BashOperator(
                task_id=f"compact_{table}",
                bash_command=BASH_PREAMBLE + f"""
                tideline maintain --tables {table} \
                    --target-file-mb {{{{ params.target_file_mb }}}} \
                    --skip-vacuum
                """,
                doc_md=f"OPTIMIZE + Z-ORDER on `{table}`. Vacuum is a separate, daily task.",
            )
        )

    after = BashOperator(
        task_id="file_stats_after",
        bash_command=BASH_PREAMBLE + """
        tideline stats --json
        """,
        # Runs even if one table's compaction failed, so the report reflects
        # reality rather than only appearing on a clean run.
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Vacuum only on the midnight run. Guarded in the shell rather than with a
    # separate DAG so the ordering against compaction stays explicit: vacuuming
    # before compaction would leave the pre-compaction files for another cycle.
    vacuum = BashOperator(
        task_id="vacuum_daily",
        bash_command=BASH_PREAMBLE + """
        if [ "{{ logical_date.hour }}" = "0" ]; then
            echo "midnight run: vacuuming with a 168h (7 day) retention"
            tideline maintain --retain-hours 168
        else
            echo "not the midnight run; skipping vacuum"
        fi
        """,
        doc_md=(
            "Deletes files no longer referenced by any version within the "
            "retention window. 168h is the time-travel horizon — shortening it "
            "reclaims disk and destroys the ability to query older versions."
        ),
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    start >> before >> compact_tasks >> after >> vacuum >> end
