#!/usr/bin/env python
"""Import the maintenance DAG and fail on anything the scheduler would reject.

Kept out of the pytest run because Airflow and Spark pin conflicting versions
of several libraries; CI installs Airflow in a separate job and runs this
standalone.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_FOLDER = REPO_ROOT / "airflow" / "dags"

os.environ.setdefault("TIDELINE_HOME", str(REPO_ROOT))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(DAGS_FOLDER))

EXPECTED_DAG_IDS = {"tideline_maintenance"}


def validate() -> int:
    from airflow.exceptions import AirflowDagCycleException
    from airflow.models import DagBag
    from airflow.utils.dag_cycle_tester import check_cycle

    dag_bag = DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)

    if dag_bag.import_errors:
        print("DAG import errors:", file=sys.stderr)
        for filename, error in dag_bag.import_errors.items():
            print(f"\n  {filename}\n    {error}", file=sys.stderr)
        return 1

    found = set(dag_bag.dag_ids)
    missing = EXPECTED_DAG_IDS - found
    if missing:
        print(f"Expected DAGs not found: {sorted(missing)}", file=sys.stderr)
        return 1

    failures = []
    for dag_id in sorted(found):
        # Not `get_dag()`: that reaches into the Airflow metadata database and
        # would make this check require a migrated DB.
        dag = dag_bag.dags[dag_id]

        if not dag.tasks:
            failures.append(f"{dag_id}: has no tasks")

        try:
            check_cycle(dag)
        except AirflowDagCycleException as exc:
            failures.append(f"{dag_id}: cycle detected — {exc}")

        for task in dag.tasks:
            if not task.owner or task.owner == "airflow":
                failures.append(f"{dag_id}.{task.task_id}: no explicit owner set")

        print(f"  ok  {dag_id:<28} {len(dag.tasks):>2} tasks, schedule={dag.schedule_interval!r}")

    if failures:
        print("\nValidation failures:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    print(f"\n{len(found)} DAG(s) validated successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(validate())
