"""NYC taxi lakehouse pipeline.

download_tlc -> bronze -> silver -> check_silver -> quality_gate -> gold -> check_gold

The quality gate fails the run when any month rejects more than 5% of its rows,
so Gold is never built from data that failed validation.
Compute tasks retry on temporary failures; checks and the gate do not retry,
because running the same data again cannot change the answer.
"""

import logging
import os
from datetime import datetime, timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

log = logging.getLogger(__name__)

PROJECT_DIR = "/opt/airflow/project"
DATA_DIR = f"{PROJECT_DIR}/data"


def notify_failure(context) -> None:
    """Runs after a task has failed and used up all of its retries."""
    ti = context.get("task_instance") or context.get("ti")
    message = (
        f"PIPELINE ALERT | dag={getattr(ti, 'dag_id', '?')} "
        f"task={getattr(ti, 'task_id', '?')} "
        f"run={getattr(ti, 'run_id', '?')}"
    )
    log.error(message)
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(f"{DATA_DIR}/pipeline_alerts.log", "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} {message}\n")


def job(task_id: str, command: str, **kwargs) -> BashOperator:
    """One pipeline step: run a command from the project root."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT_DIR} && {command}",
        **kwargs,
    )


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=45),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="nyc_taxi_lakehouse",
    description="Bronze, silver, quality gate, and gold for NYC taxi trips",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,  # manual for now; a monthly schedule comes on Day 9
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["lakehouse", "spark"],
    doc_md=__doc__,
):
    download = job("download_tlc", "python ingestion/download_tlc.py")
    bronze = job("bronze", "python -m jobs.bronze")
    silver = job("silver", "python -m jobs.silver")
    check_silver = job(
        "check_silver",
        "SPARK_DRIVER_MEMORY=1g python -m jobs.check_silver",
        retries=0,
    )
    quality_gate = job(
        "quality_gate",
        "SPARK_DRIVER_MEMORY=1g python -m jobs.quality_report --strict",
        retries=0
    )
    gold = job("gold", "python -m jobs.gold")
    check_gold = job(
        "check_gold",
        "SPARK_DRIVER_MEMORY=1g python -m jobs.check_gold",
        retries=0,
    )
    download >> bronze >> silver >> check_silver >> quality_gate >> gold >> check_gold