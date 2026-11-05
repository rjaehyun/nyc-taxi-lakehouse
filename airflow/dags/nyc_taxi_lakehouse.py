import logging
import os
from datetime import datetime, timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

log = logging.getLogger(__name__)

PROJECT_DIR = "/opt/airflow/project"
DATA_DIR = f"{PROJECT_DIR}/data"

# Set by plan_months: space-separated months such as "2025-04"
MONTHS = "{{ ti.xcom_pull(task_ids='plan_months') }}"
LIGHT_SPARK = "SPARK_DRIVER_MEMORY=1g"  # checks and the gate need less memory than the jobs


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
    description="Incremental bronze, silver, quality gate, and gold for NYC taxi trips",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Manila"),
    schedule="0 6 * * 1",  # Mondays 06:00 Manila time: poll for a newly published month
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "months": Param(
            "",
            type="string",
            pattern=r"^$|^\d{4}-\d{2}([ ,]+\d{4}-\d{2})*$",
            description="Leave blank for the next published month, or enter e.g. 2025-04",
        )
    },
    tags=["lakehouse", "spark"],
    doc_md=__doc__,
):
    plan = job(
        "plan_months",
        'python -m jobs.plan_months "{{ params.months }}"',
        skip_on_exit_code=99,  # exit 99 means "nothing to do"
    )
    download = job("download_tlc", f"python ingestion/download_tlc.py {MONTHS}")
    bronze = job("bronze", f"python -m jobs.bronze {MONTHS}")
    silver = job("silver", f"python -m jobs.silver {MONTHS}")
    check_silver = job(
        "check_silver", f"{LIGHT_SPARK} python -m jobs.check_silver {MONTHS}", retries=0
    )
    quality_gate = job(
        "quality_gate",
        f"{LIGHT_SPARK} python -m jobs.quality_report --strict {MONTHS}",
        retries=0,
    )
    gold = job("gold", f"python -m jobs.gold {MONTHS}")
    check_gold = job(
        "check_gold",
        f"SPARK_DRIVER_MEMORY=2g python -m jobs.check_gold {MONTHS}",
        retries=0,
    )

    plan >> download >> bronze >> silver >> check_silver >> quality_gate >> gold >> check_gold