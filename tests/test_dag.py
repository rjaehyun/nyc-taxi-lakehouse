"""Structural tests for the nyc_taxi_lakehouse Airflow DAG.

These parse the DAG file as Python source code without importing Airflow or
Spark, so they run in CI without a JVM. The full import check is done with
`airflow dags list-import-errors` inside the container.
"""

import ast
from pathlib import Path

DAG_FILE = (
    Path(__file__).resolve().parents[1]
    / "airflow"
    / "dags"
    / "nyc_taxi_lakehouse.py"
)

EXPECTED_TASKS = {
    "plan_months",
    "download_tlc",
    "bronze",
    "silver",
    "check_silver",
    "quality_gate",
    "gold",
    "check_gold",
}


def _tree() -> ast.Module:
    return ast.parse(DAG_FILE.read_text(encoding="utf-8"))


def _dag_kwargs() -> dict:
    """Return the keyword arguments passed to the DAG() constructor."""
    for node in ast.walk(_tree()):
        if isinstance(node, ast.With):
            for item in node.items:
                call = item.context_expr
                if (
                    isinstance(call, ast.Call)
                    and isinstance(call.func, ast.Name)
                    and call.func.id == "DAG"
                ):
                    return {kw.arg: kw.value for kw in call.keywords}
    raise AssertionError("DAG context manager not found")


def _task_ids() -> set[str]:
    """Return all task_id values passed to job() calls."""
    ids = set()
    for node in ast.walk(_tree()):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "job"
            and node.args
        ):
            first = node.args[0]
            if isinstance(first, ast.Constant):
                ids.add(first.value)
    return ids


def test_dag_file_is_valid_python():
    """A syntax error in the DAG file should fail CI immediately."""
    assert _tree() is not None


def test_dag_id():
    """The DAG ID must match the file name so logs are easy to find."""
    kwargs = _dag_kwargs()
    assert ast.literal_eval(kwargs["dag_id"]) == "nyc_taxi_lakehouse"


def test_no_catchup():
    """Catchup must be off so Airflow does not run every past interval."""
    kwargs = _dag_kwargs()
    assert ast.literal_eval(kwargs["catchup"]) is False


def test_max_active_runs():
    """Only one run at a time, so concurrent writes cannot corrupt the lake."""
    kwargs = _dag_kwargs()
    assert ast.literal_eval(kwargs["max_active_runs"]) == 1


def test_has_a_schedule():
    """A missing schedule key defaults to None, which means manual only."""
    kwargs = _dag_kwargs()
    assert "schedule" in kwargs


def test_expected_tasks_exist():
    """Every pipeline step must be present."""
    assert _task_ids() == EXPECTED_TASKS


def test_plan_months_task_comes_first():
    """plan_months must exist, since everything else depends on its XCom output."""
    assert "plan_months" in _task_ids()