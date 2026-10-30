# Orchestration

```
download_tlc -> bronze -> silver -> check_silver -> quality_gate -> gold -> check_gold
```

## How it runs
- Airflow 3.3.2 in Docker, using a custom image with Java and PySpark.
- Each task runs the same command used for local development
  (for example `python -m jobs.silver`), so there is one code path.

## Failure behavior
| Task type | Retries | Why |
|---|---|---|
| Compute (download, bronze, silver, gold) | 2, one minute apart | Network or memory problems can be temporary |
| Checks and quality gate | 0 | The same data would give the same answer |

If the quality gate fails, gold and check_gold are skipped (upstream_failed).
A failure appends a PIPELINE ALERT line to data/pipeline_alerts.log.

## What I observed
- Run time per task: ...
- What the gate failure looked like: ...
- Rerun produced identical counts: yes / no

## Trade-offs
- Spark runs inside the Airflow worker (local mode). Production would submit
  jobs to a cluster or a service like AWS Glue (planned for Phase 3).
- The Airflow image bundles PySpark, so the version must match the notebook
  image and CI.