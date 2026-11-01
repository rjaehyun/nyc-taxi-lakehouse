# Operations Runbook: NYC Taxi Lakehouse

## Who this is for
Anyone who needs to operate, monitor, or debug the pipeline after it is set up.

## Normal operation

The DAG runs every Monday at 06:00 Asia/Manila. A typical successful run:

1. `plan_months` checks whether TLC has published a new month and picks it.
   If nothing is new, it exits with code 99 and all other tasks are skipped.
2. `download_tlc` downloads the month's Parquet file to `data/landing/`.
   Existing files are skipped, so reruns are cheap.
3. `bronze` copies the file into `data/lake/bronze/`, partitioned by month.
4. `silver` standardizes, validates, and quarantines bad rows.
5. `check_silver` re-applies all quality rules to the Silver output.
   Zero violations is the pass condition.
6. `quality_gate` runs the quality report in strict mode.
   The run stops here if a month rejects more than 6% of its rows.
7. `gold` builds the star schema and aggregates.
8. `check_gold` verifies counts, key uniqueness, and referential integrity.

## How to trigger a manual run

1. Open http://localhost:8080 (or your Airflow host).
2. Find `nyc_taxi_lakehouse` and click the play button.
3. Leave `months` blank for the next published month.
   Enter a month (for example `2025-06`) to reprocess a specific one.

## How to check if the pipeline is healthy

```bash
# In the Airflow UI: look for the green Grid view.

# Or from the command line:
cd airflow
docker compose exec airflow-scheduler \
  airflow dags list-import-errors

# Check the quality report
cat docs/quality_report.md

# Check the alert log
cat data/pipeline_alerts.log
```

## What to do when a task fails

### plan_months fails (not skipped)
- A Python error, not a "nothing to do" signal.
- Open the task log. The most common cause is a network error on the TLC
  URL check. Clear the task and retry.

### download_tlc fails
- A network error or a 404 (the month's file does not exist yet).
- Check the URL: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
- If the file exists, clear the task and retry.

### bronze or silver fails
- Open the task log and find the Python traceback.
- Common causes: out of memory (raise `SPARK_DRIVER_MEMORY`), or a schema
  change in the TLC source file (check `docs/data_quality_findings.md`).

### check_silver fails with violations
- Silver has rows that break a quality rule.
- The log shows which rule and how many rows.
- Check whether the TLC source data changed (new column types, new codes).
- If the data changed legitimately, update the rule in `jobs/transforms.py`.

### quality_gate fails
- The rejection rate for a month exceeded 6%.
- Read the quality report: `cat docs/quality_report.md`.
- Decide: is this a real data problem or a rule that is too strict?
  - Real problem: investigate the TLC source and quarantine data.
  - Rule too strict: update `MAX_REJECT_RATE` with a documented reason.

### check_gold fails
- A count mismatch, orphan key, or memory error.
- If `Cannot allocate memory`: the task ran out of RAM. Raise
  `SPARK_DRIVER_MEMORY` in the DAG file for that task.
- If a count mismatch: delete the Gold layer and rerun from Silver.

## How to reprocess one month

Trigger the DAG with `months = YYYY-MM`. The pipeline runs from the beginning
for only that month and replaces its partitions everywhere. Other months are
untouched.

## How to add a new month manually (skip the planner)

```bash
cd airflow
docker compose exec airflow-worker bash -c \
  "cd /opt/airflow/project && python ingestion/download_tlc.py 2025-06"
```

Then trigger the DAG with `months = 2025-06`.

## Emergency: rebuild from scratch

```powershell
# Delete the lake (landing files are kept)
Remove-Item data\lake -Recurse -Force

# Rebuild all months
# Trigger the DAG with months = "2025-01 2025-02 2025-03 2025-04 2025-05"
```

## How to stop everything

```powershell
cd airflow && docker compose stop
cd .. && docker compose stop
```

## Known issues and workarounds

| Issue | Workaround |
|---|---|
| `check_gold` runs out of memory | Raise to `3g` in the DAG file |
| Scheduled run triggers before TLC publishes | `plan_months` exits 99 and skips. Safe. |
| `airflow.cfg` regenerated in config/ | Already in `.gitignore`. Do not commit it. |
| Port 8080 conflict with weather project | Stop the weather Airflow first |

## File layout

```
data/
├── landing/        raw TLC Parquet files (never deleted)
├── lake/
│   ├── bronze/     raw + metadata, partitioned by source_month
│   ├── silver/     clean, validated, partitioned by source_month
│   ├── quarantine/ rejected rows, partitioned by source_month
│   └── gold/       star schema and aggregates
├── pipeline_alerts.log   failure log from the on_failure_callback
└── quality_report.md     latest quality report (regenerated each run)
```