# NYC Taxi Lakehouse

![CI](https://github.com/rjaehyun/nyc-taxi-lakehouse/actions/workflows/ci.yml/badge.svg)

A medallion-architecture (bronze, silver, gold) data pipeline built with PySpark.
It processes NYC TLC yellow taxi trips (10.4 million trips for January to March
2025) from raw files to an analytics-ready star schema, with data quality rules,
quarantine of rejected rows, and automated tests.

> Status: v0.2, a fully local lakehouse with incremental loading and Airflow orchestration.
> Phase 3 (AWS S3, Glue, Athena, Terraform) is next.

## Architecture

```
TLC monthly Parquet files + zone lookup CSV
        │
        ▼
    BRONZE    raw data exactly as published, plus lineage columns
        │     (source_file, ingested_at, source_month)
        ▼
    SILVER    standardized names, enforced types, quality rules,
        │     deterministic trip_id, duplicates removed
        │            └──► QUARANTINE: rejected rows with reason codes
        ▼
     GOLD     star schema: fact_trips + 4 dimensions + 3 aggregates
```

## Tech stack

| Area | Tool |
|---|---|
| Processing | PySpark (Spark 4) in Docker |
| Storage | Parquet, partitioned by month |
| Testing | pytest (unit tests on tiny DataFrames) |
| CI | GitHub Actions (ruff lint and Spark tests) |
| Coming next | Airflow, AWS S3, Glue, Athena, dbt, Terraform |

## What makes it production-minded

| Practice | How it works |
|---|---|
| Nothing disappears | Rejected rows go to quarantine with reason codes, and each job checks that rows in = rows out |
| Idempotent runs | Each month replaces only its own partition, so reruns never duplicate data |
| Explicit contract | Silver has a fixed schema, so a source schema change cannot silently change the output |
| Quality gate | `quality_report --strict` fails when a month rejects more than 5% of its rows |
| Tested logic | Every quality rule is tested in isolation, and transformations are pure functions |
| Verified layers | `check_silver` and `check_gold` re-validate rules, key uniqueness, and referential integrity |

## Data model (Gold)

| Table | Grain |
|---|---|
| fact_trips | one row per trip |
| dim_date, dim_zone, dim_payment_type, dim_rate_code | one row per member (the lookups include an "unknown" row) |
| agg_daily, agg_hourly, agg_zone_monthly | daily, hourly, and zone-month rollups |

See [docs/gold_model.md](docs/gold_model.md) for the diagram and design decisions,
[docs/silver_rules.md](docs/silver_rules.md) for every quality rule and the reasoning,
and [docs/quality_report.md](docs/quality_report.md) for rejection rates.

## Example findings

- JFK Airport is 4th by trip count but 1st by revenue, because airport fares are much higher.
- Manhattan to Manhattan is about 84% of all trips, and Queens to Manhattan trips average $79.80, consistent with airport traffic.

## Getting started

Requires Docker Desktop and Python 3.12.

```bash
git clone https://github.com/rjaehyun/nyc-taxi-lakehouse.git
cd nyc-taxi-lakehouse
python -m venv .venv
.venv\Scripts\activate            # Mac/Linux: source .venv/bin/activate
pip install -r requirements.txt

python ingestion/download_tlc.py 2025-01 2025-02 2025-03
docker compose up -d

docker compose exec spark python -m jobs.bronze
docker compose exec spark python -m jobs.silver
docker compose exec spark python -m jobs.gold
```

Verify and explore:

```bash
docker compose exec spark python -m jobs.check_silver
docker compose exec spark python -m jobs.check_gold
docker compose exec spark python -m jobs.quality_report
docker compose exec spark python -m jobs.gold_queries
```

Run the tests:

```bash
docker compose exec spark pip install pytest
docker compose exec spark python -m pytest tests -v
```

## Project structure

```
ingestion/   download script for TLC files
jobs/        Spark jobs: bronze, silver, gold, checks, quality report
tests/       pytest tests for the transformation logic
sql/gold/    analytical queries over the Gold tables
docs/        lake layout, rules, model, quality report, notes
```

## Next steps

- [ ] Orchestrate with Airflow (monthly runs, retries, quality gate)
- [ ] Move storage to AWS S3 and run the jobs on AWS Glue
- [ ] Query with Athena and model with dbt
- [ ] Provision infrastructure with Terraform
- [ ] Enrich trips with weather data

## Data source

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
Check their terms of use for attribution requirements.

## How to run

### Start everything

```bash
# 1. Start the weather database (if you also run the weather pipeline)
# cd weather-etl-pipeline && docker compose up -d && cd ..

# 2. Start the lakehouse database (not used in Phase 1-2, but reserved)
# docker compose up -d

# 3. Start Airflow
cd airflow
cp .env.example .env   # then set a Fernet key
docker compose up airflow-init
docker compose up -d

# 4. Open the UI
open http://localhost:8080
```

### Trigger a run

In the Airflow UI, find `nyc_taxi_lakehouse`, click Trigger, and leave
`months` blank. Or specify a month to reprocess: `months = 2025-04`.

### Run the tests

```bash
pytest tests -v
```

See `docs/runbook.md` for operating instructions and troubleshooting.
