"""Gold layer: star schema (dimensions + fact) and aggregates.

Run from the project root:
    docker compose exec spark python -m jobs.gold              # every month in Silver
    docker compose exec spark python -m jobs.gold 2025-01      # fact for one month

Dimensions and aggregates are always rebuilt in full (they are small).
The fact table is rewritten one month at a time, so reruns never duplicate rows.
"""

import sys
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs.config import lake_path
from jobs.gold_transforms import (
    PAYMENT_TYPES,
    RATE_CODES,
    build_agg_daily,
    build_agg_hourly,
    build_agg_zone_monthly,
    build_dim_date,
    build_dim_zone,
    build_fact_trips,
    build_lookup_dim,
)
from jobs.silver import SILVER_TRIPS, SILVER_ZONES, count_partition, write_partition
from jobs.spark_session import get_spark


def gold_path(table: str) -> str:
    return lake_path("gold", table)


def write_table(spark: SparkSession, df, table: str) -> None:
    """Overwrite a small table as a single file and report its size."""
    df.coalesce(1).write.mode("overwrite").parquet(gold_path(table))
    print(f"  {table}: {spark.read.parquet(gold_path(table)).count():,} rows")


def silver_months() -> list[str]:
    months = sorted(p.name.split("=")[1] for p in Path(SILVER_TRIPS).glob("source_month=*"))
    if not months:
        raise SystemExit("Silver is empty. Run the silver job first.")
    return months


def build_dimensions(spark: SparkSession, months: list[str]) -> None:
    # dim_date covers whole calendar years, so adding a month never needs a rebuild
    years = sorted({int(m[:4]) for m in months})
    tables = {
        "dim_date": build_dim_date(spark, date(years[0], 1, 1), date(years[-1], 12, 31)),
        "dim_zone": build_dim_zone(spark.read.parquet(SILVER_ZONES)),
        "dim_payment_type": build_lookup_dim(
            spark, PAYMENT_TYPES, "payment_type_key", "payment_type_name"
        ),
        "dim_rate_code": build_lookup_dim(spark, RATE_CODES, "rate_code_key", "rate_code_name"),
    }
    for name, df in tables.items():
        write_table(spark, df, name)


def process_month(spark: SparkSession, month: str) -> int:
    silver = spark.read.parquet(f"{SILVER_TRIPS}/source_month={month}")
    silver = silver.withColumn("source_month", F.lit(month))
    rows_in = silver.count()

    write_partition(build_fact_trips(silver), gold_path("fact_trips"), month)
    rows_out = count_partition(spark, gold_path("fact_trips"), month)

    # Reconciliation: Gold must keep every Silver trip.
    if rows_in != rows_out:
        raise RuntimeError(f"{month}: silver {rows_in:,} rows but fact {rows_out:,} rows")
    print(f"  {month}: {rows_out:,} trips -> fact_trips")
    return rows_out


def build_aggregates(spark: SparkSession) -> None:
    fact = spark.read.parquet(gold_path("fact_trips"))
    write_table(spark, build_agg_daily(fact), "agg_daily")
    write_table(spark, build_agg_hourly(fact), "agg_hourly")
    write_table(spark, build_agg_zone_monthly(fact), "agg_zone_monthly")


def main(requested: list[str]) -> None:
    spark = get_spark("gold")
    spark.sparkContext.setLogLevel("WARN")

    available = silver_months()
    missing = set(requested) - set(available)
    if missing:
        raise SystemExit(f"Not in Silver: {sorted(missing)}")

    print("Dimensions:")
    build_dimensions(spark, available)

    print("Fact:")
    total = sum(process_month(spark, month) for month in (requested or available))

    print("Aggregates:")
    build_aggregates(spark)

    print(f"\nDone. {total:,} trips processed into Gold.")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])