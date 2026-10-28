"""Silver layer: standardize, type, validate, deduplicate, and quarantine.

Run from the project root:
    docker compose exec spark python -m jobs.silver              # every month in Bronze
    docker compose exec spark python -m jobs.silver 2025-01      # one month only

Safe to rerun: each month replaces only its own partition.
"""

import shutil
import sys
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs.config import lake_path
from jobs.spark_session import get_spark
from jobs.transforms import (
    add_trip_id,
    cast_types,
    clean_zones,
    flag_duplicates,
    flag_quality_issues,
    standardize_columns,
    unexpected_columns,
)

BRONZE_TRIPS = lake_path("bronze", "yellow_trips")
BRONZE_ZONES = lake_path("bronze", "taxi_zones")
SILVER_TRIPS = lake_path("silver", "yellow_trips")
SILVER_ZONES = lake_path("silver", "taxi_zones")
QUARANTINE_TRIPS = lake_path("quarantine", "yellow_trips")


def clear_partition(table_root: str, month: str) -> None:
    """Delete one month's folder before rewriting it (local filesystem only).

    Spark writes nothing for an empty DataFrame, so without this a stale
    partition from an earlier run could survive a rerun. Day 13 replaces
    this for S3.
    """
    shutil.rmtree(Path(table_root) / f"source_month={month}", ignore_errors=True)


def write_partition(df, table_root: str, month: str) -> None:
    clear_partition(table_root, month)
    (
        df.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("source_month")
        .parquet(table_root)
    )


def count_partition(spark: SparkSession, table_root: str, month: str) -> int:
    path = Path(table_root) / f"source_month={month}"
    return spark.read.parquet(str(path)).count() if path.exists() else 0


def process_zones(spark: SparkSession) -> list[int]:
    """Clean the zone lookup, write it to Silver, and return the valid zone IDs."""
    zones = clean_zones(spark.read.parquet(BRONZE_ZONES))
    total = zones.count()

    zones = zones.filter(F.col("location_id").isNotNull()).dropDuplicates(["location_id"])
    kept = zones.count()
    if kept == 0:
        raise RuntimeError("Zone lookup is empty after cleaning")

    zones.write.mode("overwrite").parquet(SILVER_ZONES)
    print(f"  zones: {total} in -> {kept} silver ({total - kept} dropped: null or duplicate id)")

    # The zone table is tiny (a few hundred rows), so collecting the IDs is fine.
    # For a large dimension you would use a broadcast join instead.
    return sorted(row["location_id"] for row in zones.select("location_id").collect())


def process_month(spark: SparkSession, month: str, valid_zone_ids: list[int]) -> dict:
    raw = spark.read.parquet(f"{BRONZE_TRIPS}/source_month={month}")
    # the partition column lives in the folder name, so add it back explicitly
    raw = raw.withColumn("source_month", F.lit(month))
    rows_in = raw.count()

    standardized = standardize_columns(raw)
    extra = unexpected_columns(standardized)
    if extra:
        print(f"  note: columns not in the Silver contract (dropped): {extra}")

    typed = add_trip_id(cast_types(standardized))
    flagged = flag_duplicates(flag_quality_issues(typed, month, valid_zone_ids))
    flagged = flagged.persist(StorageLevel.MEMORY_AND_DISK)  # reused by several actions below

    try:
        is_clean = F.col("reject_reasons") == ""
        silver = flagged.filter(is_clean).drop("reject_reasons")
        silver = silver.withColumn("pickup_date", F.to_date("pickup_ts"))
        quarantine = flagged.filter(~is_clean)

        write_partition(silver, SILVER_TRIPS, month)
        write_partition(quarantine, QUARANTINE_TRIPS, month)

        silver_rows = count_partition(spark, SILVER_TRIPS, month)
        quarantine_rows = count_partition(spark, QUARANTINE_TRIPS, month)

        # Reconciliation: every input row must land in exactly one place.
        if rows_in != silver_rows + quarantine_rows:
            raise RuntimeError(
                f"{month}: {rows_in:,} in, but {silver_rows:,} silver + "
                f"{quarantine_rows:,} quarantined = {silver_rows + quarantine_rows:,}"
            )

        print(
            f"  {month}: {rows_in:,} in -> {silver_rows:,} silver, "
            f"{quarantine_rows:,} quarantined ({quarantine_rows / rows_in:.2%})"
        )
        if quarantine_rows:
            print("  rejection reasons (a row with several reasons counts in each):")
            (
                quarantine.select(F.explode(F.split("reject_reasons", r"\|")).alias("reason"))
                .groupBy("reason")
                .count()
                .orderBy(F.desc("count"))
                .show(truncate=False)
            )
        return {"month": month, "rows_in": rows_in, "silver": silver_rows, "quarantine": quarantine_rows}
    finally:
        flagged.unpersist()


def discover_months(requested: list[str]) -> list[str]:
    available = sorted(p.name.split("=")[1] for p in Path(BRONZE_TRIPS).glob("source_month=*"))
    months = requested or available
    missing = set(months) - set(available)
    if missing:
        raise SystemExit(f"Not in Bronze: {sorted(missing)}. Run the bronze job first.")
    if not months:
        raise SystemExit("Bronze is empty. Run the bronze job first.")
    return months


def main(requested: list[str]) -> None:
    spark = get_spark("silver")
    spark.sparkContext.setLogLevel("WARN")

    print("Zones:")
    valid_zone_ids = process_zones(spark)

    print("Trips:")
    results = [process_month(spark, month, valid_zone_ids) for month in discover_months(requested)]

    total_in = sum(r["rows_in"] for r in results)
    total_silver = sum(r["silver"] for r in results)
    print(f"\nDone. {total_in:,} in -> {total_silver:,} silver across {len(results)} month(s).")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])