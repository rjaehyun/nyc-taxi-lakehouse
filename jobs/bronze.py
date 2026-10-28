"""Bronze layer: land raw TLC data with ingestion metadata, otherwise unchanged.

Run from the project root:
    docker compose exec spark python -m jobs.bronze              # every file in landing
    docker compose exec spark python -m jobs.bronze 2025-01      # one month only

Safe to rerun: each month overwrites only its own partition.
"""

import re
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.config import LANDING_DIR, lake_path
from jobs.spark_session import get_spark

TRIPS_TABLE = "yellow_trips"
ZONES_TABLE = "taxi_zones"
MONTH_PATTERN = re.compile(r"yellow_tripdata_(\d{4}-\d{2})\.parquet$")


def add_metadata(df: DataFrame, source_file: str) -> DataFrame:
    """Attach lineage columns so every row can be traced to its source."""
    return df.withColumn("source_file", F.lit(source_file)).withColumn(
        "ingested_at", F.current_timestamp()
    )


def ingest_trips(spark: SparkSession, path: Path, month: str) -> int:
    """Copy one month of trips into Bronze. Returns the number of rows written."""
    target = lake_path("bronze", TRIPS_TABLE)

    raw = spark.read.parquet(str(path))
    rows_in = raw.count()

    bronze = add_metadata(raw, path.name).withColumn("source_month", F.lit(month))

    # Dynamic partition overwrite: replace only this month's folder,
    # and leave the other months untouched.
    (
        bronze.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("source_month")
        .parquet(target)
    )

    # Reconciliation: what we wrote must equal what we read.
    rows_out = spark.read.parquet(f"{target}/source_month={month}").count()
    if rows_in != rows_out:
        raise RuntimeError(f"{month}: read {rows_in:,} rows but wrote {rows_out:,}")
    return rows_out


def ingest_zones(spark: SparkSession) -> int:
    """Copy the zone lookup into Bronze. Columns stay as raw strings."""
    path = LANDING_DIR / "taxi_zone_lookup.csv"
    raw = spark.read.option("header", True).csv(str(path))
    bronze = add_metadata(raw, path.name)
    bronze.write.mode("overwrite").parquet(lake_path("bronze", ZONES_TABLE))
    return bronze.count()


def discover_trip_files(requested: list[str]) -> list[tuple[Path, str]]:
    """Find trip files in landing, optionally limited to the requested months."""
    found = []
    for path in sorted(LANDING_DIR.glob("yellow_tripdata_*.parquet")):
        match = MONTH_PATTERN.search(path.name)
        if match and (not requested or match.group(1) in requested):
            found.append((path, match.group(1)))

    missing = set(requested) - {month for _, month in found}
    if missing:
        raise SystemExit(f"No landing file for: {sorted(missing)}")
    if not found:
        raise SystemExit("No trip files in data/landing. Run the download script first.")
    return found


def main(requested: list[str]) -> None:
    spark = get_spark("bronze")
    spark.sparkContext.setLogLevel("WARN")

    print("Zones:")
    print(f"  {ingest_zones(spark):,} rows -> bronze/{ZONES_TABLE}")

    print("Trips:")
    total = 0
    for path, month in discover_trip_files(requested):
        rows = ingest_trips(spark, path, month)
        total += rows
        print(f"  {month}: {rows:,} rows -> bronze/{TRIPS_TABLE}/source_month={month}")

    print(f"\nDone. {total:,} trip rows landed.")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])