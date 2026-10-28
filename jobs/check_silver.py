"""Verify the Silver layer.

Run from the project root:
    docker compose exec spark python -m jobs.check_silver
"""

from pathlib import Path

from pyspark.sql import functions as F

from jobs.config import lake_path
from jobs.spark_session import get_spark
from jobs.transforms import flag_quality_issues


def main() -> None:
    spark = get_spark("check-silver")
    spark.sparkContext.setLogLevel("WARN")

    zone_ids = [
        row["location_id"]
        for row in spark.read.parquet(lake_path("silver", "taxi_zones")).select("location_id").collect()
    ]

    silver_root = Path(lake_path("silver", "yellow_trips"))
    failures = 0

    for part in sorted(silver_root.glob("source_month=*")):
        month = part.name.split("=")[1]
        df = spark.read.parquet(str(part))

        # 1. Re-apply the rules (including zone rules): a clean layer has zero violations
        violations = (
            flag_quality_issues(df, month, zone_ids).filter(F.col("reject_reasons") != "").count()
        )
        # 2. trip_id must be unique within the month
        total = df.count()
        duplicates = total - df.select("trip_id").distinct().count()
        bounds = df.agg(F.min("pickup_ts"), F.max("pickup_ts")).first()

        print(
            f"{month}: {total:,} rows, pickups {bounds[0]} -> {bounds[1]}, "
            f"rule violations: {violations}, duplicate trip_ids: {duplicates}"
        )
        failures += violations + duplicates

    first = next(iter(sorted(silver_root.glob("source_month=*"))))
    print("\nSilver schema:")
    spark.read.parquet(str(first)).printSchema()

    spark.stop()
    if failures:
        raise SystemExit(f"FAILED: {failures:,} problems found in Silver")
    print("OK: Silver passes its rules, has unique trip_ids, and only valid zones.")


if __name__ == "__main__":
    main()