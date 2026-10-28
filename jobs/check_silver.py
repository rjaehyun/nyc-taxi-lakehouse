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

    silver_root = Path(lake_path("silver", "yellow_trips"))
    failures = 0

    for part in sorted(silver_root.glob("source_month=*")):
        month = part.name.split("=")[1]
        df = spark.read.parquet(str(part))

        # Re-apply the rules: a clean layer must produce zero violations.
        violations = flag_quality_issues(df, month).filter(F.col("reject_reasons") != "").count()
        bounds = df.agg(F.min("pickup_ts"), F.max("pickup_ts"), F.count("*")).first()
        print(f"{month}: {bounds[2]:,} rows, pickups {bounds[0]} -> {bounds[1]}, rule violations: {violations}")
        failures += violations

    first = next(iter(sorted(silver_root.glob("source_month=*"))))
    print("\nSilver schema:")
    spark.read.parquet(str(first)).printSchema()

    quarantine_root = Path(lake_path("quarantine", "yellow_trips"))
    print("Sample quarantined rows:")
    (
        spark.read.parquet(str(next(iter(sorted(quarantine_root.glob("source_month=*"))))))
        .select("pickup_ts", "dropoff_ts", "trip_distance", "fare_amount", "total_amount", "reject_reasons")
        .show(10, truncate=False)
    )

    spark.stop()
    if failures:
        raise SystemExit(f"FAILED: {failures:,} violations found in Silver")
    print("OK: Silver passes all of its own quality rules.")


if __name__ == "__main__":
    main()