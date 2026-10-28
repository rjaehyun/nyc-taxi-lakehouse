"""Inspect the Bronze layer.

Run from the project root:
    docker compose exec spark python -m jobs.check_bronze
"""

from pathlib import Path

from pyspark.sql import functions as F

from jobs.config import lake_path
from jobs.spark_session import get_spark


def main() -> None:
    spark = get_spark("check-bronze")
    spark.sparkContext.setLogLevel("WARN")

    trips_root = Path(lake_path("bronze", "yellow_trips"))
    print("Trips by partition:")
    for part in sorted(trips_root.glob("source_month=*")):
        df = spark.read.parquet(str(part))
        row = df.agg(
            F.count("*").alias("rows"),
            F.min("ingested_at").alias("first_ingest"),
            F.max("ingested_at").alias("last_ingest"),
        ).first()
        files = len(list(part.glob("*.parquet")))
        print(
            f"  {part.name}: {row['rows']:,} rows, {files} file(s), "
            f"ingested {row['first_ingest']} -> {row['last_ingest']}"
        )

    sample = spark.read.parquet(str(next(iter(sorted(trips_root.glob("source_month=*"))))))
    print("\nSchema of one partition (note the three metadata columns at the end):")
    sample.printSchema()

    zones = spark.read.parquet(lake_path("bronze", "taxi_zones"))
    print(f"Zones: {zones.count()} rows")
    zones.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()