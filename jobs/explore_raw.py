"""Explore the raw TLC files with Spark: schemas, schema drift, row counts.

Run from the project root:
    docker compose exec spark python -m jobs.explore_raw
"""

from pathlib import Path

from pyspark.sql import functions as F

from jobs.spark_session import get_spark

LANDING = Path("data/landing").resolve()


def schema_of(df) -> dict:
    """Column name -> type string, e.g. {'fare_amount': 'double'}."""
    return {f.name: f.dataType.simpleString() for f in df.schema.fields}


def main() -> None:
    spark = get_spark("explore-raw")
    spark.sparkContext.setLogLevel("WARN")

    files = sorted(LANDING.glob("yellow_tripdata_*.parquet"))
    if not files:
        raise SystemExit("No trip files in data/landing. Run the download script first.")

    # Read each file separately. Reading them together can fail if column
    # types differ between months, which is itself a finding worth knowing.
    schemas = {}
    for path in files:
        df = spark.read.parquet(str(path))
        schemas[path.name] = schema_of(df)
        print(f"{path.name}: {df.count():,} rows, {len(df.columns)} columns")

    # Schema drift check against the first file
    base_name, base = next(iter(schemas.items()))
    print(f"\nSchema drift check (baseline: {base_name})")
    for name, cols in schemas.items():
        if cols == base:
            print(f"  {name}: identical schema")
            continue
        added = sorted(set(cols) - set(base))
        removed = sorted(set(base) - set(cols))
        retyped = sorted(c for c in set(cols) & set(base) if cols[c] != base[c])
        print(f"  {name}: DIFFERENT")
        print(f"    added:   {added}")
        print(f"    removed: {removed}")
        print(f"    retyped: {[(c, base[c], cols[c]) for c in retyped]}")

    # DataFrame API: trips and average fare by pickup hour (first file)
    trips = spark.read.parquet(str(files[0]))
    print("\nTrips by pickup hour (DataFrame API):")
    (
        trips.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .groupBy("pickup_hour")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        )
        .orderBy("pickup_hour")
        .show(24)
    )

    # The same idea in Spark SQL
    trips.createOrReplaceTempView("trips")
    print("Average tip by payment type (Spark SQL):")
    spark.sql(
        """
        SELECT payment_type,
               COUNT(*) AS trips,
               ROUND(AVG(tip_amount), 2) AS avg_tip
        FROM trips
        GROUP BY payment_type
        ORDER BY trips DESC
        """
    ).show()

    spark.stop()


if __name__ == "__main__":
    main()