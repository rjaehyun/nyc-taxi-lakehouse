"""Verify the Gold layer.

Run from the project root:
    docker compose exec spark python -m jobs.check_gold
"""
import sys

from pathlib import Path

from pyspark.sql import functions as F

from jobs.config import lake_path
from jobs.gold_transforms import UNKNOWN_KEY
from jobs.silver import SILVER_TRIPS
from jobs.spark_session import get_spark


def main() -> None:
    spark = get_spark("check-gold")
    spark.sparkContext.setLogLevel("WARN")

    def table(name: str):
        return spark.read.parquet(lake_path("gold", name))

    fact = table("fact_trips")
    failures: list[str] = []

    # 1. Gold keeps every Silver trip, month by month
    print("Row counts (silver vs fact):")
    fact_counts = {r["source_month"]: r["count"] for r in fact.groupBy("source_month").count().collect()}
    
    requested = sys.argv[1:]
    
    for part in sorted(Path(SILVER_TRIPS).glob("source_month=*")):
        month = part.name.split("=")[1]
        silver_rows = spark.read.parquet(str(part)).count()
        fact_rows = fact_counts.get(month, 0)
        print(f"  {month}: silver {silver_rows:,} | fact {fact_rows:,}")
        if silver_rows != fact_rows:
            failures.append(f"{month}: silver {silver_rows:,} vs fact {fact_rows:,}")

    # 2. trip_id must be unique within each processed month
    duplicates = 0
    for part in sorted(Path(SILVER_TRIPS).glob("source_month=*")):
        month = part.name.split("=")[1]
        if requested and month not in requested:
            continue
        month_df = spark.read.parquet(lake_path("gold", "fact_trips") + f"/source_month={month}")
        total = month_df.count()
        distinct = month_df.select("trip_id").distinct().count()
        month_dupes = total - distinct
        duplicates += month_dupes

    # 3. Referential integrity: every fact key must exist in its dimension
    print("\nOrphan keys (fact rows with no matching dimension row):")
    keys = [
        ("pu_zone_key", "dim_zone", "zone_key"),
        ("do_zone_key", "dim_zone", "zone_key"),
        ("payment_type_key", "dim_payment_type", "payment_type_key"),
        ("rate_code_key", "dim_rate_code", "rate_code_key"),
        ("pickup_date_key", "dim_date", "date_key"),
    ]
    for fact_col, dim_name, dim_col in keys:
        dim = table(dim_name)  # read once, so the join condition refers to the same DataFrame
        orphans = fact.join(dim, fact[fact_col] == dim[dim_col], "left_anti").count()
        print(f"  {fact_col} -> {dim_name}: {orphans}")
        if orphans:
            failures.append(f"{orphans:,} orphan {fact_col}")

    # 4. Aggregates reconcile with the fact table
    fact_totals = fact.agg(F.count("*"), F.sum("total_amount")).first()
    agg_totals = table("agg_daily").agg(F.sum("trips"), F.sum("total_revenue")).first()
    print(f"\nFact trips {fact_totals[0]:,} vs agg_daily trips {agg_totals[0]:,}")
    print(f"Fact revenue {fact_totals[1]} vs agg_daily revenue {agg_totals[1]}")
    if fact_totals[0] != agg_totals[0] or fact_totals[1] != agg_totals[1]:
        failures.append("agg_daily does not reconcile with fact_trips")

    # 5. Information only: codes that fell into the unknown member
    print("\nRows mapped to the unknown member (informational):")
    for col in ("payment_type_key", "rate_code_key"):
        print(f"  {col}: {fact.filter(F.col(col) == UNKNOWN_KEY).count():,}")

    spark.stop()
    if failures:
        raise SystemExit("FAILED: " + "; ".join(failures))
    print("\nOK: Gold reconciles with Silver, keys are unique, and every key resolves.")


if __name__ == "__main__":
    main()