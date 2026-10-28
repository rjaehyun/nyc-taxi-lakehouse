"""Run every query in sql/gold/ against the Gold tables.

Run from the project root:
    docker compose exec spark python -m jobs.gold_queries
"""

from pathlib import Path

from jobs.config import lake_path
from jobs.spark_session import get_spark

TABLES = [
    "fact_trips",
    "dim_date",
    "dim_zone",
    "dim_payment_type",
    "dim_rate_code",
    "agg_daily",
    "agg_hourly",
    "agg_zone_monthly",
]


def main() -> None:
    spark = get_spark("gold-queries")
    spark.sparkContext.setLogLevel("WARN")

    for table in TABLES:
        spark.read.parquet(lake_path("gold", table)).createOrReplaceTempView(table)

    for path in sorted(Path("sql/gold").glob("*.sql")):
        print(f"\n{'=' * 60}\n{path.name}\n{'=' * 60}")
        spark.sql(path.read_text(encoding="utf-8").strip().rstrip(";")).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()