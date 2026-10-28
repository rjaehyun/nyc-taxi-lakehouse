"""Tiny demonstration of flag_duplicates (Day 7 turns this into real pytest tests).

Run:  docker compose exec spark python -m jobs.demo_dedupe
"""

from jobs.spark_session import get_spark
from jobs.transforms import flag_duplicates


def main() -> None:
    spark = get_spark("demo-dedupe")
    spark.sparkContext.setLogLevel("WARN")

    rows = [
        ("trip-a", ""),                    # unique, clean
        ("trip-b", ""),                    # first copy, clean
        ("trip-b", ""),                    # second copy -> DUPLICATE_TRIP
        ("trip-c", "NON_POSITIVE_FARE"),   # already rejected
        ("trip-c", "NON_POSITIVE_FARE"),   # second copy -> both reasons
    ]
    df = spark.createDataFrame(rows, ["trip_id", "reject_reasons"])
    flag_duplicates(df).orderBy("trip_id", "reject_reasons").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()