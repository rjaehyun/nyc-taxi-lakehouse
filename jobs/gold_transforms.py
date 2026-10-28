"""Pure builders for the Gold star schema (no file reading or writing)."""

from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

UNKNOWN_KEY = -1

# From the TLC yellow taxi data dictionary. Verify against the current version
# on the TLC site; any code missing here lands in the "unknown" row.
PAYMENT_TYPES = {
    0: "Flex fare trip",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}
RATE_CODES = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride",
    99: "Unknown",
}


def build_lookup_dim(
    spark: SparkSession, mapping: dict[int, str], key_col: str, name_col: str
) -> DataFrame:
    """Small code -> name dimension, plus the unknown member."""
    rows = list(mapping.items()) + [(UNKNOWN_KEY, "Missing or unmapped code")]
    return spark.createDataFrame(rows, f"{key_col} int, {name_col} string")


def build_dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    """One row per calendar day. day_of_week follows ISO: Monday=1 ... Sunday=7."""
    days = spark.sql(
        f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date"
    )
    iso_dow = ((F.dayofweek("date") + 5) % 7) + 1  # Spark: Sunday=1 -> ISO: Sunday=7
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        F.date_format("date", "EEEE").alias("day_name"),
        iso_dow.alias("day_of_week"),
        (iso_dow >= 6).alias("is_weekend"),
        F.weekofyear("date").alias("week_of_year"),
    )


def build_dim_zone(zones: DataFrame) -> DataFrame:
    return zones.select(
        F.col("location_id").alias("zone_key"), "borough", "zone", "service_zone"
    )


def _known_or_unknown(column: str, mapping: dict[int, str]) -> Column:
    """Keep the code if it is in the lookup, else point at the unknown member."""
    return (
        F.when(F.col(column).isin(list(mapping)), F.col(column))
        .otherwise(F.lit(UNKNOWN_KEY))
        .cast("int")
    )


def build_fact_trips(silver: DataFrame) -> DataFrame:
    """One row per trip, with dimension keys and a few derived measures.

    Expects Silver columns plus source_month (added back by the job, because
    it lives in the folder name rather than inside the files).
    """
    duration_min = (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0
    return silver.select(
        "trip_id",
        F.date_format("pickup_date", "yyyyMMdd").cast("int").alias("pickup_date_key"),
        "pickup_ts",
        "dropoff_ts",
        F.hour("pickup_ts").alias("pickup_hour"),
        F.col("pu_location_id").alias("pu_zone_key"),
        F.col("do_location_id").alias("do_zone_key"),
        _known_or_unknown("payment_type", PAYMENT_TYPES).alias("payment_type_key"),
        _known_or_unknown("ratecode_id", RATE_CODES).alias("rate_code_key"),
        "vendor_id",
        "passenger_count",
        "trip_distance",
        F.round(duration_min, 2).alias("trip_duration_min"),
        F.when(
            duration_min >= 1,
            F.round(F.col("trip_distance") / (duration_min / 60.0), 1),
        ).alias("avg_speed_mph"),
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
        "total_amount",
        # Only credit card tips are recorded; fare > 0 is guaranteed by Silver.
        F.when(
            F.col("payment_type") == 1,
            (F.col("tip_amount") / F.col("fare_amount")).cast("double"),
        ).alias("tip_pct"),
        "source_month",
    )


def build_agg_daily(fact: DataFrame) -> DataFrame:
    return (
        fact.groupBy("pickup_date_key")
        .agg(
            F.count("*").alias("trips"),
            F.sum("total_amount").alias("total_revenue"),
            F.sum("fare_amount").alias("fare_revenue"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi"),
            F.round(F.avg("trip_duration_min"), 1).alias("avg_duration_min"),
            F.round(F.avg("tip_pct") * 100, 1).alias("avg_card_tip_pct"),
        )
        .withColumn(
            "pickup_date", F.to_date(F.col("pickup_date_key").cast("string"), "yyyyMMdd")
        )
    )


def build_agg_hourly(fact: DataFrame) -> DataFrame:
    return fact.groupBy("pickup_date_key", "pickup_hour").agg(
        F.count("*").alias("trips"),
        F.sum("total_amount").alias("total_revenue"),
        F.round(F.avg("avg_speed_mph"), 1).alias("avg_speed_mph"),
    )


def build_agg_zone_monthly(fact: DataFrame) -> DataFrame:
    return fact.groupBy("source_month", "pu_zone_key").agg(
        F.count("*").alias("trips"),
        F.sum("total_amount").alias("total_revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("tip_pct") * 100, 1).alias("avg_card_tip_pct"),
    )