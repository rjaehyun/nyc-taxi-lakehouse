"""Builders for tiny, typed test DataFrames."""

from datetime import datetime
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from jobs.transforms import SILVER_SCHEMA

# One clean, realistic trip. Tests override only the fields they care about.
BASE_TRIP = {
    "vendor_id": 1,
    "pickup_ts": datetime(2025, 1, 15, 10, 0, 0),
    "dropoff_ts": datetime(2025, 1, 15, 10, 30, 0),  # a 30 minute trip
    "passenger_count": 1,
    "trip_distance": 2.5,
    "ratecode_id": 1,
    "store_and_fwd_flag": "N",
    "pu_location_id": 161,
    "do_location_id": 236,
    "payment_type": 1,  # credit card
    "fare_amount": Decimal("15.00"),
    "extra": Decimal("0.50"),
    "mta_tax": Decimal("0.50"),
    "tip_amount": Decimal("3.00"),
    "tolls_amount": Decimal("0.00"),
    "improvement_surcharge": Decimal("1.00"),
    "congestion_surcharge": Decimal("2.50"),
    "airport_fee": Decimal("0.00"),
    "cbd_congestion_fee": Decimal("0.00"),
    "total_amount": Decimal("22.50"),
}


def make_trips(spark: SparkSession, *overrides: dict) -> DataFrame:
    """Typed rows in the Silver column contract: one row per dict of overrides.

    make_trips(spark, {}) gives one clean trip.
    make_trips(spark, {}, {"fare_amount": Decimal("0.00")}) gives two rows.
    Setting a field to None makes it NULL.
    """
    schema = StructType([StructField(name, dtype, True) for name, dtype in SILVER_SCHEMA.items()])
    rows = []
    for override in overrides:
        trip = {**BASE_TRIP, **override}
        rows.append(tuple(trip.get(name) for name in SILVER_SCHEMA))
    return spark.createDataFrame(rows, schema)


def make_silver(spark: SparkSession, *overrides: dict) -> DataFrame:
    """Like make_trips, plus the columns Gold expects to find in Silver."""
    return (
        make_trips(spark, *overrides)
        .withColumn("trip_id", F.monotonically_increasing_id().cast("string"))
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("source_month", F.lit("2025-01"))
    )