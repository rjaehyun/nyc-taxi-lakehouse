"""Pure transformation functions for the Silver layer (no file reading or writing)."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)

MONEY = DecimalType(10, 2)

# Raw (lower-cased) column name -> clean name. Anything else is just lower-cased,
# which also absorbs drift like "Airport_fee" vs "airport_fee".
COLUMN_RENAMES = {
    "vendorid": "vendor_id",
    "ratecodeid": "ratecode_id",
    "pulocationid": "pu_location_id",
    "dolocationid": "do_location_id",
    "tpep_pickup_datetime": "pickup_ts",
    "tpep_dropoff_datetime": "dropoff_ts",
}

# The Silver contract: column name -> type. Missing source columns become NULL,
# so every month ends up with the same schema even if the source changed.
SILVER_SCHEMA = {
    "vendor_id": IntegerType(),
    "pickup_ts": TimestampType(),
    "dropoff_ts": TimestampType(),
    "passenger_count": IntegerType(),
    "trip_distance": DoubleType(),
    "ratecode_id": IntegerType(),
    "store_and_fwd_flag": StringType(),
    "pu_location_id": IntegerType(),
    "do_location_id": IntegerType(),
    "payment_type": IntegerType(),
    "fare_amount": MONEY,
    "extra": MONEY,
    "mta_tax": MONEY,
    "tip_amount": MONEY,
    "tolls_amount": MONEY,
    "improvement_surcharge": MONEY,
    "total_amount": MONEY,
    "congestion_surcharge": MONEY,
    "airport_fee": MONEY,
    "cbd_congestion_fee": MONEY,
}

LINEAGE_COLUMNS = ["source_file", "ingested_at", "source_month"]

# Thresholds, in one place so they are easy to defend and to change.
MAX_TRIP_HOURS = 24
MAX_DISTANCE_MILES = 200
MAX_PASSENGERS = 8


def standardize_columns(df: DataFrame) -> DataFrame:
    """Lower-case every column name and apply the explicit renames."""
    for old in df.columns:
        lowered = old.lower()
        new = COLUMN_RENAMES.get(lowered, lowered)
        if new != old:
            df = df.withColumnRenamed(old, new)
    return df


def unexpected_columns(df: DataFrame) -> list[str]:
    """Columns in the data that the Silver contract does not know about."""
    known = set(SILVER_SCHEMA) | set(LINEAGE_COLUMNS)
    return sorted(set(df.columns) - known)


def cast_types(df: DataFrame) -> DataFrame:
    """Select exactly the contract columns, in order, with the right types."""
    columns = []
    for name, dtype in SILVER_SCHEMA.items():
        if name in df.columns:
            columns.append(F.col(name).cast(dtype).alias(name))
        else:
            columns.append(F.lit(None).cast(dtype).alias(name))
    lineage = [F.col(c) for c in LINEAGE_COLUMNS if c in df.columns]
    return df.select(*columns, *lineage)


def quality_rules(month: str) -> dict[str, Column]:
    """Reason code -> condition that is True when a row breaks the rule."""
    duration_hours = (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 3600
    return {
        "NULL_TIMESTAMP": F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull(),
        "NULL_LOCATION": F.col("pu_location_id").isNull() | F.col("do_location_id").isNull(),
        "NULL_AMOUNT": (
            F.col("fare_amount").isNull()
            | F.col("total_amount").isNull()
            | F.col("trip_distance").isNull()
        ),
        "DROPOFF_BEFORE_PICKUP": F.col("dropoff_ts") < F.col("pickup_ts"),
        "TRIP_TOO_LONG": duration_hours > MAX_TRIP_HOURS,
        "PICKUP_OUTSIDE_MONTH": F.date_format("pickup_ts", "yyyy-MM") != F.lit(month),
        "NON_POSITIVE_FARE": F.col("fare_amount") <= 0,
        "NEGATIVE_TOTAL": F.col("total_amount") < 0,
        "NON_POSITIVE_DISTANCE": F.col("trip_distance") <= 0,
        "DISTANCE_OUTLIER": F.col("trip_distance") > MAX_DISTANCE_MILES,
        # NULL and 0 passengers are kept as "unknown"; only impossible values are rejected.
        "INVALID_PASSENGER_COUNT": (
            (F.col("passenger_count") < 0) | (F.col("passenger_count") > MAX_PASSENGERS)
        ),
    }


def flag_quality_issues(df: DataFrame, month: str) -> DataFrame:
    """Add reject_reasons: '' for a clean row, else codes joined by '|'."""
    checks = [
        # coalesce: a rule on a NULL value is "not triggered" (the NULL_* rules catch those)
        F.when(F.coalesce(condition, F.lit(False)), F.lit(code))
        for code, condition in quality_rules(month).items()
    ]
    # concat_ws skips NULLs, so only the rules that fired remain
    return df.withColumn("reject_reasons", F.concat_ws("|", *checks))