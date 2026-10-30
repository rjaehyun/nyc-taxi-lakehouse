"""One place that builds the SparkSession, so every job configures Spark the same way."""

import os
from pyspark.sql import SparkSession


def get_spark(app_name: str = "nyc-taxi-lakehouse") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")  # use all CPU cores inside the container
        .config("spark.driver.memory", "3g")
        .config("spark.sql.session.timeZone", "UTC")  # wall-clock times stay unshifted
        .config("spark.sql.ansi.enabled", "false")  # bad casts become NULL, not errors
        .getOrCreate()
    )