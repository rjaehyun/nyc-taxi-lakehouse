"""One place that builds the SparkSession, so every job configures Spark the same way."""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "nyc-taxi-lakehouse") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )