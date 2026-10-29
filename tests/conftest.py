"""Shared pytest setup: one small Spark session for the whole test run."""

import os
import time

import pytest
from pyspark.sql import SparkSession

# Python converts naive datetimes using its own timezone when building DataFrames.
# Pinning it to UTC makes the tests behave the same on every machine.
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):  # not available on Windows, but tests run on Linux
    time.tzset()


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("lakehouse-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")  # tiny data, so few partitions
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        # The two settings our transforms depend on (see jobs/spark_session.py)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()