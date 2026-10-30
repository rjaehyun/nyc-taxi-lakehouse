FROM apache/airflow:3.3.2

# Spark needs Java. default-jre-headless installs whatever Java version
# this Debian release ships, and Spark 4 supports 17 and 21.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Same PySpark version as the Docker notebook image and CI
ARG PYSPARK_VERSION=4.2.0
RUN pip install --no-cache-dir "pyspark==${PYSPARK_VERSION}"