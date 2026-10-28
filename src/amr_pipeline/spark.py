"""Spark session configuration for the AMR data pipeline."""

import os

from pyspark.sql import SparkSession

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


def create_spark_session(
    app_name: str = "AMR-Data-Pipeline",
) -> SparkSession:
    """Create or retrieve a local Spark session."""
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark