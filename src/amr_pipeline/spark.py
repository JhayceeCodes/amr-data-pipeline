"""Spark session configuration for the AMR data pipeline."""

from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str = "AMR-Data-Pipeline",
) -> SparkSession:
    """Create or retrieve a local Spark session."""
    return SparkSession.builder.master("local[*]").appName(app_name).getOrCreate()
