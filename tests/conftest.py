"""Shared pytest fixtures for the AMR data pipeline tests."""

import pytest
from pyspark.sql import SparkSession

from amr_pipeline.spark import create_spark_session


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provide a shared local Spark session for the test suite."""
    session = create_spark_session("AMR-Pytest")

    yield session

    session.stop()
