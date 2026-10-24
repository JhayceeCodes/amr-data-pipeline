"""Tests for Spark session configuration."""

from amr_pipeline.spark import create_spark_session


def test_create_spark_session() -> None:
    spark = create_spark_session("AMR-Test")

    try:
        assert spark.version == "4.2.0"

        df = spark.createDataFrame(
            [("E. coli", "Resistant")],
            ["organism", "susceptibility"],
        )

        assert df.count() == 1
        assert df.first()["organism"] == "E. coli"
    finally:
        spark.stop()
