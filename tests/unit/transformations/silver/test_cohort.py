"""Tests for the Silver culture cohort transformation."""

import pytest
from pyspark.sql import types as T

from amr_pipeline.transformations.silver.cohort import transform_cohort

SCHEMA = T.StructType(
    [
        T.StructField("anon_id", T.StringType(), True),
        T.StructField("pat_enc_csn_id_coded", T.StringType(), True),
        T.StructField("order_proc_id_coded", T.StringType(), True),
        T.StructField("order_time_jittered_utc", T.StringType(), True),
        T.StructField("ordering_mode", T.StringType(), True),
        T.StructField("culture_description", T.StringType(), True),
        T.StructField("was_positive", T.StringType(), True),
        T.StructField("organism", T.StringType(), True),
        T.StructField("antibiotic", T.StringType(), True),
        T.StructField("susceptibility", T.StringType(), True),
    ]
)


@pytest.fixture
def cohort_rows():
    return [
        (
            "JC001",
            "1001",
            "2001",
            "2021-03-15 10:30:00+00:00",
            "Outpatient",
            "URINE",
            "0",
            "Null",
            "Null",
            "Null",
        ),
        (
            "JC002",
            "1002",
            "2002",
            "2021-03-16 11:45:00+00:00",
            "Inpatient",
            "BLOOD",
            "1",
            "Escherichia coli",
            "Ampicillin",
            "Resistant",
        ),
        (
            "JC003",
            "not-an-id",
            "2003",
            "2021-03-17 12:00:00+00:00",
            "Outpatient",
            "URINE",
            "0",
            "Null",
            "Null",
            "Null",
        ),
    ]


def test_transforms_valid_rows_and_quarantines_invalid_rows(spark, cohort_rows):
    source = spark.createDataFrame(cohort_rows, SCHEMA)

    silver, quarantine, metrics = transform_cohort(source)

    assert silver.count() == 2
    assert quarantine.count() == 1
    assert metrics["input_rows"] == 3
    assert metrics["output_rows"] + metrics["quarantined_rows"] == 3

    rejected = quarantine.first()
    assert "invalid_or_missing_encounter_id" in rejected.rejection_reasons


def test_converts_exact_null_placeholders(spark, cohort_rows):
    source = spark.createDataFrame(cohort_rows, SCHEMA)

    silver, _, metrics = transform_cohort(source)

    first = silver.filter("anon_id = 'JC001'").first()

    assert first.organism is None
    assert first.antibiotic is None
    assert first.susceptibility is None
    assert metrics["null_placeholders_converted.organism"] == 2
    assert metrics["null_placeholders_converted.antibiotic"] == 2
    assert metrics["null_placeholders_converted.susceptibility"] == 2


def test_converts_positive_flag_to_boolean(spark, cohort_rows):
    source = spark.createDataFrame(cohort_rows, SCHEMA)

    silver, _, _ = transform_cohort(source)

    flags = {
        row.anon_id: row.was_positive
        for row in silver.select("anon_id", "was_positive").collect()
    }

    assert flags == {"JC001": False, "JC002": True}


def test_quarantine_preserves_original_source_values(spark, cohort_rows):
    source = spark.createDataFrame(cohort_rows, SCHEMA)

    _, quarantine, _ = transform_cohort(source)

    rejected = quarantine.first()

    assert rejected.pat_enc_csn_id_coded == "not-an-id"
    assert rejected.organism == "Null"
    assert rejected.susceptibility == "Null"


def test_missing_required_column_fails(spark, cohort_rows):
    source = spark.createDataFrame(cohort_rows, SCHEMA).drop("anon_id")

    with pytest.raises(ValueError, match="missing required columns"):
        transform_cohort(source)


def test_quarantines_malformed_timestamp(spark, cohort_rows):
    rows = list(cohort_rows)
    row = list(rows[0])
    row[3] = "not-a-timestamp"
    rows[0] = tuple(row)

    source = spark.createDataFrame(rows, SCHEMA)
    silver, quarantine, metrics = transform_cohort(source)

    assert silver.count() == 1
    assert quarantine.count() == 2
    assert any(
        "invalid_or_missing_culture_time" in row.rejection_reasons
        for row in quarantine.collect()
    )
    assert metrics["input_rows"] == (
        metrics["output_rows"] + metrics["quarantined_rows"]
    )


def test_quarantines_invalid_positive_flag(spark, cohort_rows):
    rows = list(cohort_rows)
    row = list(rows[0])
    row[6] = "unknown"
    rows[0] = tuple(row)

    source = spark.createDataFrame(rows, SCHEMA)
    silver, quarantine, _ = transform_cohort(source)

    assert silver.count() == 1
    assert quarantine.count() == 2
    assert any(
        "invalid_or_missing_was_positive" in row.rejection_reasons
        for row in quarantine.collect()
    )
