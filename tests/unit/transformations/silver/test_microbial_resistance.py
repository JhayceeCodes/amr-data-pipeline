"""Tests for the Silver microbial resistance transformation."""


import pytest
from pyspark.sql import types as T
from pyspark.sql import functions as F

from amr_pipeline.transformations.silver.microbial_resistance import (
    transform_microbial_resistance,
)


@pytest.fixture
def source_schema() -> T.StructType:
    """Return the expected Bronze source schema."""
    columns = [
        "anon_id",
        "pat_enc_csn_id_coded",
        "order_proc_id_coded",
        "order_time_jittered_utc",
        "organism",
        "antibiotic",
        "resistant_time_to_culturetime",
    ]

    return T.StructType(
        [
            T.StructField(column, T.StringType(), True)
            for column in columns
        ]
    )


@pytest.fixture
def valid_row() -> dict[str, str]:
    """Return a valid microbial resistance observation."""
    return {
        "anon_id": "1001",
        "pat_enc_csn_id_coded": "2001",
        "order_proc_id_coded": "3001",
        "order_time_jittered_utc": "2020-05-10 12:30:00+00:00",
        "organism": "Escherichia coli",
        "antibiotic": "Ciprofloxacin",
        "resistant_time_to_culturetime": "120",
    }


def make_source(spark, schema, rows):
    """Create a Spark DataFrame from source records."""
    return spark.createDataFrame(rows, schema=schema)




def test_transforms_valid_record(spark, source_schema, valid_row):
    source = make_source(spark, source_schema, [valid_row])

    silver, quarantine, metrics = transform_microbial_resistance(source)

    row = silver.first()

    assert silver.count() == 1
    assert quarantine.count() == 0

    assert row["anon_id"] == "1001"
    assert row["encounter_id"] == 2001
    assert row["culture_order_id"] == 3001

    # Validate the timestamp instant independently of Python's local timezone.
    epoch_seconds = (
        silver.select(
            F.unix_timestamp("culture_time").alias("epoch_seconds")
        ).first()["epoch_seconds"]
    )
    assert epoch_seconds == 1589113800

    assert row["organism"] == "Escherichia coli"
    assert row["antibiotic"] == "Ciprofloxacin"
    assert row["resistant_time_to_culturetime"] == 120

    assert metrics["input_rows"] == 1
    assert metrics["output_rows"] == 1
    assert metrics["quarantined_rows"] == 0


def test_silver_schema_and_data_types(spark, source_schema, valid_row):
    source = make_source(spark, source_schema, [valid_row])

    silver, _, _ = transform_microbial_resistance(source)

    expected_types = {
        "anon_id": T.StringType,
        "encounter_id": T.LongType,
        "culture_order_id": T.LongType,
        "culture_time": T.TimestampType,
        "organism": T.StringType,
        "antibiotic": T.StringType,
        "resistant_time_to_culturetime": T.IntegerType,
    }

    actual_fields = {
        field.name: type(field.dataType)
        for field in silver.schema.fields
    }

    assert actual_fields == expected_types


@pytest.mark.parametrize(
    ("source_column", "value"),
    [
        ("anon_id", None),
        ("anon_id", "Null"),
        ("pat_enc_csn_id_coded", None),
        ("pat_enc_csn_id_coded", "invalid"),
        ("order_proc_id_coded", None),
        ("order_proc_id_coded", "invalid"),
        ("order_time_jittered_utc", None),
        ("order_time_jittered_utc", "not-a-timestamp"),
        ("organism", None),
        ("organism", "Null"),
        ("antibiotic", None),
        ("antibiotic", "Null"),
    ],
)
def test_invalid_required_values_are_quarantined(
    spark,
    source_schema,
    valid_row,
    source_column,
    value,
):
    invalid_row = valid_row.copy()
    invalid_row[source_column] = value

    source = make_source(spark, source_schema, [invalid_row])

    silver, quarantine, metrics = transform_microbial_resistance(source)

    assert silver.count() == 0
    assert quarantine.count() == 1
    assert metrics["output_rows"] == 0
    assert metrics["quarantined_rows"] == 1

    reasons = quarantine.first()["rejection_reasons"]
    assert reasons


def test_multiple_required_field_failures_have_multiple_reasons(
    spark,
    source_schema,
    valid_row,
):
    invalid_row = valid_row.copy()
    invalid_row["anon_id"] = "Null"
    invalid_row["pat_enc_csn_id_coded"] = "not-an-id"
    invalid_row["organism"] = None
    invalid_row["antibiotic"] = "Null"

    source = make_source(spark, source_schema, [invalid_row])

    silver, quarantine, _ = transform_microbial_resistance(source)

    assert silver.count() == 0

    reasons = quarantine.first()["rejection_reasons"]

    assert "missing_anon_id" in reasons
    assert "invalid_or_missing_encounter_id" in reasons
    assert "missing_organism" in reasons
    assert "missing_antibiotic" in reasons


def test_quarantine_preserves_original_source_values(
    spark,
    source_schema,
    valid_row,
):
    invalid_row = valid_row.copy()
    invalid_row["pat_enc_csn_id_coded"] = "bad-encounter-id"
    invalid_row["resistant_time_to_culturetime"] = "bad-interval"

    source = make_source(spark, source_schema, [invalid_row])

    _, quarantine, _ = transform_microbial_resistance(source)

    row = quarantine.first()

    assert row["pat_enc_csn_id_coded"] == "bad-encounter-id"
    assert row["resistant_time_to_culturetime"] == "bad-interval"
    assert "invalid_or_missing_encounter_id" in row["rejection_reasons"]


def test_exact_null_placeholders_are_normalized_and_counted(
    spark,
    source_schema,
    valid_row,
):
    row = valid_row.copy()
    row["resistant_time_to_culturetime"] = "Null"

    source = make_source(spark, source_schema, [row])

    silver, _, metrics = transform_microbial_resistance(source)

    assert silver.count() == 1
    assert silver.first()["resistant_time_to_culturetime"] is None

    assert metrics["null_placeholders_converted"][
        "resistant_time_to_culturetime"
    ] == 1
    assert metrics["null_placeholders_converted_total"] == 1


def test_missing_optional_interval_is_retained(
    spark,
    source_schema,
    valid_row,
):
    row = valid_row.copy()
    row["resistant_time_to_culturetime"] = None

    source = make_source(spark, source_schema, [row])

    silver, quarantine, metrics = transform_microbial_resistance(source)

    assert silver.count() == 1
    assert quarantine.count() == 0
    assert silver.first()["resistant_time_to_culturetime"] is None
    assert metrics["invalid_intervals"] == 0
    assert metrics["negative_intervals"] == 0


def test_invalid_non_null_interval_is_retained_and_counted(
    spark,
    source_schema,
    valid_row,
):
    row = valid_row.copy()
    row["resistant_time_to_culturetime"] = "not-a-number"

    source = make_source(spark, source_schema, [row])

    silver, quarantine, metrics = transform_microbial_resistance(source)

    assert silver.count() == 1
    assert quarantine.count() == 0
    assert silver.first()["resistant_time_to_culturetime"] is None
    assert metrics["invalid_intervals"] == 1


def test_negative_interval_is_retained_and_counted(
    spark,
    source_schema,
    valid_row,
):
    row = valid_row.copy()
    row["resistant_time_to_culturetime"] = "-10"

    source = make_source(spark, source_schema, [row])

    silver, quarantine, metrics = transform_microbial_resistance(source)

    assert silver.count() == 1
    assert quarantine.count() == 0
    assert silver.first()["resistant_time_to_culturetime"] == -10
    assert metrics["negative_intervals"] == 1


def test_zero_interval_is_valid(
    spark,
    source_schema,
    valid_row,
):
    row = valid_row.copy()
    row["resistant_time_to_culturetime"] = "0"

    source = make_source(spark, source_schema, [row])

    silver, quarantine, metrics = transform_microbial_resistance(source)

    assert silver.count() == 1
    assert quarantine.count() == 0
    assert silver.first()["resistant_time_to_culturetime"] == 0
    assert metrics["invalid_intervals"] == 0
    assert metrics["negative_intervals"] == 0


def test_repeated_observations_with_different_intervals_are_retained(
    spark,
    source_schema,
    valid_row,
):
    first = valid_row.copy()
    second = valid_row.copy()
    first["resistant_time_to_culturetime"] = "91"
    second["resistant_time_to_culturetime"] = "115"

    source = make_source(spark, source_schema, [first, second])

    silver, quarantine, metrics = transform_microbial_resistance(source)

    assert silver.count() == 2
    assert quarantine.count() == 0

    intervals = {
        row["resistant_time_to_culturetime"]
        for row in silver.collect()
    }
    assert intervals == {91, 115}
    assert metrics["output_rows"] == 2


def test_identical_records_are_not_deduplicated(
    spark,
    source_schema,
    valid_row,
):
    source = make_source(
        spark,
        source_schema,
        [valid_row, valid_row.copy()],
    )

    silver, quarantine, metrics = transform_microbial_resistance(source)

    assert silver.count() == 2
    assert quarantine.count() == 0
    assert metrics["input_rows"] == 2
    assert metrics["output_rows"] == 2


def test_row_reconciliation_with_valid_and_invalid_records(
    spark,
    source_schema,
    valid_row,
):
    valid = valid_row.copy()
    invalid = valid_row.copy()
    invalid["order_proc_id_coded"] = "invalid"

    source = make_source(spark, source_schema, [valid, invalid])

    silver, quarantine, metrics = transform_microbial_resistance(source)

    assert silver.count() == 1
    assert quarantine.count() == 1
    assert metrics["input_rows"] == (
        metrics["output_rows"] + metrics["quarantined_rows"]
    )


def test_missing_required_source_column_raises_value_error(
    spark,
    source_schema,
    valid_row,
):
    incomplete_schema = T.StructType(
        [
            field
            for field in source_schema.fields
            if field.name != "antibiotic"
        ]
    )
    incomplete_row = {
        key: value
        for key, value in valid_row.items()
        if key != "antibiotic"
    }

    source = make_source(spark, incomplete_schema, [incomplete_row])

    with pytest.raises(
        ValueError,
        match="Missing required source columns",
    ):
        transform_microbial_resistance(source)