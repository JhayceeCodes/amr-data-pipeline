"""Tests for the Silver prior procedure transformation."""

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from amr_pipeline.transformations.silver.prior_procedure import (
    transform_prior_procedures,
)

SCHEMA = T.StructType(
    [
        T.StructField("anon_id", T.StringType(), True),
        T.StructField("pat_enc_csn_id_coded", T.StringType(), True),
        T.StructField("order_proc_id_coded", T.StringType(), True),
        T.StructField("order_time_jittered_utc", T.StringType(), True),
        T.StructField("procedure_description", T.StringType(), True),
        T.StructField("procedure_time_to_culturetime", T.StringType(), True),
    ]
)


@pytest.fixture
def prior_procedure_source(spark):
    """Create valid and invalid source records for transformation tests."""
    records = [
        # Same procedure and culture order, distinct intervals: preserve all.
        (
            "JC862464",
            "131326809054",
            "775667819",
            "2022-02-03 22:57:00+00:00",
            "parenteral_nutrition",
            "640",
        ),
        (
            "JC862464",
            "131326809054",
            "775667819",
            "2022-02-03 22:57:00+00:00",
            "parenteral_nutrition",
            "646",
        ),
        (
            "JC862464",
            "131326809054",
            "775667819",
            "2022-02-03 22:57:00+00:00",
            "parenteral_nutrition",
            "634",
        ),
        # Negative interval: retain and track as a data-quality anomaly.
        (
            "JC862464",
            "131326809054",
            "775667819",
            "2022-02-03 22:57:00+00:00",
            "prior_procedure_negative",
            "-828",
        ),
        # Missing patient ID.
        (
            None,
            "131326809054",
            "775667820",
            "2022-02-03 22:57:00+00:00",
            "procedure_missing_patient",
            "100",
        ),
        # Invalid encounter ID.
        (
            "JC862465",
            "invalid",
            "775667821",
            "2022-02-03 22:57:00+00:00",
            "procedure_invalid_encounter",
            "200",
        ),
        # Invalid culture order ID.
        (
            "JC862466",
            "131326809056",
            "invalid",
            "2022-02-03 22:57:00+00:00",
            "procedure_invalid_order",
            "300",
        ),
        # Invalid timestamp.
        (
            "JC862467",
            "131326809057",
            "775667823",
            "invalid-timestamp",
            "procedure_invalid_timestamp",
            None,
        ),
        # Missing required procedure description.
        (
            "JC862468",
            "131326809058",
            "775667824",
            "2022-02-03 22:57:00+00:00",
            None,
            "500",
        ),
        # Malformed optional interval: retain with a null parsed interval.
        (
            "JC862469",
            "131326809059",
            "775667825",
            "2022-02-03 22:57:00+00:00",
            "procedure_invalid_interval",
            "invalid",
        ),
        # Missing optional interval: retain without counting as malformed.
        (
            "JC862470",
            "131326809060",
            "775667826",
            "2022-02-03 22:57:00+00:00",
            "procedure_missing_interval",
            None,
        ),
        # Literal null placeholder in an optional field.
        (
            "JC862471",
            "131326809061",
            "775667827",
            "2022-02-03 22:57:00+00:00",
            "procedure_null_interval",
            "Null",
        ),
        # Literal null placeholder in a required field.
        (
            "JC862472",
            "131326809062",
            "775667828",
            "2022-02-03 22:57:00+00:00",
            "Null",
            "100",
        ),
        # Zero is valid and should not count as a negative interval.
        (
            "JC862473",
            "131326809063",
            "775667829",
            "2022-02-03 22:57:00+00:00",
            "procedure_zero_interval",
            "0",
        ),
        # Two invalid required fields on one row.
        (
            None,
            "invalid",
            "775667830",
            "2022-02-03 22:57:00+00:00",
            "procedure_multiple_errors",
            "100",
        ),
    ]

    return spark.createDataFrame(records, schema=SCHEMA)


def test_transforms_valid_and_invalid_records(prior_procedure_source):
    silver, quarantine, metrics = transform_prior_procedures(prior_procedure_source)

    assert silver.count() == 8
    assert quarantine.count() == 7

    assert metrics["input_rows"] == 15
    assert metrics["output_rows"] == 8
    assert metrics["quarantined_rows"] == 7
    assert metrics["invalid_intervals"] == 1
    assert metrics["negative_intervals"] == 1

    assert metrics["output_rows"] + metrics["quarantined_rows"] == metrics["input_rows"]


def test_output_schema_has_expected_types(prior_procedure_source):
    silver, _, _ = transform_prior_procedures(prior_procedure_source)

    expected_schema = [
        ("anon_id", T.StringType()),
        ("encounter_id", T.LongType()),
        ("culture_order_id", T.LongType()),
        ("culture_time", T.TimestampType()),
        ("procedure_name", T.StringType()),
        ("days_since_prior_procedure", T.IntegerType()),
    ]

    actual_schema = [(field.name, field.dataType) for field in silver.schema.fields]

    assert actual_schema == expected_schema


def test_casts_identifiers_timestamp_and_interval(prior_procedure_source):
    silver, _, _ = transform_prior_procedures(prior_procedure_source)

    record = silver.filter(F.col("days_since_prior_procedure") == 640).first()

    assert record is not None
    assert record["anon_id"] == "JC862464"
    assert record["encounter_id"] == 131326809054
    assert record["culture_order_id"] == 775667819
    assert record["culture_time"] is not None
    assert record["procedure_name"] == "parenteral_nutrition"
    assert record["days_since_prior_procedure"] == 640


def test_preserves_repeated_procedure_observations(prior_procedure_source):
    silver, _, _ = transform_prior_procedures(prior_procedure_source)

    records = (
        silver.filter(
            (F.col("culture_order_id") == 775667819)
            & (F.col("procedure_name") == "parenteral_nutrition")
        )
        .select("days_since_prior_procedure")
        .collect()
    )

    intervals = [row["days_since_prior_procedure"] for row in records]

    assert len(intervals) == 3
    assert sorted(intervals) == [634, 640, 646]


def test_retains_negative_interval_and_tracks_metric(prior_procedure_source):
    silver, _, metrics = transform_prior_procedures(prior_procedure_source)

    record = silver.filter(
        F.col("procedure_name") == "prior_procedure_negative"
    ).first()

    assert record is not None
    assert record["days_since_prior_procedure"] == -828
    assert metrics["negative_intervals"] == 1


def test_retains_invalid_interval_as_null_and_tracks_metric(
    prior_procedure_source,
):
    silver, _, metrics = transform_prior_procedures(prior_procedure_source)

    record = silver.filter(F.col("anon_id") == "JC862469").first()

    assert record is not None
    assert record["days_since_prior_procedure"] is None
    assert metrics["invalid_intervals"] == 1


def test_retains_missing_optional_interval(prior_procedure_source):
    silver, _, metrics = transform_prior_procedures(prior_procedure_source)

    record = silver.filter(F.col("anon_id") == "JC862470").first()

    assert record is not None
    assert record["days_since_prior_procedure"] is None

    # A genuinely missing interval is not a malformed interval.
    assert metrics["invalid_intervals"] == 1


def test_retains_zero_interval_without_negative_metric(prior_procedure_source):
    silver, _, metrics = transform_prior_procedures(prior_procedure_source)

    record = silver.filter(F.col("anon_id") == "JC862473").first()

    assert record is not None
    assert record["days_since_prior_procedure"] == 0
    assert metrics["negative_intervals"] == 1


def test_converts_literal_null_placeholders(prior_procedure_source):
    silver, quarantine, metrics = transform_prior_procedures(prior_procedure_source)

    # Optional interval placeholder becomes SQL NULL; retain the row.
    optional_record = silver.filter(F.col("anon_id") == "JC862471").first()

    assert optional_record is not None
    assert optional_record["days_since_prior_procedure"] is None

    # Required procedure placeholder becomes SQL NULL and is quarantined.
    required_record = quarantine.filter(F.col("anon_id") == "JC862472").first()

    assert required_record is not None
    assert required_record["procedure_description"] == "Null"

    assert metrics["null_placeholders_converted_total"] == 2


def test_quarantines_missing_procedure_name(prior_procedure_source):
    _, quarantine, _ = transform_prior_procedures(prior_procedure_source)

    record = quarantine.filter(F.col("anon_id") == "JC862468").first()

    assert record is not None
    assert "missing_procedure_name" in record["rejection_reasons"]


def test_quarantines_missing_patient_id(prior_procedure_source):
    _, quarantine, _ = transform_prior_procedures(prior_procedure_source)

    record = quarantine.filter(F.col("order_proc_id_coded") == "775667820").first()

    assert record is not None
    assert "missing_anon_id" in record["rejection_reasons"]


def test_quarantines_invalid_encounter_id(prior_procedure_source):
    _, quarantine, _ = transform_prior_procedures(prior_procedure_source)

    record = quarantine.filter(F.col("anon_id") == "JC862465").first()

    assert record is not None
    assert "invalid_or_missing_encounter_id" in record["rejection_reasons"]


def test_quarantines_invalid_culture_order_id(prior_procedure_source):
    _, quarantine, _ = transform_prior_procedures(prior_procedure_source)

    record = quarantine.filter(F.col("anon_id") == "JC862466").first()

    assert record is not None
    assert "invalid_or_missing_culture_order_id" in record["rejection_reasons"]


def test_quarantines_invalid_timestamp(prior_procedure_source):
    _, quarantine, _ = transform_prior_procedures(prior_procedure_source)

    record = quarantine.filter(F.col("anon_id") == "JC862467").first()

    assert record is not None
    assert "invalid_or_missing_culture_time" in record["rejection_reasons"]


def test_records_multiple_rejection_reasons(prior_procedure_source):
    _, quarantine, _ = transform_prior_procedures(prior_procedure_source)

    record = quarantine.filter(F.col("order_proc_id_coded") == "775667830").first()

    assert record is not None
    assert "missing_anon_id" in record["rejection_reasons"]
    assert "invalid_or_missing_encounter_id" in record["rejection_reasons"]


def test_preserves_original_values_in_quarantine(prior_procedure_source):
    _, quarantine, _ = transform_prior_procedures(prior_procedure_source)

    record = quarantine.filter(F.col("anon_id") == "JC862465").first()

    assert record is not None
    assert record["pat_enc_csn_id_coded"] == "invalid"


def test_fails_when_required_source_column_is_missing(prior_procedure_source):
    source_without_procedure = prior_procedure_source.drop("procedure_description")

    with pytest.raises((ValueError, KeyError)):
        transform_prior_procedures(source_without_procedure)
