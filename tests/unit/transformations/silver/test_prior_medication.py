"""Tests for the Silver prior medication transformation."""

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from amr_pipeline.transformations.silver.prior_medication import transform_prior_medication


SCHEMA = T.StructType(
    [
        T.StructField("anon_id", T.StringType(), True),
        T.StructField("pat_enc_csn_id_coded", T.StringType(), True),
        T.StructField("order_proc_id_coded", T.StringType(), True),
        T.StructField("order_time_jittered_utc", T.StringType(), True),
        T.StructField("medication_name", T.StringType(), True),
        T.StructField("medication_time_to_culturetime", T.StringType(), True),
        T.StructField("medication_category", T.StringType(), True),
    ]
)


import pytest


@pytest.fixture
def prior_medication_rows():
    return [
        # Valid baseline record
        (
            "100001",
            "200001",
            "300001",
            "2024-01-15 10:30:00+00:00",
            "Ciprofloxacin",
            "5.5",
            "Antibiotic",
        ),
        # Same identifiers, different medication
        (
            "100001",
            "200001",
            "300001",
            "2024-01-15 10:30:00+00:00",
            "Amoxicillin",
            "3.0",
            "Antibiotic",
        ),
        # Exact duplicate of the first record
        (
            "100001",
            "200001",
            "300001",
            "2024-01-15 10:30:00+00:00",
            "Ciprofloxacin",
            "5.5",
            "Antibiotic",
        ),
        # Missing optional medication name
        (
            "100002",
            "200002",
            "300002",
            "2024-02-10 08:15:00+00:00",
            None,
            "2.0",
            "Antibiotic",
        ),
        # Missing optional medication category
        (
            "100003",
            "200003",
            "300003",
            "2024-03-20 14:00:00+00:00",
            "Azithromycin",
            "7.0",
            None,
        ),
        # Missing optional interval
        (
            "100004",
            "200004",
            "300004",
            "2024-04-05 09:00:00+00:00",
            "Doxycycline",
            None,
            "Antibiotic",
        ),
        # Negative interval
        (
            "100005",
            "200005",
            "300005",
            "2024-05-12 11:45:00+00:00",
            "Metronidazole",
            "-2.5",
            "Antibiotic",
        ),
        # Invalid interval
        (
            "100006",
            "200006",
            "300006",
            "2024-06-18 16:20:00+00:00",
            "Ceftriaxone",
            "invalid",
            "Antibiotic",
        ),
        # Missing required patient identifier
        (
            None,
            "200007",
            "300007",
            "2024-07-01 12:00:00+00:00",
            "Gentamicin",
            "4.0",
            "Antibiotic",
        ),
        # Invalid required encounter identifier
        (
            "100008",
            "invalid",
            "300008",
            "2024-08-09 13:30:00+00:00",
            "Vancomycin",
            "1.5",
            "Antibiotic",
        ),
        # Missing required culture order identifier
        (
            "100009",
            "200009",
            None,
            "2024-09-14 07:45:00+00:00",
            "Meropenem",
            "6.0",
            "Antibiotic",
        ),
    ]



def test_exact_duplicate_rows_return_one_copy(spark, prior_medication_rows):
    """Test that exact duplicate rows are reduced to one copy."""
    source = spark.createDataFrame(prior_medication_rows, schema=SCHEMA)

    silver, _, metrics = transform_prior_medication(source)

    copies = silver.filter(
        (F.col("anon_id") == "100001")
        & (F.col("encounter_id") == 200001)
        & (F.col("culture_order_id") == 300001)
        & (F.col("medication") == "Ciprofloxacin")
        & (F.col("days_since_prior_medication") == 5.5)
        & (F.col("med_category") == "Antibiotic")
    )

    assert copies.count() == 1
    assert metrics["input_rows"] == 11
    assert metrics["duplicate_rows_removed"] == 1
    assert metrics["output_rows"] == 7




def test_same_identifiers_different_medication_retain_both(spark, prior_medication_rows):
    """Test that records with identical identifiers but different medications remain distinct."""
    source = spark.createDataFrame(prior_medication_rows, schema=SCHEMA)

    silver, _, metrics = transform_prior_medication(source)

    copies = silver.filter(
        (F.col("anon_id") == "100001")
        & (F.col("encounter_id") == 200001)
        & (F.col("culture_order_id") == 300001)
    )

    assert copies.count() == 2
    assert copies.filter(F.col("medication") == "Ciprofloxacin").count() == 1
    assert copies.filter(F.col("medication") == "Amoxicillin").count() == 1



def test_invalid_required_fields_are_quarantined(spark, prior_medication_rows):
    """Test that records with invalid required identifiers are quarantined."""
    source = spark.createDataFrame(prior_medication_rows, schema=SCHEMA)

    _, quarantine, metrics = transform_prior_medication(source)

    assert metrics["quarantined_rows"] == 3

    reasons = {
        reason
        for row in quarantine.select("rejection_reasons").collect()
        for reason in row["rejection_reasons"]
    }

    assert reasons == {
        "missing_anon_id",
        "invalid_or_missing_encounter_id",
        "invalid_or_missing_culture_order_id",
    }



def test_missing_optional_fields_are_retained(spark, prior_medication_rows):
    """Test that missing optional medication fields do not cause quarantine."""
    source = spark.createDataFrame(prior_medication_rows, schema=SCHEMA)

    silver, quarantine, _ = transform_prior_medication(source)

    # The three records with missing optional fields should remain in Silver.
    assert silver.filter(F.col("anon_id") == "100002").count() == 1
    assert silver.filter(F.col("anon_id") == "100003").count() == 1
    assert silver.filter(F.col("anon_id") == "100004").count() == 1

    # None of these records should be quarantined.
    assert quarantine.filter(
        F.col("anon_id").isin("100002", "100003", "100004")
    ).count() == 0


def test_null_placeholders_are_converted_to_sql_null(spark, prior_medication_rows):
    """Test that exact 'Null' placeholders are converted to SQL NULL."""
    rows = prior_medication_rows + [
        (
            "100010",
            "200010",
            "300010",
            "2024-10-01 10:00:00+00:00",
            "Null",
            "Null",
            "Null",
        )
    ]

    source = spark.createDataFrame(rows, schema=SCHEMA)

    silver, _, metrics = transform_prior_medication(source)

    record = silver.filter(F.col("anon_id") == "100010").first()

    assert record is not None
    assert record["medication"] is None
    assert record["days_since_prior_medication"] is None
    assert record["med_category"] is None

    assert metrics["null_placeholders_converted.medication_name"] == 1
    assert metrics[
        "null_placeholders_converted.medication_time_to_culturetime"
    ] == 1
    assert metrics["null_placeholders_converted.medication_category"] == 1


def test_medication_interval_is_parsed(spark, prior_medication_rows):
    """Test that valid medication intervals are converted to numeric values."""
    source = spark.createDataFrame(prior_medication_rows, schema=SCHEMA)

    silver, _, _ = transform_prior_medication(source)

    record = silver.filter(
        (F.col("anon_id") == "100001")
        & (F.col("medication") == "Ciprofloxacin")
    ).first()

    assert record["days_since_prior_medication"] == pytest.approx(5.5)
    assert isinstance(record["days_since_prior_medication"], float)


def test_negative_medication_intervals_are_retained(spark, prior_medication_rows):
    """Test that negative intervals are retained and counted as DQ anomalies."""
    source = spark.createDataFrame(prior_medication_rows, schema=SCHEMA)

    silver, quarantine, metrics = transform_prior_medication(source)

    record = silver.filter(F.col("anon_id") == "100005").first()

    assert record is not None
    assert record["days_since_prior_medication"] == pytest.approx(-2.5)

    assert quarantine.filter(F.col("anon_id") == "100005").count() == 0
    assert metrics["negative_medication_intervals"] == 1


def test_invalid_medication_intervals_are_handled(spark, prior_medication_rows):
    """Test that malformed, non-null intervals become SQL NULL and remain in Silver."""
    source = spark.createDataFrame(prior_medication_rows, schema=SCHEMA)

    silver, quarantine, metrics = transform_prior_medication(source)

    record = silver.filter(F.col("anon_id") == "100006").first()

    assert record is not None
    assert record["days_since_prior_medication"] is None

    assert quarantine.filter(F.col("anon_id") == "100006").count() == 0
    assert metrics["invalid_medication_intervals"] == 1


def test_row_reconciliation_metrics(spark, prior_medication_rows):
    """Test that input rows reconcile with duplicates, Silver, and quarantine."""
    source = spark.createDataFrame(prior_medication_rows, schema=SCHEMA)

    silver, quarantine, metrics = transform_prior_medication(source)

    assert metrics["input_rows"] == 11
    assert metrics["deduplicated_rows"] == 10
    assert metrics["duplicate_rows_removed"] == 1

    assert metrics["output_rows"] == silver.count() == 7
    assert metrics["quarantined_rows"] == quarantine.count() == 3

    assert (
        metrics["input_rows"]
        == metrics["duplicate_rows_removed"]
        + metrics["output_rows"]
        + metrics["quarantined_rows"]
    )


