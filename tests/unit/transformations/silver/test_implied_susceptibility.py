
"""Tests for the Silver implied susceptibility transformation."""

import pytest
from pyspark.sql import types as T

from amr_pipeline.transformations.silver.implied_susceptibility import (
    SOURCE_TO_SILVER,
    transform_implied_susceptibility,
)


SOURCE_SCHEMA = T.StructType(
    [
        T.StructField("anon_id", T.StringType(), True),
        T.StructField("pat_enc_csn_id_coded", T.StringType(), True),
        T.StructField("order_proc_id_coded", T.StringType(), True),
        T.StructField("organism", T.StringType(), True),
        T.StructField("antibiotic", T.StringType(), True),
        T.StructField("susceptibility", T.StringType(), True),
        T.StructField("implied_susceptibility", T.StringType(), True),
    ]
)

EXPECTED_SCHEMA = T.StructType(
    [
        T.StructField("anon_id", T.StringType(), True),
        T.StructField("encounter_id", T.LongType(), True),
        T.StructField("culture_order_id", T.LongType(), True),
        T.StructField("organism", T.StringType(), True),
        T.StructField("antibiotic", T.StringType(), True),
        T.StructField("susceptibility", T.StringType(), True),
        T.StructField("implied_susceptibility", T.StringType(), True),
    ]
)


def make_source(spark, rows):
    """Create a source DataFrame using the complete source schema."""
    return spark.createDataFrame(rows, schema=SOURCE_SCHEMA)


def valid_row(**overrides):
    """Return a valid source row, applying any requested overrides."""
    row = {
        "anon_id": "patient-001",
        "pat_enc_csn_id_coded": "12345",
        "order_proc_id_coded": "67890",
        "organism": "Escherichia coli",
        "antibiotic": "Ciprofloxacin",
        "susceptibility": "Resistant",
        "implied_susceptibility": "Susceptible",
    }
    row.update(overrides)
    return row


def test_transforms_valid_row_with_expected_schema(spark):
    source = make_source(spark, [valid_row()])

    silver, quarantine, metrics = transform_implied_susceptibility(
        source
    )

    assert silver.schema == EXPECTED_SCHEMA
    assert silver.count() == 1
    assert quarantine.count() == 0

    row = silver.first()
    assert row.anon_id == "patient-001"
    assert row.encounter_id == 12345
    assert row.culture_order_id == 67890
    assert row.organism == "Escherichia coli"
    assert row.antibiotic == "Ciprofloxacin"
    assert row.susceptibility == "Resistant"
    assert row.implied_susceptibility == "Susceptible"

    assert metrics["input_rows"] == 1
    assert metrics["output_rows"] == 1
    assert metrics["quarantined_rows"] == 0


@pytest.mark.parametrize(
    ("field", "expected_reason"),
    [
        ("anon_id", "missing_anon_id"),
        (
            "pat_enc_csn_id_coded",
            "invalid_or_missing_encounter_id",
        ),
        (
            "order_proc_id_coded",
            "invalid_or_missing_culture_order_id",
        ),
        ("organism", "missing_organism"),
        ("antibiotic", "missing_antibiotic"),
    ],
)
@pytest.mark.parametrize("bad_value", [None, "Null"])
def test_required_missing_values_are_quarantined(
    spark,
    field,
    expected_reason,
    bad_value,
):
    source = make_source(spark, [valid_row(**{field: bad_value})])

    silver, quarantine, metrics = transform_implied_susceptibility(
        source
    )

    assert silver.count() == 0
    assert quarantine.count() == 1

    rejected = quarantine.first()
    assert expected_reason in rejected.rejection_reasons
    assert metrics["input_rows"] == 1
    assert metrics["output_rows"] == 0
    assert metrics["quarantined_rows"] == 1


def test_invalid_numeric_identifiers_are_quarantined(spark):
    source = make_source(
        spark,
        [
            valid_row(pat_enc_csn_id_coded="not-a-number"),
            valid_row(order_proc_id_coded="12.5"),
        ],
    )

    silver, quarantine, metrics = transform_implied_susceptibility(
        source
    )

    assert silver.count() == 0
    assert quarantine.count() == 2

    reasons = [
        set(row.rejection_reasons)
        for row in quarantine.collect()
    ]
    assert {"invalid_or_missing_encounter_id"} in reasons
    assert {"invalid_or_missing_culture_order_id"} in reasons
    assert metrics["input_rows"] == (
        metrics["output_rows"] + metrics["quarantined_rows"]
    )


def test_quarantine_preserves_original_source_values(spark):
    source = make_source(
        spark,
        [
            valid_row(
                pat_enc_csn_id_coded="invalid-id",
                susceptibility="Null",
                implied_susceptibility="No Interpretation",
            )
        ],
    )

    _, quarantine, _ = transform_implied_susceptibility(source)

    rejected = quarantine.first()
    assert rejected.pat_enc_csn_id_coded == "invalid-id"
    assert rejected.susceptibility == "Null"
    assert rejected.implied_susceptibility == "No Interpretation"
    assert "invalid_or_missing_encounter_id" in (
        rejected.rejection_reasons
    )


def test_exact_null_placeholders_are_normalized_and_counted(spark):
    source = make_source(
        spark,
        [
            valid_row(
                susceptibility="Null",
                implied_susceptibility="Null",
            ),
            valid_row(
                susceptibility="Resistant",
                implied_susceptibility="Null",
            ),
            valid_row(
                susceptibility="Null",
                implied_susceptibility="Susceptible",
            ),
        ],
    )

    silver, quarantine, metrics = transform_implied_susceptibility(
        source
    )

    rows = silver.orderBy("susceptibility", "implied_susceptibility").collect()

    assert quarantine.count() == 0
    assert silver.count() == 3
    assert sum(row.susceptibility is None for row in rows) == 2
    assert sum(row.implied_susceptibility is None for row in rows) == 2

    assert metrics["null_placeholders_converted"] == {
        "susceptibility": 2,
        "implied_susceptibility": 2,
    }
    assert metrics["null_placeholders_converted_total"] == 4


def test_null_interpretations_do_not_cause_quarantine(spark):
    source = make_source(
        spark,
        [
            valid_row(susceptibility=None),
            valid_row(implied_susceptibility=None),
            valid_row(
                susceptibility=None,
                implied_susceptibility=None,
            ),
        ],
    )

    silver, quarantine, metrics = transform_implied_susceptibility(
        source
    )

    assert silver.count() == 3
    assert quarantine.count() == 0
    assert metrics["output_rows"] == 3


@pytest.mark.parametrize(
    "interpretation",
    [
        "Intermediate",
        "No Interpretation",
        "See Comment",
        "Not done",
        "Susceptible",
        "Resistant",
    ],
)
def test_interpretation_categories_are_preserved(spark, interpretation):
    source = make_source(
        spark,
        [
            valid_row(
                susceptibility=interpretation,
                implied_susceptibility=interpretation,
            )
        ],
    )

    silver, _, _ = transform_implied_susceptibility(source)

    row = silver.first()
    assert row.susceptibility == interpretation
    assert row.implied_susceptibility == interpretation


def test_susceptibility_fields_are_preserved_independently(spark):
    source = make_source(
        spark,
        [
            valid_row(
                susceptibility="Resistant",
                implied_susceptibility="Susceptible",
            )
        ],
    )

    silver, _, _ = transform_implied_susceptibility(source)

    row = silver.first()
    assert row.susceptibility == "Resistant"
    assert row.implied_susceptibility == "Susceptible"


def test_conflicting_observations_are_retained(spark):
    source = make_source(
        spark,
        [
            valid_row(susceptibility="Resistant"),
            valid_row(susceptibility="Susceptible"),
        ],
    )

    silver, quarantine, metrics = transform_implied_susceptibility(
        source
    )

    assert silver.count() == 2
    assert quarantine.count() == 0
    assert {row.susceptibility for row in silver.collect()} == {
        "Resistant",
        "Susceptible",
    }
    assert metrics["output_rows"] == 2


def test_identical_rows_are_not_deduplicated(spark):
    row = valid_row()
    source = make_source(spark, [row, row])

    silver, quarantine, metrics = transform_implied_susceptibility(
        source
    )

    assert silver.count() == 2
    assert quarantine.count() == 0
    assert metrics["input_rows"] == 2
    assert metrics["output_rows"] == 2


def test_missing_source_column_raises_value_error(spark):
    source = spark.createDataFrame(
        [("patient-001",)],
        schema=T.StructType(
            [T.StructField("anon_id", T.StringType(), True)]
        ),
    )

    with pytest.raises(ValueError, match="Missing required source columns"):
        transform_implied_susceptibility(source)


def test_row_reconciliation_holds_for_valid_and_quarantined_rows(spark):
    source = make_source(
        spark,
        [
            valid_row(),
            valid_row(organism=None),
            valid_row(order_proc_id_coded="bad-id"),
            valid_row(susceptibility=None),
        ],
    )

    silver, quarantine, metrics = transform_implied_susceptibility(
        source
    )

    assert silver.count() == 2
    assert quarantine.count() == 2
    assert metrics["input_rows"] == 4
    assert metrics["output_rows"] == 2
    assert metrics["quarantined_rows"] == 2
    assert metrics["input_rows"] == (
        metrics["output_rows"] + metrics["quarantined_rows"]
    )


def test_source_mapping_contains_expected_fields():
    assert SOURCE_TO_SILVER == {
        "anon_id": "anon_id",
        "pat_enc_csn_id_coded": "encounter_id",
        "order_proc_id_coded": "culture_order_id",
        "organism": "organism",
        "antibiotic": "antibiotic",
        "susceptibility": "susceptibility",
        "implied_susceptibility": "implied_susceptibility",
    }
