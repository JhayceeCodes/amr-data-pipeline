import pytest
from pyspark.sql import types as T

from amr_pipeline.transformations.silver.antibiotic_subtype_exposure import (
    transform_antibiotic_subtype_exposure,
)

SCHEMA = T.StructType(
    [
        T.StructField("anon_id", T.StringType(), True),
        T.StructField("pat_enc_csn_id_coded", T.StringType(), True),
        T.StructField("order_proc_id_coded", T.StringType(), True),
        T.StructField("order_time_jittered_utc", T.StringType(), True),
        T.StructField("medication_category", T.StringType(), True),
        T.StructField("medication_name", T.StringType(), True),
        T.StructField("antibiotic_subtype", T.StringType(), True),
        T.StructField("antibiotic_subtype_category", T.StringType(), True),
        T.StructField("medication_time_to_cultureTime", T.StringType(), True),
    ]
)


def make_record(**overrides):
    record = {
        "anon_id": "JC661393",
        "pat_enc_csn_id_coded": "131323239770",
        "order_proc_id_coded": "764294462",
        "order_time_jittered_utc": "2021-12-11 00:00:00+00:00",
        "medication_category": "RIF2",
        "medication_name": "Rifabutin",
        "antibiotic_subtype": "Ansamycin",
        "antibiotic_subtype_category": "ANS",
        "medication_time_to_cultureTime": "31",
    }
    record.update(overrides)
    return record


def transform(spark, records):
    source = spark.createDataFrame(records, schema=SCHEMA)
    return transform_antibiotic_subtype_exposure(source)


def test_transforms_records_to_expected_schema(spark):
    silver, quarantine, metrics = transform(spark, [make_record()])

    assert silver.columns == [
        "anon_id",
        "encounter_id",
        "culture_order_id",
        "culture_time",
        "med_category",
        "medication",
        "subtype",
        "subtype_category",
        "days_since_prior_medication",
    ]

    fields = {field.name: field.dataType for field in silver.schema.fields}

    assert isinstance(fields["anon_id"], T.StringType)
    assert isinstance(fields["encounter_id"], T.LongType)
    assert isinstance(fields["culture_order_id"], T.LongType)
    assert isinstance(fields["culture_time"], T.TimestampType)
    assert isinstance(fields["med_category"], T.StringType)
    assert isinstance(fields["medication"], T.StringType)
    assert isinstance(fields["subtype"], T.StringType)
    assert isinstance(fields["subtype_category"], T.StringType)
    assert isinstance(fields["days_since_prior_medication"], T.IntegerType)

    assert silver.count() == 1
    assert quarantine.count() == 0
    assert metrics["input_rows"] == 1
    assert metrics["output_rows"] == 1
    assert metrics["quarantined_rows"] == 0


def test_casts_identifiers_timestamp_and_interval(spark):
    silver, _, _ = transform(spark, [make_record()])

    row = silver.first()

    assert row.anon_id == "JC661393"
    assert row.encounter_id == 131323239770
    assert row.culture_order_id == 764294462
    assert row.culture_time is not None
    assert row.days_since_prior_medication == 31


def test_preserves_repeated_exposure_records_with_different_intervals(spark):
    records = [
        make_record(medication_time_to_cultureTime="43"),
        make_record(medication_time_to_cultureTime="48"),
    ]

    silver, quarantine, metrics = transform(spark, records)

    assert silver.count() == 2
    assert quarantine.count() == 0
    assert {row.days_since_prior_medication for row in silver.collect()} == {43, 48}
    assert metrics["input_rows"] == 2
    assert metrics["output_rows"] == 2


def test_preserves_records_with_different_subtype_categories(spark):
    records = [
        make_record(
            antibiotic_subtype="Cephalosporin",
            antibiotic_subtype_category="CEP13",
        ),
        make_record(
            antibiotic_subtype="Cephalosporin",
            antibiotic_subtype_category="CEP12",
        ),
    ]

    silver, quarantine, _ = transform(spark, records)

    assert silver.count() == 2
    assert quarantine.count() == 0
    assert {row.subtype_category for row in silver.collect()} == {"CEP12", "CEP13"}


def test_does_not_deduplicate_identical_source_records(spark):
    record = make_record()

    silver, quarantine, metrics = transform(spark, [record, record])

    assert silver.count() == 2
    assert quarantine.count() == 0
    assert metrics["input_rows"] == 2
    assert metrics["output_rows"] == 2


@pytest.mark.parametrize(
    ("column", "value", "expected_reason"),
    [
        ("anon_id", None, "missing_anon_id"),
        ("anon_id", "Null", "missing_anon_id"),
        (
            "pat_enc_csn_id_coded",
            "not-a-number",
            "invalid_or_missing_encounter_id",
        ),
        (
            "pat_enc_csn_id_coded",
            None,
            "invalid_or_missing_encounter_id",
        ),
        (
            "order_proc_id_coded",
            "not-a-number",
            "invalid_or_missing_culture_order_id",
        ),
        (
            "order_proc_id_coded",
            None,
            "invalid_or_missing_culture_order_id",
        ),
        (
            "order_time_jittered_utc",
            "not-a-timestamp",
            "invalid_or_missing_culture_time",
        ),
        (
            "order_time_jittered_utc",
            None,
            "invalid_or_missing_culture_time",
        ),
    ],
)
def test_quarantines_invalid_required_fields(
    spark,
    column,
    value,
    expected_reason,
):
    silver, quarantine, metrics = transform(
        spark,
        [make_record(**{column: value})],
    )

    assert silver.count() == 0
    assert quarantine.count() == 1
    assert expected_reason in quarantine.first().rejection_reasons
    assert metrics["quarantined_rows"] == 1


def test_quarantine_preserves_raw_values(spark):
    silver, quarantine, _ = transform(
        spark,
        [
            make_record(
                pat_enc_csn_id_coded="invalid-id",
                medication_time_to_cultureTime="15",
            )
        ],
    )

    row = quarantine.first()

    assert silver.count() == 0
    assert row.pat_enc_csn_id_coded == "invalid-id"
    assert row.medication_time_to_cultureTime == "15"
    assert "invalid_or_missing_encounter_id" in row.rejection_reasons


def test_collects_multiple_rejection_reasons(spark):
    silver, quarantine, _ = transform(
        spark,
        [
            make_record(
                anon_id="Null",
                pat_enc_csn_id_coded="invalid-id",
                order_proc_id_coded="invalid-order",
                order_time_jittered_utc="invalid-time",
            )
        ],
    )

    reasons = set(quarantine.first().rejection_reasons)

    assert silver.count() == 0
    assert reasons == {
        "missing_anon_id",
        "invalid_or_missing_encounter_id",
        "invalid_or_missing_culture_order_id",
        "invalid_or_missing_culture_time",
    }


def test_converts_exact_null_placeholders_to_sql_null(spark):
    silver, quarantine, metrics = transform(
        spark,
        [
            make_record(
                medication_category="Null",
                medication_name="Null",
                antibiotic_subtype="Null",
                antibiotic_subtype_category="Null",
            )
        ],
    )

    row = silver.first()

    assert quarantine.count() == 0
    assert row.med_category is None
    assert row.medication is None
    assert row.subtype is None
    assert row.subtype_category is None

    assert metrics["null_placeholders_converted.medication_category"] == 1
    assert metrics["null_placeholders_converted.medication_name"] == 1
    assert metrics["null_placeholders_converted.antibiotic_subtype"] == 1
    assert metrics["null_placeholders_converted.antibiotic_subtype_category"] == 1


def test_preserves_optional_missing_values(spark):
    silver, quarantine, _ = transform(
        spark,
        [
            make_record(
                medication_category=None,
                medication_name=None,
                antibiotic_subtype=None,
                antibiotic_subtype_category=None,
                medication_time_to_cultureTime=None,
            )
        ],
    )

    row = silver.first()

    assert silver.count() == 1
    assert quarantine.count() == 0
    assert row.med_category is None
    assert row.medication is None
    assert row.subtype is None
    assert row.subtype_category is None
    assert row.days_since_prior_medication is None


def test_invalid_interval_is_retained_and_counted(spark):
    silver, quarantine, metrics = transform(
        spark,
        [make_record(medication_time_to_cultureTime="not-an-integer")],
    )

    assert silver.count() == 1
    assert quarantine.count() == 0
    assert silver.first().days_since_prior_medication is None
    assert metrics["invalid_intervals"] == 1


def test_negative_interval_is_retained_and_counted(spark):
    silver, quarantine, metrics = transform(
        spark,
        [make_record(medication_time_to_cultureTime="-5")],
    )

    assert silver.count() == 1
    assert quarantine.count() == 0
    assert silver.first().days_since_prior_medication == -5
    assert metrics["negative_intervals"] == 1


def test_zero_interval_is_valid(spark):
    silver, quarantine, metrics = transform(
        spark,
        [make_record(medication_time_to_cultureTime="0")],
    )

    assert silver.count() == 1
    assert quarantine.count() == 0
    assert silver.first().days_since_prior_medication == 0
    assert metrics["invalid_intervals"] == 0
    assert metrics["negative_intervals"] == 0


def test_rejects_missing_required_source_column(spark):
    source = spark.createDataFrame(
        [
            {
                key: value
                for key, value in make_record().items()
                if key != "antibiotic_subtype"
            }
        ]
    )

    with pytest.raises(
        ValueError,
        match="missing required columns",
    ):
        transform_antibiotic_subtype_exposure(source)


def test_metrics_reconcile_input_output_and_quarantine(spark):
    records = [
        make_record(),
        make_record(order_proc_id_coded="invalid-order"),
        make_record(medication_time_to_cultureTime="bad-interval"),
        make_record(medication_time_to_cultureTime="-2"),
    ]

    silver, quarantine, metrics = transform(spark, records)

    assert metrics["input_rows"] == 4
    assert metrics["output_rows"] == silver.count() == 3
    assert metrics["quarantined_rows"] == quarantine.count() == 1
    assert metrics["input_rows"] == (
        metrics["output_rows"] + metrics["quarantined_rows"]
    )
    assert metrics["invalid_intervals"] == 1
    assert metrics["negative_intervals"] == 1
