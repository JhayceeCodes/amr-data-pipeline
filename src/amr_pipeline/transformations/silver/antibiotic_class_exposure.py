from pyspark.sql import DataFrame
from pyspark.sql import functions as F


TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ssXXX"

SOURCE_TO_SILVER = {
    "anon_id": "anon_id",
    "pat_enc_csn_id_coded": "encounter_id",
    "order_proc_id_coded": "culture_order_id",
    "order_time_jittered_utc": "culture_time",
    "medication_category": "med_category",
    "medication_name": "medication",
    "antibiotic_class": "antibiotic_class",
    "time_to_culturetime": "days_before_culture",
}

REQUIRED_COLUMNS = {
    "anon_id",
    "pat_enc_csn_id_coded",
    "order_proc_id_coded",
    "order_time_jittered_utc",
    "medication_category",
    "medication_name",
    "antibiotic_class",
    "time_to_culturetime",
}


def transform_antibiotic_class_exposure(
    source: DataFrame,
) -> tuple[DataFrame, DataFrame, dict]:
    """Transform antibiotic class exposure records into Silver.

    Exact 'Null' placeholders become SQL NULL. Required identifier and
    timestamp failures are quarantined. Optional missing values and invalid
    intervals are retained, with invalid intervals represented as NULL.

    Records are not deduplicated because repeated combinations may represent
    distinct exposure intervals.
    """
    missing_columns = REQUIRED_COLUMNS - set(source.columns)
    if missing_columns:
        raise ValueError(
            f"Missing required source columns: {sorted(missing_columns)}"
        )

    input_rows = source.count()

    placeholder_counts = {}
    normalized = source

    for column in source.columns:
        count = source.filter(F.col(column) == "Null").count()
        placeholder_counts[column] = count

        normalized = normalized.withColumn(
            column,
            F.when(F.col(column) == "Null", F.lit(None))
            .otherwise(F.col(column)),
        )

    # Retain original values for quarantine investigation.
    for column in source.columns:
        normalized = normalized.withColumnRenamed(
            column,
            f"_raw_{column}",
        )

    # Build parsed fields without modifying the retained raw values.
    parsed = normalized

    parsed = parsed.withColumn(
        "anon_id",
        F.col("_raw_anon_id"),
    ).withColumn(
        "encounter_id",
        F.expr("try_cast(_raw_pat_enc_csn_id_coded AS BIGINT)"),
    ).withColumn(
        "culture_order_id",
        F.expr("try_cast(_raw_order_proc_id_coded AS BIGINT)"),
    ).withColumn(
        "culture_time",
        F.try_to_timestamp(
            F.col("_raw_order_time_jittered_utc"),
            F.lit(TIMESTAMP_FORMAT),
        ),
    ).withColumn(
        "med_category",
        F.col("_raw_medication_category"),
    ).withColumn(
        "medication",
        F.col("_raw_medication_name"),
    ).withColumn(
        "antibiotic_class",
        F.col("_raw_antibiotic_class"),
    ).withColumn(
        "days_before_culture",
        F.expr("try_cast(_raw_time_to_culturetime AS INT)"),
    )

    # Required-field validation. Optional exposure fields are not required
    # for a record to remain in Silver.
    rejection_conditions = {
        "missing_anon_id": F.col("anon_id").isNull(),
        "invalid_encounter_id": F.col("encounter_id").isNull(),
        "invalid_culture_order_id": F.col("culture_order_id").isNull(),
        "invalid_culture_time": F.col("culture_time").isNull(),
    }

    rejection_reasons = F.filter(
        F.array(
            *[
                F.when(condition, F.lit(reason))
                for reason, condition in rejection_conditions.items()
            ]
        ),
        lambda reason: reason.isNotNull(),
    )

    classified = parsed.withColumn(
        "rejection_reasons",
        rejection_reasons,
    )

    quarantine = classified.filter(
        F.size("rejection_reasons") > 0
    )

    valid = classified.filter(
        F.size("rejection_reasons") == 0
    )

    silver_columns = [
        "anon_id",
        "encounter_id",
        "culture_order_id",
        "culture_time",
        "med_category",
        "medication",
        "antibiotic_class",
        "days_before_culture",
    ]

    silver = valid.select(*silver_columns)

    invalid_intervals = valid.filter(
        F.col("_raw_time_to_culturetime").isNotNull()
        & F.col("days_before_culture").isNull()
    ).count()

    negative_intervals = valid.filter(
        F.col("days_before_culture") < 0
    ).count()

    output_rows = silver.count()
    quarantined_rows = quarantine.count()

    metrics = {
        "input_rows": input_rows,
        "output_rows": output_rows,
        "quarantined_rows": quarantined_rows,
        "invalid_intervals": invalid_intervals,
        "negative_intervals": negative_intervals,
        "null_placeholders_converted_total": sum(
            placeholder_counts.values()
        ),
        "null_placeholders_converted_by_column": placeholder_counts,
    }

    return silver, quarantine, metrics