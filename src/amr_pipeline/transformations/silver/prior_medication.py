"""Silver transformation for prior medication records."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


SOURCE_TO_SILVER = {
    "anon_id": "anon_id",
    "pat_enc_csn_id_coded": "encounter_id",
    "order_proc_id_coded": "culture_order_id",
    "medication_name": "medication",
    "medication_time_to_culturetime": "days_since_prior_medication",
    "medication_category": "med_category",
}

REQUIRED_COLUMNS = {
    "anon_id",
    "pat_enc_csn_id_coded",
    "order_proc_id_coded",
}

# Source fields that require parsing or type conversion.
DERIVED_COLUMNS = {
    "pat_enc_csn_id_coded": "_encounter_id",
    "order_proc_id_coded": "_culture_order_id",
    "medication_time_to_culturetime": "_days_since_prior_medication",
}

# Explicit output types for fields that need casting.
COLUMN_CASTS = {
    "anon_id": "string",
}


def transform_prior_medication(
    source: DataFrame,
) -> tuple[DataFrame, DataFrame, dict[str, int]]:
    """Transform Bronze prior medication records into Silver and quarantine."""

    # Validate the source schema.
    missing_columns = REQUIRED_COLUMNS - set(source.columns)

    if missing_columns:
        raise ValueError(
            "Bronze prior medication is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    # Validate that every mapped source column exists.
    mapped_columns = set(SOURCE_TO_SILVER)
    missing_mapped_columns = mapped_columns - set(source.columns)

    if missing_mapped_columns:
        raise ValueError(
            "Bronze prior medication is missing mapped columns: "
            f"{sorted(missing_mapped_columns)}"
        )

    # Capture the original input count.
    input_rows = source.count()

    # Remove exact duplicate source records.
    # All source columns participate in the equality comparison.
    deduplicated = source.dropDuplicates(source.columns)

    deduplicated_rows = deduplicated.count()
    duplicate_rows_removed = input_rows - deduplicated_rows

    # Capture exact placeholder counts after deduplication.
    placeholder_metrics = {
        column: deduplicated.filter(F.col(column) == "Null").count()
        for column in deduplicated.columns
    }

    placeholder_metrics = {
        column: count
        for column, count in placeholder_metrics.items()
        if count > 0
    }

    # Retain original source values for rejected records.
    cleaned = deduplicated

    for column in deduplicated.columns:
        cleaned = cleaned.withColumn(
            f"_raw_{column}",
            F.col(column),
        )

    # Convert exact "Null" placeholders to SQL NULL.
    for column in deduplicated.columns:
        cleaned = cleaned.withColumn(
            column,
            F.when(
                F.col(column) == "Null",
                F.lit(None),
            ).otherwise(F.col(column)),
        )

    # Parse required identifiers.
    cleaned = (
        cleaned.withColumn(
            DERIVED_COLUMNS["pat_enc_csn_id_coded"],
            F.expr("try_cast(pat_enc_csn_id_coded AS BIGINT)"),
        )
        .withColumn(
            DERIVED_COLUMNS["order_proc_id_coded"],
            F.expr("try_cast(order_proc_id_coded AS BIGINT)"),
        )
        .withColumn(
            DERIVED_COLUMNS["medication_time_to_culturetime"],
            F.expr(
                "try_cast(medication_time_to_culturetime AS DOUBLE)"
            ),
        )
    )

    # Identify malformed medication intervals.
    # The interval is not a required field, so malformed values
    # are recorded as DQ metrics rather than quarantined.
    invalid_interval = (
        F.col("medication_time_to_culturetime").isNotNull()
        & F.col("_days_since_prior_medication").isNull()
    )

    invalid_interval_count = cleaned.filter(invalid_interval).count()

    # Flag negative intervals without changing or rejecting them.
    negative_interval_count = cleaned.filter(
        F.col("_days_since_prior_medication") < 0
    ).count()

    # Build rejection reasons for invalid required fields.
    rejection_reasons = F.array_compact(
        F.array(
            F.when(
                F.col("anon_id").isNull(),
                F.lit("missing_anon_id"),
            ),
            F.when(
                F.col("_encounter_id").isNull(),
                F.lit("invalid_or_missing_encounter_id"),
            ),
            F.when(
                F.col("_culture_order_id").isNull(),
                F.lit("invalid_or_missing_culture_order_id"),
            ),
        )
    )

    assessed = cleaned.withColumn(
        "rejection_reasons",
        rejection_reasons,
    )

    # Preserve original source values in quarantine.
    quarantine = (
        assessed.filter(F.size(F.col("rejection_reasons")) > 0)
        .select(
            *[
                F.col(f"_raw_{column}").alias(column)
                for column in deduplicated.columns
            ],
            F.col("rejection_reasons"),
        )
    )

    # Dynamically construct the Silver output columns.
    silver_columns = []

    for source_column, silver_column in SOURCE_TO_SILVER.items():
        working_column = DERIVED_COLUMNS.get(
            source_column,
            source_column,
        )

        expression = F.col(working_column)

        if source_column in COLUMN_CASTS:
            expression = expression.cast(COLUMN_CASTS[source_column])

        silver_columns.append(
            expression.alias(silver_column)
        )

    silver = (
        assessed.filter(F.size(F.col("rejection_reasons")) == 0)
        .select(*silver_columns)
    )

    # Reconcile original input, duplicates, Silver, and quarantine.
    output_rows = silver.count()
    quarantined_rows = quarantine.count()

    if input_rows != (
        duplicate_rows_removed + output_rows + quarantined_rows
    ):
        raise RuntimeError(
            "Prior medication row reconciliation failed: "
            f"input={input_rows}, "
            f"duplicates_removed={duplicate_rows_removed}, "
            f"output={output_rows}, "
            f"quarantined={quarantined_rows}"
        )

    # Collect transformation metrics.
    metrics = {
        "input_rows": input_rows,
        "deduplicated_rows": deduplicated_rows,
        "duplicate_rows_removed": duplicate_rows_removed,
        "output_rows": output_rows,
        "quarantined_rows": quarantined_rows,
        "invalid_medication_intervals": invalid_interval_count,
        "negative_medication_intervals": negative_interval_count,
        "null_placeholders_converted_total": sum(
            placeholder_metrics.values()
        ),
        **{
            f"null_placeholders_converted.{column}": count
            for column, count in placeholder_metrics.items()
        },
    }

    return silver, quarantine, metrics