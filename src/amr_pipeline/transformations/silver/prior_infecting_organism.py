"""Silver transformation for the microbiology prior infecting organisms."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ssXXX"

SOURCE_TO_SILVER = {
    "anon_id": "anon_id",
    "pat_enc_csn_id_coded": "encounter_id",
    "order_proc_id_coded": "culture_order_id",
    "order_time_jittered_utc": "culture_time",
    "prior_organism": "prior_organism",
    "prior_infecting_organism_days_to_culutre": ("days_since_prior_infection"),
}

REQUIRED_COLUMNS = {
    "anon_id",
    "pat_enc_csn_id_coded",
    "order_proc_id_coded",
    "order_time_jittered_utc",
    "prior_organism",
}

DERIVED_COLUMNS = {
    "pat_enc_csn_id_coded": "_encounter_id",
    "order_proc_id_coded": "_culture_order_id",
    "order_time_jittered_utc": "_culture_time",
    "prior_infecting_organism_days_to_culutre": ("_days_since_prior_infection"),
}


def transform_prior_infecting_organisms(
    source: DataFrame,
) -> tuple[DataFrame, DataFrame, dict[str, int]]:
    """Transform prior infecting organism records into Silver format.

    Returns:
        A tuple containing:
            - Silver DataFrame.
            - Quarantine DataFrame.
            - Transformation metrics.
    """
    missing_columns = REQUIRED_COLUMNS - set(source.columns)

    if missing_columns:
        raise ValueError(f"Missing required source columns: {sorted(missing_columns)}")

    interval_col = "prior_infecting_organism_days_to_culutre"

    # Add a temporary identifier to track individual source rows.
    source = source.withColumn(
        "_source_row_id",
        F.monotonically_increasing_id(),
    )

    source_columns = [column for column in source.columns if column != "_source_row_id"]

    # Count exact "Null" placeholders before normalization.
    placeholder_metrics = [
        F.sum(F.when(F.col(column) == "Null", 1).otherwise(0)).alias(column)
        for column in source_columns
    ]

    placeholder_counts = source.agg(*placeholder_metrics).first().asDict()

    null_placeholders_converted = {
        column: int(placeholder_counts[column] or 0) for column in source_columns
    }

    null_placeholders_converted_total = sum(null_placeholders_converted.values())

    # Preserve original values for quarantine.
    raw_source = source

    # Normalize exact "Null" placeholders to SQL NULL.
    normalized = source

    for column in source_columns:
        normalized = normalized.withColumn(
            column,
            F.when(
                F.col(column) == "Null",
                F.lit(None),
            ).otherwise(F.col(column)),
        )

    # Cast identifiers, timestamp, and interval.
    typed = (
        normalized.withColumn(
            "_encounter_id",
            F.expr("try_cast(`pat_enc_csn_id_coded` AS BIGINT)"),
        )
        .withColumn(
            "_culture_order_id",
            F.expr("try_cast(`order_proc_id_coded` AS BIGINT)"),
        )
        .withColumn(
            "_culture_time",
            F.try_to_timestamp(
                F.col("order_time_jittered_utc"),
                F.lit(TIMESTAMP_FORMAT),
            ),
        )
        .withColumn(
            "_days_since_prior_infection",
            F.expr(f"try_cast(`{interval_col}` AS INT)"),
        )
    )

    # Identify records that fail required-field validation.
    rejection_reasons = F.filter(
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
            F.when(
                F.col("_culture_time").isNull(),
                F.lit("invalid_or_missing_culture_time"),
            ),
            F.when(
                F.col("prior_organism").isNull(),
                F.lit("missing_prior_organism"),
            ),
        ),
        lambda reason: reason.isNotNull(),
    )

    validated = typed.withColumn(
        "rejection_reasons",
        rejection_reasons,
    )

    valid_condition = F.size("rejection_reasons") == 0

    valid_records = validated.filter(valid_condition)
    rejected_records = validated.filter(~valid_condition)

    # Build the Silver dataset.
    silver = valid_records.select(
        *[
            (
                F.col(DERIVED_COLUMNS[column])
                if column in DERIVED_COLUMNS
                else F.col(column)
            ).alias(silver_column)
            for column, silver_column in SOURCE_TO_SILVER.items()
        ]
    )

    # Retrieve original source values for rejected rows.
    quarantine = raw_source.join(
        rejected_records.select(
            "_source_row_id",
            "rejection_reasons",
        ),
        on="_source_row_id",
        how="inner",
    ).select(
        *[F.col(column) for column in source_columns],
        "rejection_reasons",
    )

    # Calculate interval quality metrics.
    invalid_interval_condition = (
        F.col(interval_col).isNotNull() & F.col("_days_since_prior_infection").isNull()
    )

    negative_interval_condition = F.col("_days_since_prior_infection") < 0

    metrics_row = validated.agg(
        F.count("*").alias("input_rows"),
        F.sum(F.when(valid_condition, 1).otherwise(0)).alias("output_rows"),
        F.sum(F.when(~valid_condition, 1).otherwise(0)).alias("quarantined_rows"),
        F.sum(F.when(invalid_interval_condition, 1).otherwise(0)).alias(
            "invalid_intervals"
        ),
        F.sum(F.when(negative_interval_condition, 1).otherwise(0)).alias(
            "negative_intervals"
        ),
    ).first()

    metrics = {
        "input_rows": int(metrics_row["input_rows"] or 0),
        "output_rows": int(metrics_row["output_rows"] or 0),
        "quarantined_rows": int(metrics_row["quarantined_rows"] or 0),
        "invalid_intervals": int(metrics_row["invalid_intervals"] or 0),
        "negative_intervals": int(metrics_row["negative_intervals"] or 0),
        "null_placeholders_converted_total": (null_placeholders_converted_total),
        "null_placeholders_converted": (null_placeholders_converted),
    }

    return silver, quarantine, metrics
