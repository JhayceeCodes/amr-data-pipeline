
"""Silver transformation for antibiotic subtype exposure records."""

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
    "antibiotic_subtype": "subtype",
    "antibiotic_subtype_category": "subtype_category",
    "medication_time_to_cultureTime": "days_since_prior_medication",
}

REQUIRED_COLUMNS = set(SOURCE_TO_SILVER)

# Required values for a record to enter Silver.
REQUIRED_FIELDS = {
    "anon_id",
    "pat_enc_csn_id_coded",
    "order_proc_id_coded",
    "order_time_jittered_utc",
}


def transform_antibiotic_subtype_exposure(
    source: DataFrame,
) -> tuple[DataFrame, DataFrame, dict[str, int]]:
    """Transform Bronze antibiotic subtype exposures into Silver and quarantine."""

    missing_columns = REQUIRED_COLUMNS - set(source.columns)

    if missing_columns:
        raise ValueError(
            "Bronze antibiotic subtype exposure is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    source_columns = source.columns

    # Count exact placeholders in one aggregation before normalization.
    placeholder_expressions = [
        F.sum(
            F.when(F.col(column) == "Null", 1).otherwise(0)
        ).alias(column)
        for column in source_columns
    ]

    placeholder_row = source.agg(*placeholder_expressions).first()

    placeholder_metrics = {
        column: int(placeholder_row[column] or 0)
        for column in source_columns
        if (placeholder_row[column] or 0) > 0
    }

    # Preserve source values for quarantine before normalizing them.
    cleaned = source

    for column in source_columns:
        cleaned = cleaned.withColumn(
            f"_raw_{column}",
            F.col(column),
        )

    # Normalize exact "Null" placeholders to SQL NULL.
    for column in source_columns:
        cleaned = cleaned.withColumn(
            column,
            F.when(F.col(column) == "Null", F.lit(None)).otherwise(
                F.col(column)
            ),
        )

    # Safely parse identifiers, timestamp, and interval.
    cleaned = (
        cleaned.withColumn(
            "_encounter_id",
            F.expr("try_cast(pat_enc_csn_id_coded AS BIGINT)"),
        )
        .withColumn(
            "_culture_order_id",
            F.expr("try_cast(order_proc_id_coded AS BIGINT)"),
        )
        .withColumn(
            "_culture_time",
            F.try_to_timestamp(
                F.col("order_time_jittered_utc"),
                F.lit(TIMESTAMP_FORMAT),
            ),
        )
        .withColumn(
            "_days_since_prior_medication",
            F.expr("try_cast(medication_time_to_cultureTime AS INT)"),
        )
    )

    # Optional intervals are retained when missing or malformed.
    invalid_interval = (
        F.col("medication_time_to_cultureTime").isNotNull()
        & F.col("_days_since_prior_medication").isNull()
    )

    negative_interval = F.col("_days_since_prior_medication") < 0

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
        ),
        lambda reason: reason.isNotNull(),
    )

    assessed = cleaned.withColumn(
        "rejection_reasons",
        rejection_reasons,
    )

    valid_condition = F.size("rejection_reasons") == 0

    # Retain original values for rejected records.
    quarantine = assessed.filter(~valid_condition).select(
        *[
            F.col(f"_raw_{column}").alias(column)
            for column in source_columns
        ],
        F.col("rejection_reasons"),
    )

    # Construct the Silver schema in dictionary-defined order.
    silver_columns = [
        F.col(
            {
                "pat_enc_csn_id_coded": "_encounter_id",
                "order_proc_id_coded": "_culture_order_id",
                "order_time_jittered_utc": "_culture_time",
                "medication_time_to_cultureTime": (
                    "_days_since_prior_medication"
                ),
            }.get(source_column, source_column)
        ).alias(silver_column)
        for source_column, silver_column in SOURCE_TO_SILVER.items()
    ]

    silver = assessed.filter(valid_condition).select(*silver_columns)

    # Aggregate row and interval metrics in one Spark action.
    metric_row = assessed.agg(
        F.count("*").alias("input_rows"),
        F.sum(F.when(valid_condition, 1).otherwise(0)).alias("output_rows"),
        F.sum(F.when(~valid_condition, 1).otherwise(0)).alias(
            "quarantined_rows"
        ),
        F.sum(F.when(invalid_interval, 1).otherwise(0)).alias(
            "invalid_intervals"
        ),
        F.sum(F.when(negative_interval, 1).otherwise(0)).alias(
            "negative_intervals"
        ),
    ).first()

    input_rows = int(metric_row["input_rows"] or 0)
    output_rows = int(metric_row["output_rows"] or 0)
    quarantined_rows = int(metric_row["quarantined_rows"] or 0)

    if input_rows != output_rows + quarantined_rows:
        raise RuntimeError(
            "Antibiotic subtype exposure row reconciliation failed: "
            f"input={input_rows}, output={output_rows}, "
            f"quarantined={quarantined_rows}"
        )

    metrics = {
        "input_rows": input_rows,
        "output_rows": output_rows,
        "quarantined_rows": quarantined_rows,
        "invalid_intervals": int(metric_row["invalid_intervals"] or 0),
        "negative_intervals": int(metric_row["negative_intervals"] or 0),
        "null_placeholders_converted_total": sum(
            placeholder_metrics.values()
        ),
        **{
            f"null_placeholders_converted.{column}": count
            for column, count in placeholder_metrics.items()
        },
    }

    return silver, quarantine, metrics
