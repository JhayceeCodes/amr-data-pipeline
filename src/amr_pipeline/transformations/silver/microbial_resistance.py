
"""Silver transformation for microbial resistance history."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ssXXX"

SOURCE_TO_SILVER = {
    "anon_id": "anon_id",
    "pat_enc_csn_id_coded": "encounter_id",
    "order_proc_id_coded": "culture_order_id",
    "order_time_jittered_utc": "culture_time",
    "organism": "organism",
    "antibiotic": "antibiotic",
    "resistant_time_to_culturetime": "resistant_time_to_culturetime",
}

REQUIRED_COLUMNS = set(SOURCE_TO_SILVER)

DERIVED_COLUMNS = {
    "pat_enc_csn_id_coded": "_encounter_id",
    "order_proc_id_coded": "_culture_order_id",
    "order_time_jittered_utc": "_culture_time",
    "resistant_time_to_culturetime": "_resistant_time_to_culturetime",
}


def transform_microbial_resistance(
    source: DataFrame,
) -> tuple[DataFrame, DataFrame, dict]:
    """Transform Bronze microbial resistance records into Silver.

    Returns:
        A tuple containing the Silver DataFrame, quarantine DataFrame,
        and transformation metrics.
    """
    missing_columns = REQUIRED_COLUMNS - set(source.columns)

    if missing_columns:
        raise ValueError(
            "Missing required source columns: "
            f"{sorted(missing_columns)}"
        )

    source_columns = source.columns

    # Count exact "Null" placeholders before normalization.
    placeholder_expressions = [
        F.sum(
            F.when(F.col(column) == "Null", 1).otherwise(0)
        ).alias(column)
        for column in source_columns
    ]

    placeholder_row = source.agg(*placeholder_expressions).first()

    null_placeholders_converted = {
        column: int(placeholder_row[column] or 0)
        for column in source_columns
        if (placeholder_row[column] or 0) > 0
    }

    # Preserve original values for rejected records.
    raw_columns = [
        F.col(column).alias(f"_raw_{column}")
        for column in source_columns
    ]

    normalized = source.select(
        *[
            F.when(
                F.col(column) == "Null",
                F.lit(None),
            )
            .otherwise(F.col(column))
            .alias(column)
            for column in source_columns
        ],
        *raw_columns,
        F.monotonically_increasing_id().alias("_source_row_id"),
    )

    interval_col = "resistant_time_to_culturetime"

    # Safely parse IDs, timestamp, and resistance interval.
    typed = (
        normalized.withColumn(
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
            "_resistant_time_to_culturetime",
            F.expr(
                "try_cast(resistant_time_to_culturetime AS INT)"
            ),
        )
    )

    # Required-field validation. The interval is intentionally optional.
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
                F.col("organism").isNull(),
                F.lit("missing_organism"),
            ),
            F.when(
                F.col("antibiotic").isNull(),
                F.lit("missing_antibiotic"),
            ),
        ),
        lambda reason: reason.isNotNull(),
    )

    assessed = typed.withColumn(
        "rejection_reasons",
        rejection_reasons,
    )

    valid_condition = F.size("rejection_reasons") == 0

    # Select Silver columns, applying the derived types.
    silver_columns = [
        (
            F.col(DERIVED_COLUMNS[column])
            if column in DERIVED_COLUMNS
            else F.col(column)
        ).alias(silver_column)
        for column, silver_column in SOURCE_TO_SILVER.items()
    ]

    silver = assessed.filter(valid_condition).select(*silver_columns)

    quarantine = assessed.filter(~valid_condition).select(
        *[
            F.col(f"_raw_{column}").alias(column)
            for column in source_columns
        ],
        F.col("rejection_reasons"),
    )

    # Exact "Null" placeholders become actual NULLs before this check.
    invalid_interval = (
        F.col(interval_col).isNotNull()
        & F.col("_resistant_time_to_culturetime").isNull()
    )
    negative_interval = F.col("_resistant_time_to_culturetime") < 0

    metrics_row = assessed.agg(
        F.count("*").alias("input_rows"),
        F.sum(
            F.when(valid_condition, 1).otherwise(0)
        ).alias("output_rows"),
        F.sum(
            F.when(~valid_condition, 1).otherwise(0)
        ).alias("quarantined_rows"),
        F.sum(
            F.when(invalid_interval, 1).otherwise(0)
        ).alias("invalid_intervals"),
        F.sum(
            F.when(negative_interval, 1).otherwise(0)
        ).alias("negative_intervals"),
    ).first()

    input_rows = int(metrics_row["input_rows"] or 0)
    output_rows = int(metrics_row["output_rows"] or 0)
    quarantined_rows = int(metrics_row["quarantined_rows"] or 0)

    if input_rows != output_rows + quarantined_rows:
        raise RuntimeError(
            "Microbial resistance row reconciliation failed: "
            f"input={input_rows}, output={output_rows}, "
            f"quarantined={quarantined_rows}"
        )

    metrics = {
        "input_rows": input_rows,
        "output_rows": output_rows,
        "quarantined_rows": quarantined_rows,
        "invalid_intervals": int(
            metrics_row["invalid_intervals"] or 0
        ),
        "negative_intervals": int(
            metrics_row["negative_intervals"] or 0
        ),
        "null_placeholders_converted_total": sum(
            null_placeholders_converted.values()
        ),
        "null_placeholders_converted": null_placeholders_converted,
    }

    return silver, quarantine, metrics
