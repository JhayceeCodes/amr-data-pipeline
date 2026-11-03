"""Silver transformation for implied susceptibility observations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TO_SILVER = {
    "anon_id": "anon_id",
    "pat_enc_csn_id_coded": "encounter_id",
    "order_proc_id_coded": "culture_order_id",
    "organism": "organism",
    "antibiotic": "antibiotic",
    "susceptibility": "susceptibility",
    "implied_susceptibility": "implied_susceptibility",
}

REQUIRED_COLUMNS = set(SOURCE_TO_SILVER)


def transform_implied_susceptibility(
    source: DataFrame,
) -> tuple[DataFrame, DataFrame, dict]:
    """Transform Bronze implied susceptibility records into Silver.

    Exact "Null" placeholders are normalized to SQL NULL. Source
    interpretation values are preserved without remapping, and repeated
    observations are retained.

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

    # Retain original values for rejected records.
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
        *[
            F.col(column).alias(f"_raw_{column}")
            for column in source_columns
        ],
    )

    typed = (
        normalized.withColumn(
            "_encounter_id",
            F.expr("try_cast(pat_enc_csn_id_coded AS BIGINT)"),
        )
        .withColumn(
            "_culture_order_id",
            F.expr("try_cast(order_proc_id_coded AS BIGINT)"),
        )
    )

    # Required identity and observation fields.
    # Susceptibility interpretations remain nullable.
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

    silver_columns = [
        (
            F.col("_encounter_id")
            if source_column == "pat_enc_csn_id_coded"
            else F.col("_culture_order_id")
            if source_column == "order_proc_id_coded"
            else F.col(source_column)
        ).alias(silver_column)
        for source_column, silver_column in SOURCE_TO_SILVER.items()
    ]

    silver = assessed.filter(valid_condition).select(*silver_columns)

    quarantine = assessed.filter(~valid_condition).select(
        *[
            F.col(f"_raw_{column}").alias(column)
            for column in source_columns
        ],
        F.col("rejection_reasons"),
    )

    metrics_row = assessed.agg(
        F.count("*").alias("input_rows"),
        F.sum(
            F.when(valid_condition, 1).otherwise(0)
        ).alias("output_rows"),
        F.sum(
            F.when(~valid_condition, 1).otherwise(0)
        ).alias("quarantined_rows"),
    ).first()

    input_rows = int(metrics_row["input_rows"] or 0)
    output_rows = int(metrics_row["output_rows"] or 0)
    quarantined_rows = int(metrics_row["quarantined_rows"] or 0)

    if input_rows != output_rows + quarantined_rows:
        raise RuntimeError(
            "Implied susceptibility row reconciliation failed: "
            f"input={input_rows}, output={output_rows}, "
            f"quarantined={quarantined_rows}"
        )

    metrics = {
        "input_rows": input_rows,
        "output_rows": output_rows,
        "quarantined_rows": quarantined_rows,
        "null_placeholders_converted_total": sum(
            null_placeholders_converted.values()
        ),
        "null_placeholders_converted": null_placeholders_converted,
    }

    return silver, quarantine, metrics