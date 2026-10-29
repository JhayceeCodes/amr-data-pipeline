"""Silver transformation for the microbiology culture cohort."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TO_SILVER = {
    "anon_id": "anon_id",
    "pat_enc_csn_id_coded": "encounter_id",
    "order_proc_id_coded": "culture_order_id",
    "order_time_jittered_utc": "culture_time",
    "ordering_mode": "care_setting",
    "culture_description": "culture_type",
    "was_positive": "was_positive",
    "organism": "organism",
    "antibiotic": "antibiotic",
    "susceptibility": "susceptibility",
}

TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ssXXX"

REQUIRED_COLUMNS = {
    "anon_id",
    "pat_enc_csn_id_coded",
    "order_proc_id_coded",
    "order_time_jittered_utc",
    "was_positive",
}


def transform_cohort(
    source: DataFrame,
) -> tuple[DataFrame, DataFrame, dict[str, int]]:
    """Transform the Bronze cohort into clean Silver and quarantine rows."""
    missing_columns = REQUIRED_COLUMNS - set(source.columns)
    if missing_columns:
        raise ValueError(
            f"Bronze cohort is missing required columns: {sorted(missing_columns)}"
        )

    # Capture exact placeholder counts before replacing them.
    placeholder_metrics = {
        column: source.filter(F.col(column) == "Null").count()
        for column in source.columns
    }
    placeholder_metrics = {
        column: count for column, count in placeholder_metrics.items() if count > 0
    }

    # Retain original source values for rejected records.
    cleaned = source
    for column in source.columns:
        cleaned = cleaned.withColumn(
            f"_raw_{column}",
            F.col(column),
        )

    # Convert exact "Null" placeholders to SQL NULL in working columns.
    for column in source.columns:
        cleaned = cleaned.withColumn(
            column,
            F.when(F.col(column) == "Null", F.lit(None)).otherwise(F.col(column)),
        )

    # Parse required identifiers without allowing malformed values to
    # abort the entire Spark job.
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
            F.expr(
                "try_to_timestamp(order_time_jittered_utc, 'yyyy-MM-dd HH:mm:ssXXX')"
            ),
        )
    )

    # Parse the positive-culture flag explicitly.
    cleaned = cleaned.withColumn(
        "_was_positive",
        F.when(F.col("was_positive") == "1", F.lit(True)).when(
            F.col("was_positive") == "0", F.lit(False)
        ),
    )

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
            F.when(
                F.col("_culture_time").isNull(),
                F.lit("invalid_or_missing_culture_time"),
            ),
            F.when(
                F.col("_was_positive").isNull(),
                F.lit("invalid_or_missing_was_positive"),
            ),
        )
    )

    assessed = cleaned.withColumn(
        "rejection_reasons",
        rejection_reasons,
    )

    quarantine = assessed.filter(F.size(F.col("rejection_reasons")) > 0).select(
        *[F.col(f"_raw_{column}").alias(column) for column in source.columns],
        F.col("rejection_reasons"),
    )

    silver = assessed.filter(F.size(F.col("rejection_reasons")) == 0).select(
        F.col("anon_id").cast("string").alias("anon_id"),
        F.col("_encounter_id").alias("encounter_id"),
        F.col("_culture_order_id").alias("culture_order_id"),
        F.col("_culture_time").alias("culture_time"),
        F.col("ordering_mode").alias("care_setting"),
        F.col("culture_description").alias("culture_type"),
        F.col("_was_positive").alias("was_positive"),
        F.col("organism").alias("organism"),
        F.col("antibiotic").alias("antibiotic"),
        F.col("susceptibility").alias("susceptibility"),
    )

    input_rows = source.count()
    output_rows = silver.count()
    quarantined_rows = quarantine.count()

    if input_rows != output_rows + quarantined_rows:
        raise RuntimeError(
            "Cohort row reconciliation failed: "
            f"input={input_rows}, output={output_rows}, "
            f"quarantined={quarantined_rows}"
        )

    metrics = {
        "input_rows": input_rows,
        "output_rows": output_rows,
        "quarantined_rows": quarantined_rows,
        "null_placeholders_converted_total": sum(placeholder_metrics.values()),
        **{
            f"null_placeholders_converted.{column}": count
            for column, count in placeholder_metrics.items()
        },
    }

    return silver, quarantine, metrics
