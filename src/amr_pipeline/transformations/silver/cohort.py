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

# Source fields that require parsing or type conversion.
DERIVED_COLUMNS = {
    "pat_enc_csn_id_coded": "_encounter_id",
    "order_proc_id_coded": "_culture_order_id",
    "order_time_jittered_utc": "_culture_time",
    "was_positive": "_was_positive",
}

# Explicit output types for fields that need casting.
COLUMN_CASTS = {
    "anon_id": "string",
}


def transform_cohort(
    source: DataFrame,
) -> tuple[DataFrame, DataFrame, dict[str, int]]:
    """Transform the Bronze cohort into clean Silver and quarantine rows."""

    # Validate the source schema.
    missing_columns = REQUIRED_COLUMNS - set(source.columns)

    if missing_columns:
        raise ValueError(
            f"Bronze cohort is missing required columns: {sorted(missing_columns)}"
        )

    # Validate that every mapped source column exists.
    mapped_columns = set(SOURCE_TO_SILVER)
    missing_mapped_columns = mapped_columns - set(source.columns)

    if missing_mapped_columns:
        raise ValueError(
            "Bronze cohort is missing mapped columns: "
            f"{sorted(missing_mapped_columns)}"
        )

    # Capture exact placeholder counts before replacing them.
    placeholder_metrics = {
        column: source.filter(F.col(column) == "Null").count()
        for column in source.columns
    }

    placeholder_metrics = {
        column: count
        for column, count in placeholder_metrics.items()
        if count > 0
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

    # Parse identifiers.
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
            DERIVED_COLUMNS["order_time_jittered_utc"],
            F.try_to_timestamp(
                F.col("order_time_jittered_utc"),
                F.lit(TIMESTAMP_FORMAT),
            ),
        )
    )

    # Parse the positive-culture flag explicitly.
    cleaned = cleaned.withColumn(
        DERIVED_COLUMNS["was_positive"],
        F.when(F.col("was_positive") == "1", F.lit(True)).when(
            F.col("was_positive") == "0", F.lit(False)
        ),
    )

    # Build rejection reasons for invalid required fields.
    rejection_reasons = F.array_compact(
        F.array(
            F.when(
                F.col("anon_id").isNull(),
                F.lit("missing_anon_id"),
            ),
            F.when(
                F.col(DERIVED_COLUMNS["pat_enc_csn_id_coded"]).isNull(),
                F.lit("invalid_or_missing_encounter_id"),
            ),
            F.when(
                F.col(DERIVED_COLUMNS["order_proc_id_coded"]).isNull(),
                F.lit("invalid_or_missing_culture_order_id"),
            ),
            F.when(
                F.col(DERIVED_COLUMNS["order_time_jittered_utc"]).isNull(),
                F.lit("invalid_or_missing_culture_time"),
            ),
            F.when(
                F.col(DERIVED_COLUMNS["was_positive"]).isNull(),
                F.lit("invalid_or_missing_was_positive"),
            ),
        )
    )

    assessed = cleaned.withColumn(
        "rejection_reasons",
        rejection_reasons,
    )

    # Preserve original source values in quarantine.
    quarantine = assessed.filter(
        F.size(F.col("rejection_reasons")) > 0
    ).select(
        *[
            F.col(f"_raw_{column}").alias(column)
            for column in source.columns
        ],
        F.col("rejection_reasons"),
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

    # Reconcile input, Silver, and quarantine row counts.
    input_rows = source.count()
    output_rows = silver.count()
    quarantined_rows = quarantine.count()

    if input_rows != output_rows + quarantined_rows:
        raise RuntimeError(
            "Cohort row reconciliation failed: "
            f"input={input_rows}, output={output_rows}, "
            f"quarantined={quarantined_rows}"
        )

    # Collect transformation metrics.
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