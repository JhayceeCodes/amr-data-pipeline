"""Silver transformation for microbiology culture cohort records."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from amr_pipeline.transformations.common.casting import (
    try_cast_bigint,
    try_parse_timestamp,
)
from amr_pipeline.transformations.common.metrics import build_row_metrics
from amr_pipeline.transformations.common.normalization import (
    normalize_null_placeholders,
)
from amr_pipeline.transformations.common.quarantine import (
    build_rejection_reasons,
    select_quarantine_rows,
)


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

REQUIRED_COLUMNS = {
    "anon_id",
    "pat_enc_csn_id_coded",
    "order_proc_id_coded",
    "order_time_jittered_utc",
    "was_positive",
}

DERIVED_COLUMNS = {
    "pat_enc_csn_id_coded": "_encounter_id",
    "order_proc_id_coded": "_culture_order_id",
    "order_time_jittered_utc": "_culture_time",
    "was_positive": "_was_positive",
}

REQUIRED_FIELDS = {
    "anon_id": "missing_anon_id",
    "pat_enc_csn_id_coded": "invalid_encounter_id",
    "order_proc_id_coded": "invalid_culture_order_id",
    "order_time_jittered_utc": "invalid_culture_time",
    "was_positive": "invalid_was_positive",
}

SILVER_COLUMNS = list(SOURCE_TO_SILVER.values())


def _is_invalid_boolean(column: str) -> Column:
    """Return true when a non-null value is not a supported Boolean flag."""
    normalized = F.lower(F.trim(F.col(column)))

    return (
        F.col(column).isNull()
        | ~normalized.isin("0", "1", "true", "false")
    )


def transform_cohort(
    source: DataFrame,
) -> tuple[DataFrame, DataFrame, dict]:
    """Transform Bronze culture cohort records into Silver and quarantine.

    Exact ``"Null"`` placeholders are converted to SQL NULL. Required
    identifiers, the culture timestamp, and the positive-culture flag are
    validated. Invalid required values are quarantined with their original
    source values and rejection reasons.

    Optional fields are retained as NULL when missing. Distinct observations
    are preserved; this transformation does not deduplicate source records.
    """
    # Validate required and mapped source columns.
    missing_required = REQUIRED_COLUMNS - set(source.columns)

    if missing_required:
        raise ValueError(
            "Bronze cohort is missing required columns: "
            f"{sorted(missing_required)}"
        )

    missing_mapped = set(SOURCE_TO_SILVER) - set(source.columns)

    if missing_mapped:
        raise ValueError(
            "Bronze cohort is missing mapped columns: "
            f"{sorted(missing_mapped)}"
        )

    input_rows = source.count()

    # Normalize placeholders while retaining original values for quarantine.
    # Requires normalize_null_placeholders(..., preserve_raw=True).
    normalized, placeholder_counts = normalize_null_placeholders(
        source,
        preserve_raw=True,
    )

    # Build parsed fields using the shared safe-casting helpers.
    assessed = (
        normalized
        .withColumn(
            DERIVED_COLUMNS["pat_enc_csn_id_coded"],
            try_cast_bigint("pat_enc_csn_id_coded"),
        )
        .withColumn(
            DERIVED_COLUMNS["order_proc_id_coded"],
            try_cast_bigint("order_proc_id_coded"),
        )
        .withColumn(
            DERIVED_COLUMNS["order_time_jittered_utc"],
            try_parse_timestamp("order_time_jittered_utc"),
        )
        .withColumn(
            DERIVED_COLUMNS["was_positive"],
            F.when(
                F.lower(F.trim(F.col("was_positive"))).isin("1", "true"),
                F.lit(True),
            )
            .when(
                F.lower(F.trim(F.col("was_positive"))).isin("0", "false"),
                F.lit(False),
            )
            .otherwise(F.lit(None).cast("boolean")),
        )
    )

    # Construct rejection conditions against the parsed fields.
    rejection_conditions = {
        "missing_anon_id": F.col("anon_id").isNull(),
        "invalid_or_missing_encounter_id": F.col("_encounter_id").isNull(),
        "invalid_or_missing_culture_order_id": F.col("_culture_order_id").isNull(),
        "invalid_or_missing_culture_time": (
                F.col("order_time_jittered_utc").isNull()
                | F.col("_culture_time").isNull()
        ),
        "invalid_or_missing_was_positive": _is_invalid_boolean("was_positive"),
    }
    assessed = assessed.withColumn(
        "rejection_reasons",
        build_rejection_reasons(rejection_conditions),
    )

    source_columns = list(source.columns)

    quarantine = select_quarantine_rows(
        assessed,
        source_columns,
    )

    # Select only accepted rows and build the standardized Silver schema.
    accepted = assessed.filter(F.size("rejection_reasons") == 0)

    silver = accepted.select(
        F.col("anon_id").cast("string").alias("anon_id"),
        F.col("_encounter_id").cast("long").alias("encounter_id"),
        F.col("_culture_order_id").cast("long").alias("culture_order_id"),
        F.col("_culture_time").alias("culture_time"),
        F.col("ordering_mode").cast("string").alias("care_setting"),
        F.col("culture_description").cast("string").alias("culture_type"),
        F.col("_was_positive").cast("boolean").alias("was_positive"),
        F.col("organism").cast("string").alias("organism"),
        F.col("antibiotic").cast("string").alias("antibiotic"),
        F.col("susceptibility").cast("string").alias("susceptibility"),
    )

    output_rows = silver.count()
    quarantined_rows = quarantine.count()

    metrics = build_row_metrics(
        input_rows=input_rows,
        output_rows=output_rows,
        quarantined_rows=quarantined_rows,
        null_placeholders_converted=placeholder_counts,
    )

    return silver, quarantine, metrics