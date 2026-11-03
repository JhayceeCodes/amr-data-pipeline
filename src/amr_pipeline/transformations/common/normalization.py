
"""Shared normalization helpers for Silver transformations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_null_placeholders(
    source: DataFrame,
) -> tuple[DataFrame, dict[str, int]]:
    """Convert exact 'Null' strings to SQL NULL and count conversions.

    Only string columns are examined. All other values are preserved.

    Returns:
        The normalized DataFrame and counts for columns where placeholders
        were found.
    """
    string_columns = [
        field.name
        for field in source.schema.fields
        if field.dataType.simpleString() == "string"
    ]

    if not string_columns:
        return source, {}

    count_expressions = [
        F.count(
            F.when(F.col(column) == "Null", 1)
        ).alias(column)
        for column in string_columns
    ]

    counts_row = source.agg(*count_expressions).first()

    placeholder_counts = {
        column: int(counts_row[column] or 0)
        for column in string_columns
        if (counts_row[column] or 0) > 0
    }

    normalized = source.select(
        *[
            (
                F.when(F.col(column) == "Null", F.lit(None))
                .otherwise(F.col(column))
                .alias(column)
                if column in string_columns
                else F.col(column)
            )
            for column in source.columns
        ]
    )

    return normalized, placeholder_counts
