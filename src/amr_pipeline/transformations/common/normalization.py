
"""Shared normalization helpers for Silver transformations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_null_placeholders(
    source: DataFrame,
    *,
    preserve_raw: bool = False,
) -> tuple[DataFrame, dict[str, int]]:
    """Convert exact 'Null' strings to SQL NULL and count conversions.

    Only string columns are examined. All other values are preserved.

    Returns:
        The normalized DataFrame and counts for columns where placeholders
        were found.
    """
    source_columns = source.columns
    string_columns = {
        field.name
        for field in source.schema.fields
        if field.dataType.simpleString() == "string"
    }

    if preserve_raw:
        collisions = {
                         f"_raw_{column}"
                         for column in source_columns
                     } & set(source_columns)

        if collisions:
            raise ValueError(
                f"Raw-column names would collide: {sorted(collisions)}"
            )

    if string_columns:
        count_expressions = [
            F.count(
                F.when(F.col(column) == "Null", 1)
            ).alias(column)
            for column in sorted(string_columns)
        ]

        counts_row = source.agg(*count_expressions).first()

        placeholder_counts = {
            column: int(counts_row[column] or 0)
            for column in sorted(string_columns)
            if (counts_row[column] or 0) > 0
        }
    else:
        placeholder_counts = {}

    expressions = []

    for column in source_columns:
        if column in string_columns:
            expressions.append(
                F.when(
                    F.col(column) == "Null",
                    F.lit(None).cast("string"),
                )
                .otherwise(F.col(column))
                .alias(column)
            )
        else:
            expressions.append(F.col(column))

    if preserve_raw:
        expressions.extend(
            F.col(column).alias(f"_raw_{column}")
            for column in source_columns
        )

    normalized = source.select(*expressions)

    return normalized, placeholder_counts
