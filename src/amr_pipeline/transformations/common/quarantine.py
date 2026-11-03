
"""Shared quarantine helpers for Silver transformations."""

from collections.abc import Mapping, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def build_rejection_reasons(
    conditions: Mapping[str, Column],
) -> Column:
    """Build an array of rejection reasons for conditions that are true.

    Args:
        conditions: Mapping of rejection reason names to Spark boolean
            expressions. A reason is included when its condition is true.
    """
    return F.filter(
        F.array(
            *[
                F.when(condition, F.lit(reason))
                for reason, condition in conditions.items()
            ]
        ),
        lambda reason: reason.isNotNull(),
    )


def add_raw_columns(
    source: DataFrame,
    columns: Sequence[str] | None = None,
) -> DataFrame:
    """Add _raw_ columns preserving the original source values."""
    selected_columns = list(columns) if columns is not None else source.columns

    missing_columns = set(selected_columns) - set(source.columns)
    if missing_columns:
        raise ValueError(
            f"Cannot preserve missing source columns: {sorted(missing_columns)}"
        )

    result = source

    for column in selected_columns:
        result = result.withColumn(f"_raw_{column}", F.col(column))

    return result


def select_quarantine_rows(
    assessed: DataFrame,
    source_columns: Sequence[str],
    *,
    reasons_column: str = "rejection_reasons",
) -> DataFrame:
    """Select rejected rows with original source values and rejection reasons."""
    rejected = assessed.filter(F.size(F.col(reasons_column)) > 0)

    return rejected.select(
        *[
            F.col(f"_raw_{column}").alias(column)
            for column in source_columns
        ],
        F.col(reasons_column),
    )
