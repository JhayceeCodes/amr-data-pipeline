
"""Shared safe-casting expressions for Silver transformations."""

from pyspark.sql import Column
from pyspark.sql import functions as F


TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ssXXX"


def try_cast_bigint(column: str) -> Column:
    """Safely cast a column to BIGINT, returning NULL on invalid input."""
    return F.expr(f"try_cast(`{column}` AS BIGINT)")


def try_cast_int(column: str) -> Column:
    """Safely cast a column to INT, returning NULL on invalid input."""
    return F.expr(f"try_cast(`{column}` AS INT)")


def try_cast_double(column: str) -> Column:
    """Safely cast a column to DOUBLE, returning NULL on invalid input."""
    return F.expr(f"try_cast(`{column}` AS DOUBLE)")


def try_parse_timestamp(
    column: str,
    timestamp_format: str = TIMESTAMP_FORMAT,
) -> Column:
    """Safely parse a timestamp using the specified Spark format."""
    return F.try_to_timestamp(
        F.col(column),
        F.lit(timestamp_format),
    )
