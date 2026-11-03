
"""Shared metrics helpers for Silver transformations."""


def build_row_metrics(
    *,
    input_rows: int,
    output_rows: int,
    quarantined_rows: int,
    null_placeholders_converted: dict[str, int],
    duplicate_rows_removed: int = 0,
    extra_metrics: dict[str, int] | None = None,
) -> dict:
    """Build consistent Silver metrics and validate row reconciliation."""
    if min(
        input_rows,
        output_rows,
        quarantined_rows,
        duplicate_rows_removed,
    ) < 0:
        raise ValueError("Row counts and duplicate counts cannot be negative.")

    if duplicate_rows_removed > input_rows:
        raise ValueError("Duplicate rows removed cannot exceed input rows.")

    placeholder_counts = {
        column: count
        for column, count in null_placeholders_converted.items()
        if count > 0
    }

    if any(count < 0 for count in placeholder_counts.values()):
        raise ValueError("Placeholder counts cannot be negative.")

    expected_rows = (
        duplicate_rows_removed + output_rows + quarantined_rows
    )

    if input_rows != expected_rows:
        raise RuntimeError(
            "Silver row reconciliation failed: "
            f"input={input_rows}, "
            f"duplicates_removed={duplicate_rows_removed}, "
            f"output={output_rows}, "
            f"quarantined={quarantined_rows}"
        )

    metrics = {
        "input_rows": input_rows,
        "deduplicated_rows": input_rows - duplicate_rows_removed,
        "duplicate_rows_removed": duplicate_rows_removed,
        "output_rows": output_rows,
        "quarantined_rows": quarantined_rows,
        "null_placeholders_converted_total": sum(
            placeholder_counts.values()
        ),
        "null_placeholders_converted": placeholder_counts,
    }

    if extra_metrics:
        metrics.update(extra_metrics)

    return metrics
