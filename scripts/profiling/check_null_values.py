"""Profile null-like values in a Bronze CSV snapshot."""

import csv
import sys
from collections import Counter

from amr_pipeline.config import BRONZE_DIR


def main() -> None:
    filename = sys.argv[1] if len(sys.argv) > 1 else (
        "microbiology_cultures_cohort.csv"
    )
    path = BRONZE_DIR / "raw" / filename

    if not path.is_file():
        raise FileNotFoundError(f"Bronze CSV not found: {path}")

    counts = Counter()
    rows = 0

    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)

        for row in reader:
            rows += 1

            for column, value in row.items():
                if value is None:
                    counts[(column, "MISSING_FIELD")] += 1
                elif value == "":
                    counts[(column, "EMPTY_STRING")] += 1
                elif value.strip() == "":
                    counts[(column, "WHITESPACE_ONLY")] += 1
                elif value == "Null":
                    counts[(column, 'LITERAL "Null"')] += 1
                elif value.strip().lower() == "null":
                    counts[(column, "NULL_CASE_VARIANT")] += 1

    print(f"\nFile: {path.name}")
    print(f"Rows scanned: {rows}")
    print("\nNull-like values by column:")

    if not counts:
        print("No null-like values found.")
        return

    for (column, value_type), count in sorted(counts.items()):
        print(f"{column:55} {value_type:20} {count:>12,}")


if __name__ == "__main__":
    main()