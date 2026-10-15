
from pathlib import Path

import pandas as pd


DATA_PATH = Path("data/source/microbiology_cultures_prior_med.csv")


def main():
    df = pd.read_csv(DATA_PATH)



    # Number of rows beyond the first occurrence of each exact row
    duplicate_excess = df.duplicated().sum()

    # Number of distinct complete rows
    distinct_rows = len(df.drop_duplicates())

    print(f"Rows in duplicate groups: {df.duplicated(keep=False).sum():,}")
    print(f"Additional exact copies: {duplicate_excess:,}")
    print(f"Distinct complete rows: {distinct_rows:,}")
    print(
        "Check:",
        f"{distinct_rows + duplicate_excess:,} == {len(df):,}",
    )


    # Frequency of exact rows
    exact_row_counts = (
        df.value_counts(dropna=False)
        .rename("occurrences")
        .reset_index()
    )

    print("Exact-row occurrence distribution:")
    print(
        exact_row_counts["occurrences"]
        .value_counts()
        .sort_index()
        .tail(20)
    )

    print("\nLargest exact duplicate groups:")
    print(exact_row_counts.head(20).to_string(index=False))



    candidate_key = [
        "anon_id",
        "pat_enc_csn_id_coded",
        "order_proc_id_coded",
        "medication_name",
        "medication_time_to_culturetime",
        "medication_category",
    ]

    timestamp_counts = (
        df.groupby(candidate_key, dropna=False)[
            "order_time_jittered_utc"
        ]
        .nunique()
    )

    conflicting_keys = timestamp_counts[timestamp_counts > 1]

    print(
        "Candidate-key groups with multiple timestamps:",
        f"{len(conflicting_keys):,}",
    )




    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")

    print("\nColumn data types:")
    print(df.dtypes)

    print("\nNull counts:")
    print(df.isna().sum())

    # Repeated rows across every column
    exact_duplicates = df.duplicated(keep=False)
    print(
        "\nRows belonging to exact-duplicate groups:",
        f"{exact_duplicates.sum():,}",
    )

    print("\nMost frequent exact duplicate rows:")
    duplicate_groups = (
        df.loc[exact_duplicates]
        .value_counts(dropna=False)
        .rename("occurrences")
        .reset_index()
    )
    print(duplicate_groups.head(10).to_string(index=False))

    # Repeated culture order identifiers are expected to be possible.
    print("\nOrder identifier profile:")
    order_ids = df["order_proc_id_coded"]

    print(f"Distinct order IDs: {order_ids.nunique():,}")
    print(f"Rows with missing order IDs: {order_ids.isna().sum():,}")

    # Test a candidate grain; this is an investigation, not a
    # declaration that these columns form the correct key.
    candidate_key = [
        "anon_id",
        "pat_enc_csn_id_coded",
        "order_proc_id_coded",
        "medication_name",
        "medication_time_to_culturetime",
        "medication_category",
    ]

    repeated_key = df.duplicated(
        subset=candidate_key,
        keep=False,
    )

    print(
        "\nRows belonging to repeated candidate-key groups:",
        f"{repeated_key.sum():,}",
    )


if __name__ == "__main__":
    main()
