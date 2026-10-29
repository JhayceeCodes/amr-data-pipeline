"""Validate order-level identity consistency in the cohort dataset."""

from pyspark.sql import functions as F

from amr_pipeline.config import BRONZE_DIR
from amr_pipeline.spark import create_spark_session

SOURCE = BRONZE_DIR / "parquet" / "microbiology_cultures_cohort"

ORDER_ID = "order_proc_id_coded"
IDENTITY_COLUMNS = {
    "patient_id": "anon_id",
    "encounter_id": "pat_enc_csn_id_coded",
    "culture_time": "order_time_jittered_utc",
}


def main() -> None:
    """Check whether each culture order maps consistently to its attributes."""
    spark = create_spark_session("AMR-Culture-Order-Identity-Check")

    try:
        df = spark.read.parquet(str(SOURCE))

        required_columns = [ORDER_ID, *IDENTITY_COLUMNS.values()]
        missing = sorted(set(required_columns) - set(df.columns))

        if missing:
            raise ValueError(f"Missing required source columns: {missing}")

        print("\n=== Source overview ===")
        row_count = df.count()
        distinct_orders = df.select(ORDER_ID).distinct().count()
        missing_order_ids = df.filter(
            F.col(ORDER_ID).isNull()
            | (F.trim(F.col(ORDER_ID)) == "")
            | (F.col(ORDER_ID) == "Null")
        ).count()

        print(f"Rows: {row_count:,}")
        print(f"Distinct order IDs, including null-like values: {distinct_orders:,}")
        print(f"Rows with missing/null-like order IDs: {missing_order_ids:,}")

        # Only non-missing order IDs can be validated as order identities.
        valid_orders = df.filter(
            F.col(ORDER_ID).isNotNull()
            & (F.trim(F.col(ORDER_ID)) != "")
            & (F.col(ORDER_ID) != "Null")
        )

        aggregations = [
            F.count("*").alias("row_count"),
        ]

        for label, source_column in IDENTITY_COLUMNS.items():
            aggregations.extend(
                [
                    F.countDistinct(source_column).alias(f"{label}_distinct"),
                    F.sum(
                        F.when(
                            F.col(source_column).isNull()
                            | (F.trim(F.col(source_column)) == "")
                            | (F.col(source_column) == "Null"),
                            1,
                        ).otherwise(0)
                    ).alias(f"{label}_missing_rows"),
                ]
            )

        per_order = valid_orders.groupBy(ORDER_ID).agg(*aggregations)

        print("\n=== Order-level consistency ===")
        print(f"Orders checked: {per_order.count():,}")

        for label in IDENTITY_COLUMNS:
            distinct_col = f"{label}_distinct"
            inconsistent = per_order.filter(F.col(distinct_col) > 1)
            inconsistent_count = inconsistent.count()

            missing_rows = (
                per_order.agg(
                    F.sum(f"{label}_missing_rows").alias("missing_rows")
                ).first()["missing_rows"]
                or 0
            )

            orders_with_missing = per_order.filter(
                F.col(f"{label}_missing_rows") > 0
            ).count()

            print(f"\n{label}:")
            print(f"  Orders with conflicting values: {inconsistent_count:,}")
            print(f"  Orders with at least one missing value: {orders_with_missing:,}")
            print(f"  Rows with missing values: {missing_rows:,}")

            if inconsistent_count:
                print("  Sample conflicting orders:")
                inconsistent.select(
                    ORDER_ID,
                    "row_count",
                    distinct_col,
                ).show(10, truncate=False)

        print("\n=== Interpretation ===")
        print(
            "A consistent order-level attribute has at most one distinct "
            "non-missing value per order. Missing values are reported "
            "separately and do not count as conflicting values."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
