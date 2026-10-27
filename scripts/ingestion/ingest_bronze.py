"""Ingest source CSV datasets into Bronze."""

import argparse
import logging

from amr_pipeline.config import SOURCE_DIR
from amr_pipeline.ingestion.bronze import ingest_csv_to_bronze
from amr_pipeline.logging_config import configure_logging

logger = logging.getLogger(__name__)


def main() -> None:
    """Parse arguments and ingest selected CSV datasets."""
    parser = argparse.ArgumentParser(
        description="Ingest source CSV datasets into Bronze Parquet."
    )
    parser.add_argument(
        "datasets",
        nargs="*",
        help="CSV filenames to ingest. Omit to ingest all source CSV files.",
    )
    args = parser.parse_args()

    configure_logging()

    if args.datasets:
        source_paths = [SOURCE_DIR / name for name in args.datasets]
    else:
        source_paths = sorted(SOURCE_DIR.glob("*.csv"))

    if not source_paths:
        parser.error(f"No CSV datasets found in {SOURCE_DIR}")

    failures = []

    for source_path in source_paths:
        try:
            logger.info("Starting Bronze ingestion: %s", source_path.name)
            manifest = ingest_csv_to_bronze(source_path)
            logger.info(
                "Ingestion complete | dataset=%s | rows=%d | parquet_rows=%d",
                manifest["source_filename"],
                manifest["source_row_count"],
                manifest["parquet_row_count"],
            )
        except Exception:
            logger.exception("Bronze ingestion failed: %s", source_path.name)
            failures.append(source_path.name)

    if failures:
        raise SystemExit(
            f"Failed to ingest {len(failures)} dataset(s): {', '.join(failures)}"
        )


if __name__ == "__main__":
    main()
