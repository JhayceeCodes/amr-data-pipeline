"""Ingest the culture cohort CSV into Bronze."""

import logging

from amr_pipeline.config import SOURCE_DIR
from amr_pipeline.ingestion.bronze import ingest_csv_to_bronze
from amr_pipeline.logging_config import configure_logging


def main() -> None:
    configure_logging()
    logging.getLogger(__name__).info("Starting cohort Bronze ingestion")

    manifest = ingest_csv_to_bronze(SOURCE_DIR / "microbiology_cultures_cohort.csv")

    logging.getLogger(__name__).info("Ingestion manifest: %s", manifest)


if __name__ == "__main__":
    main()
