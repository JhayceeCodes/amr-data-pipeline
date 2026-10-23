"""Logging configuration for the AMR data pipeline."""

import logging
import sys


def configure_logging(
    level: int = logging.INFO,
) -> None:
    """Configure application logging to write to the console."""
    logging.basicConfig(
        level=level,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )