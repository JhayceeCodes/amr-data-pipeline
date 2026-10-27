"""Logging configuration for the AMR data pipeline."""

import logging
from logging.handlers import RotatingFileHandler

from amr_pipeline.config import LOG_DIR

LOG_FILE = LOG_DIR / "amr_pipeline.log"


def configure_logging(
    level: int = logging.INFO,
) -> None:
    """Log to the console and a rotating application log file."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    logging.basicConfig(
        level=level,
        handlers=[console_handler, file_handler],
        force=True,
    )
