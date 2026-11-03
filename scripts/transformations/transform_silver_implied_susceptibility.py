
"""Run the Silver implied susceptibility transformation."""

import json
import logging
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from amr_pipeline.spark import create_spark_session
from amr_pipeline.transformations.silver.implied_susceptibility import (
    transform_implied_susceptibility,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SOURCE_PATH = (
    PROJECT_ROOT
    / "data/bronze/parquet/microbiology_cultures_implied_susceptibility"
)
SILVER_PATH = PROJECT_ROOT / "data/silver/implied_susceptibility"
QUARANTINE_PATH = (
    PROJECT_ROOT / "data/silver/quarantine/implied_susceptibility"
)
STAGING_ROOT = PROJECT_ROOT / "data/silver/_staging"
REPORTS_PATH = PROJECT_ROOT / "data/silver/runs"

LOGGER = logging.getLogger(__name__)


def configure_logging() -> None:
    """Configure console logging for the runner."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def write_json_atomic(path: Path, payload: dict) -> None:
    """Write a JSON report through a temporary file and rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(".json.tmp")

    try:
        with temporary_path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, indent=2, sort_keys=True)
            file.write("\n")

        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)


def publish_directory(staged_path: Path, destination: Path) -> None:
    """Publish a staged directory without replacing existing output."""
    if destination.exists():
        raise FileExistsError(
            f"Refusing to overwrite existing output: {destination}"
        )

    destination.parent.mkdir(parents=True, exist_ok=True)

    # Both paths should reside on the same filesystem for atomic rename.
    staged_path.rename(destination)


def main() -> int:
    """Execute the Silver transformation and publish its outputs."""
    configure_logging()

    run_id = str(uuid4())
    started_at = datetime.now(UTC)
    staging_path = STAGING_ROOT / run_id
    staged_silver_path = staging_path / "implied_susceptibility"
    staged_quarantine_path = staging_path / "quarantine"

    spark = None
    report = {
        "dataset": "implied_susceptibility",
        "run_id": run_id,
        "status": "running",
        "started_at": started_at.isoformat(),
        "source_path": str(SOURCE_PATH),
        "silver_path": str(SILVER_PATH),
        "quarantine_path": str(QUARANTINE_PATH),
    }

    try:
        if not SOURCE_PATH.is_dir():
            raise FileNotFoundError(
                f"Bronze source directory does not exist: {SOURCE_PATH}"
            )

        # Prevent accidental replacement of previously published outputs.
        existing_outputs = [
            path
            for path in (SILVER_PATH, QUARANTINE_PATH)
            if path.exists()
        ]
        if existing_outputs:
            paths = ", ".join(str(path) for path in existing_outputs)
            raise FileExistsError(
                f"Existing output detected; refusing to overwrite: {paths}"
            )

        staging_path.mkdir(parents=True, exist_ok=False)

        spark = create_spark_session("Silver-Implied-Susceptibility")

        LOGGER.info("Reading Bronze dataset from %s", SOURCE_PATH)
        source = spark.read.parquet(str(SOURCE_PATH))

        silver, quarantine, metrics = transform_implied_susceptibility(
            source
        )

        LOGGER.info(
            "Transformation metrics: %s",
            json.dumps(metrics, sort_keys=True),
        )

        LOGGER.info("Writing staged Silver dataset")
        silver.write.mode("errorifexists").parquet(
            str(staged_silver_path)
        )

        LOGGER.info("Writing staged quarantine dataset")
        quarantine.write.mode("errorifexists").parquet(
            str(staged_quarantine_path)
        )

        # Verify the persisted Parquet outputs, not just in-memory frames.
        persisted_silver_rows = spark.read.parquet(
            str(staged_silver_path)
        ).count()
        persisted_quarantine_rows = spark.read.parquet(
            str(staged_quarantine_path)
        ).count()

        persisted_counts_match = (
            persisted_silver_rows == metrics["output_rows"]
            and persisted_quarantine_rows == metrics["quarantined_rows"]
        )

        reconciliation_passed = (
            metrics["input_rows"]
            == persisted_silver_rows + persisted_quarantine_rows
        )

        report.update(
            {
                "metrics": metrics,
                "persisted_silver_rows": persisted_silver_rows,
                "persisted_quarantine_rows": persisted_quarantine_rows,
                "persisted_counts_match": persisted_counts_match,
                "reconciliation_passed": reconciliation_passed,
            }
        )

        if not persisted_counts_match or not reconciliation_passed:
            raise RuntimeError(
                "Persisted row-count validation failed: "
                f"input={metrics['input_rows']}, "
                f"silver={persisted_silver_rows}, "
                f"quarantine={persisted_quarantine_rows}"
            )

        # Recheck before publishing in case an output appeared during the run.
        if SILVER_PATH.exists() or QUARANTINE_PATH.exists():
            raise FileExistsError(
                "An output path appeared during execution; "
                "refusing to overwrite existing data."
            )

        publish_directory(staged_silver_path, SILVER_PATH)
        publish_directory(staged_quarantine_path, QUARANTINE_PATH)

        report["status"] = "success"
        LOGGER.info("Silver outputs published successfully")

    except Exception as exc:
        report["status"] = "failed"
        report["error"] = str(exc)
        LOGGER.exception("Implied susceptibility Silver run failed")

    finally:
        report["completed_at"] = datetime.now(UTC).isoformat()

        if staging_path.exists():
            shutil.rmtree(staging_path, ignore_errors=True)

        if spark is not None:
            try:
                spark.stop()
            except Exception:
                LOGGER.exception("Failed to stop Spark session")

        try:
            write_json_atomic(
                REPORTS_PATH / f"{run_id}.json",
                report,
            )
        except Exception:
            LOGGER.exception("Failed to write the run report")
            return 1

    if report["status"] != "success":
        LOGGER.error(
            "Run %s failed. Report: %s",
            run_id,
            REPORTS_PATH / f"{run_id}.json",
        )
        return 1

    LOGGER.info(
        "Run %s completed. Silver rows: %s; quarantine rows: %s; "
        "report: %s",
        run_id,
        report["persisted_silver_rows"],
        report["persisted_quarantine_rows"],
        REPORTS_PATH / f"{run_id}.json",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
