"""Run the Silver antibiotic subtype exposure transformation."""

import json
import logging
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession

from amr_pipeline.spark import create_spark_session
from amr_pipeline.transformations.silver.antibiotic_subtype_exposure import (
    transform_antibiotic_subtype_exposure,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "parquet"
    / "microbiology_cultures_antibiotic_subtype_exposure"
)

SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "antibiotic_subtype_exposure"

QUARANTINE_PATH = (
    PROJECT_ROOT / "data" / "silver" / "quarantine" / "antibiotic_subtype_exposure"
)

RUNS_PATH = (
    PROJECT_ROOT / "data" / "silver" / "runs"
)

logger = logging.getLogger(__name__)


def write_json_atomically(path: Path, payload: dict) -> None:
    """Write JSON to a temporary file before publishing it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(".tmp")

    with temporary_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, sort_keys=True)
        file.write("\n")

    temporary_path.replace(path)


def publish_staged_output(
    spark: SparkSession,
    staging_path: Path,
    final_path: Path,
    expected_rows: int,
) -> None:
    """Validate a staged Parquet dataset and publish it if the target is absent."""
    if final_path.exists():
        raise FileExistsError(f"Refusing to overwrite existing output: {final_path}")

    persisted = spark.read.parquet(str(staging_path))
    actual_rows = persisted.count()

    if actual_rows != expected_rows:
        raise RuntimeError(
            f"Persisted row-count mismatch for {staging_path}: "
            f"expected={expected_rows}, actual={actual_rows}"
        )

    staging_path.rename(final_path)


def main() -> int:
    run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC).isoformat()

    staging_root = PROJECT_ROOT / "data" / "silver" / "_staging" / run_id
    staged_silver = staging_root / "antibiotic_subtype_exposure"
    staged_quarantine = staging_root / "quarantine"

    report = {
        "run_id": run_id,
        "transformation": "silver_antibiotic_subtype_exposure",
        "status": "failed",
        "started_at": started_at,
    }

    spark = None

    try:
        if not BRONZE_PATH.exists():
            raise FileNotFoundError(
                f"Bronze Parquet input does not exist: {BRONZE_PATH}"
            )

        # Create a Spark session using the project's shared configuration.
        spark = create_spark_session("Silver-Antibiotic-Subtype-Exposure")

        logger.info("Run ID: %s", run_id)
        logger.info("Reading Bronze input: %s", BRONZE_PATH)

        source = spark.read.parquet(str(BRONZE_PATH))
        silver, quarantine, metrics = transform_antibiotic_subtype_exposure(source)

        # Persist each output under a unique staging directory.
        staged_silver.parent.mkdir(parents=True, exist_ok=True)
        silver.write.mode("errorifexists").parquet(str(staged_silver))
        quarantine.write.mode("errorifexists").parquet(str(staged_quarantine))

        # Verify persisted output counts before publishing either dataset.
        silver_persisted_count = spark.read.parquet(str(staged_silver)).count()
        quarantine_persisted_count = spark.read.parquet(str(staged_quarantine)).count()

        if silver_persisted_count != metrics["output_rows"]:
            raise RuntimeError(
                "Silver persisted row-count mismatch: "
                f"expected={metrics['output_rows']}, "
                f"actual={silver_persisted_count}"
            )

        if quarantine_persisted_count != metrics["quarantined_rows"]:
            raise RuntimeError(
                "Quarantine persisted row-count mismatch: "
                f"expected={metrics['quarantined_rows']}, "
                f"actual={quarantine_persisted_count}"
            )

        if metrics["input_rows"] != (
            metrics["output_rows"] + metrics["quarantined_rows"]
        ):
            raise RuntimeError("Input/output/quarantine reconciliation failed.")

        # Publish without overwriting an earlier successful run.
        SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
        QUARANTINE_PATH.parent.mkdir(parents=True, exist_ok=True)

        publish_staged_output(
            spark,
            staged_silver,
            SILVER_PATH,
            metrics["output_rows"],
        )
        publish_staged_output(
            spark,
            staged_quarantine,
            QUARANTINE_PATH,
            metrics["quarantined_rows"],
        )

        report.update(
            {
                "status": "success",
                "finished_at": datetime.now(UTC).isoformat(),
                "source_path": str(BRONZE_PATH.relative_to(PROJECT_ROOT)),
                "silver_path": str(SILVER_PATH.relative_to(PROJECT_ROOT)),
                "quarantine_path": str(QUARANTINE_PATH.relative_to(PROJECT_ROOT)),
                "metrics": metrics,
                "persisted_silver_rows": silver_persisted_count,
                "persisted_quarantine_rows": quarantine_persisted_count,
                "reconciliation_passed": True,
            }
        )

        logger.info("Silver transformation completed successfully.")
        logger.info("Metrics: %s", json.dumps(metrics, sort_keys=True))
        return 0

    except Exception:
        logger.exception("Silver antibiotic subtype exposure transformation failed.")
        report["finished_at"] = datetime.now(UTC).isoformat()
        report["error"] = "Transformation failed; see execution logs."
        return 1

    finally:
        try:
            write_json_atomically(
                RUNS_PATH / f"{run_id}.json",
                report,
            )
        except Exception:
            logger.exception("Failed to write the run report.")
            # If the run otherwise succeeded, report-write failure must
            # still be visible as a failed process exit.
            if report["status"] == "success":
                report["status"] = "failed"
                report["error"] = "Run report could not be written."

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    sys.exit(main())
