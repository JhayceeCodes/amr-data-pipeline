import json
import logging
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path

from amr_pipeline.spark import create_spark_session
from amr_pipeline.transformations.silver.antibiotic_class_exposure import (
    transform_antibiotic_class_exposure,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SOURCE_PATH = (
    PROJECT_ROOT
    / "data/bronze/parquet/microbiology_cultures_antibiotic_class_exposure"
)

SILVER_PATH = PROJECT_ROOT / "data/silver/antibiotic_class_exposure"
QUARANTINE_PATH = (
    PROJECT_ROOT / "data/silver/quarantine/antibiotic_class_exposure"
)
STAGING_ROOT = PROJECT_ROOT / "data/silver/_staging"
RUNS_PATH = PROJECT_ROOT / "data/silver/runs"

logger = logging.getLogger(__name__)


def write_json_atomic(path: Path, payload: dict) -> None:
    """Write a JSON report through a temporary file."""
    path.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = path.with_suffix(".tmp")

    with temporary_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, default=str)
        file.write("\n")

    temporary_path.replace(path)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not SOURCE_PATH.exists():
        raise FileNotFoundError(
            f"Bronze dataset not found: {SOURCE_PATH}"
        )

    # Prevent accidental replacement of an existing published dataset.
    if SILVER_PATH.exists() or QUARANTINE_PATH.exists():
        raise FileExistsError(
            "Silver or quarantine output already exists. "
            "Inspect the existing outputs before rerunning."
        )

    run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC)

    staging_run = STAGING_ROOT / run_id
    staged_silver = staging_run / "antibiotic_class_exposure"
    staged_quarantine = staging_run / "quarantine"

    published_silver = False
    published_quarantine = False

    spark = create_spark_session("AMR-Silver-Antibiotic-Class-Exposure")

    try:
        logger.info("Starting Silver transformation; run_id=%s", run_id)
        logger.info("Reading Bronze dataset from %s", SOURCE_PATH)

        source = spark.read.parquet(str(SOURCE_PATH))

        silver, quarantine, metrics = (
            transform_antibiotic_class_exposure(source)
        )

        staging_run.mkdir(parents=True, exist_ok=False)

        silver.write.mode("errorifexists").parquet(str(staged_silver))
        quarantine.write.mode("errorifexists").parquet(
            str(staged_quarantine)
        )

        # Validate persisted output rather than relying only on in-memory
        # counts.
        persisted_silver_count = spark.read.parquet(
            str(staged_silver)
        ).count()

        persisted_quarantine_count = spark.read.parquet(
            str(staged_quarantine)
        ).count()

        reconciliation_passed = (
            metrics["input_rows"]
            == persisted_silver_count + persisted_quarantine_count
            and persisted_silver_count == metrics["output_rows"]
            and persisted_quarantine_count == metrics["quarantined_rows"]
        )

        if not reconciliation_passed:
            raise RuntimeError(
                "Row-count reconciliation failed. "
                f"Metrics={metrics}, "
                f"persisted_silver={persisted_silver_count}, "
                f"persisted_quarantine={persisted_quarantine_count}"
            )

        # Recheck immediately before publishing.
        if SILVER_PATH.exists() or QUARANTINE_PATH.exists():
            raise FileExistsError(
                "An output destination appeared before publication."
            )

        SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
        QUARANTINE_PATH.parent.mkdir(parents=True, exist_ok=True)

        # These renames are separate operations, not one atomic transaction.
        staged_silver.rename(SILVER_PATH)
        published_silver = True

        staged_quarantine.rename(QUARANTINE_PATH)
        published_quarantine = True

        completed_at = datetime.now(UTC)

        report = {
            "run_id": run_id,
            "dataset": "antibiotic_class_exposure",
            "status": "success",
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "source_path": str(SOURCE_PATH),
            "silver_path": str(SILVER_PATH),
            "quarantine_path": str(QUARANTINE_PATH),
            "persisted_silver_rows": persisted_silver_count,
            "persisted_quarantine_rows": persisted_quarantine_count,
            "reconciliation_passed": reconciliation_passed,
            "metrics": metrics,
        }

        write_json_atomic(RUNS_PATH / f"{run_id}.json", report)

        logger.info("Silver transformation completed successfully.")
        logger.info("Run ID: %s", run_id)
        logger.info("Metrics: %s", json.dumps(metrics, default=str))

    except Exception:
        logger.exception(
            "Silver transformation failed; run_id=%s",
            run_id,
        )

        # Remove only unpublished staging data. If one destination was
        # published before a later step failed, preserve it for inspection.
        if not published_silver and not published_quarantine:
            shutil.rmtree(staging_run, ignore_errors=True)

        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()