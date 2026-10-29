"""Transform the Bronze culture cohort into Silver."""

import json
import logging
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession

from amr_pipeline.config import (
    BRONZE_DIR,
    PROJECT_ROOT,
    SILVER_DIR,
    SILVER_QUARANTINE_DIR,
    SILVER_RUNS_DIR,
)
from amr_pipeline.logging_config import configure_logging
from amr_pipeline.spark import create_spark_session
from amr_pipeline.transformations.silver.cohort import transform_cohort

logger = logging.getLogger(__name__)

SOURCE_PATH = BRONZE_DIR / "parquet" / "microbiology_cultures_cohort"
OUTPUT_PATH = SILVER_DIR / "cohort"
QUARANTINE_PATH = SILVER_QUARANTINE_DIR / "cohort"
STAGING_ROOT = SILVER_DIR / "_staging"


def write_json_report(path: Path, report: dict[str, object]) -> None:
    """Write a JSON report without overwriting an existing report."""
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("x", encoding="utf-8") as output:
        json.dump(report, output, indent=2)
        output.write("\n")


def run_transformation(spark: SparkSession) -> dict[str, object]:
    """Transform, validate, stage, and publish the culture cohort."""
    run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC)

    staging_path = STAGING_ROOT / run_id
    staged_silver_path = staging_path / "cohort"
    staged_quarantine_path = staging_path / "quarantine"

    if not SOURCE_PATH.is_dir():
        raise FileNotFoundError(f"Bronze input not found: {SOURCE_PATH}")

    for output_path in (OUTPUT_PATH, QUARANTINE_PATH):
        if output_path.exists():
            raise FileExistsError(
                f"Output already exists: {output_path}. "
                "Inspect it before rerunning; this runner never overwrites."
            )

    if staging_path.exists():
        raise FileExistsError(f"Run staging directory already exists: {staging_path}")

    logger.info("Reading Bronze cohort from %s", SOURCE_PATH)
    source = spark.read.parquet(str(SOURCE_PATH))

    silver, quarantine, metrics = transform_cohort(source)

    input_rows = metrics["input_rows"]
    output_rows = metrics["output_rows"]
    quarantined_rows = metrics["quarantined_rows"]

    if input_rows != output_rows + quarantined_rows:
        raise RuntimeError(
            "Row reconciliation failed: "
            f"input={input_rows}, output={output_rows}, "
            f"quarantined={quarantined_rows}"
        )

    # Each run writes to its own directory. Failed staging data is preserved
    # for inspection; this runner does not automatically delete directories.
    staged_silver_path.parent.mkdir(parents=True, exist_ok=False)

    logger.info("Writing staged Silver cohort to %s", staged_silver_path)
    silver.write.mode("errorifexists").parquet(str(staged_silver_path))

    logger.info("Writing staged quarantine records to %s", staged_quarantine_path)
    quarantine.write.mode("errorifexists").parquet(str(staged_quarantine_path))

    # Verify persisted counts before publishing either dataset.
    silver_written = spark.read.parquet(str(staged_silver_path)).count()
    quarantine_written = spark.read.parquet(str(staged_quarantine_path)).count()

    if silver_written != output_rows:
        raise RuntimeError(
            f"Staged Silver write mismatch: expected {output_rows}, "
            f"found {silver_written}"
        )

    if quarantine_written != quarantined_rows:
        raise RuntimeError(
            f"Staged quarantine write mismatch: expected {quarantined_rows}, "
            f"found {quarantine_written}"
        )

    if input_rows != silver_written + quarantine_written:
        raise RuntimeError("Staged row reconciliation failed.")

    # Recheck destinations immediately before publishing.
    for output_path in (OUTPUT_PATH, QUARANTINE_PATH):
        if output_path.exists():
            raise FileExistsError(
                f"Output appeared during this run: {output_path}. "
                f"Validated staging data remains at {staging_path}"
            )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    QUARANTINE_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Publish only validated datasets. Track successful moves so failures
    # leave the data in place for manual recovery rather than deleting it.
    published_paths: list[Path] = []

    try:
        staged_silver_path.rename(OUTPUT_PATH)
        published_paths.append(OUTPUT_PATH)

        staged_quarantine_path.rename(QUARANTINE_PATH)
        published_paths.append(QUARANTINE_PATH)
    except Exception:
        logger.exception(
            "Publishing failed. Do not delete data manually without inspecting "
            "these paths. Published=%s, staging=%s",
            published_paths,
            staging_path,
        )
        raise

    completed_at = datetime.now(UTC)
    report: dict[str, object] = {
        "run_id": run_id,
        "transformation": "silver_culture_cohort",
        "status": "success",
        "started_at_utc": started_at.isoformat(),
        "completed_at_utc": completed_at.isoformat(),
        "source_path": str(SOURCE_PATH.relative_to(PROJECT_ROOT)),
        "silver_path": str(OUTPUT_PATH.relative_to(PROJECT_ROOT)),
        "quarantine_path": str(QUARANTINE_PATH.relative_to(PROJECT_ROOT)),
        "staging_path": str(staging_path.relative_to(PROJECT_ROOT)),
        "metrics": metrics,
        "persisted_silver_rows": silver_written,
        "persisted_quarantined_rows": quarantine_written,
        "reconciliation_passed": True,
    }

    report_path = SILVER_RUNS_DIR / f"{run_id}.json"
    write_json_report(report_path, report)
    report["report_path"] = str(report_path.relative_to(PROJECT_ROOT))

    logger.info(
        "Silver cohort complete: input=%d silver=%d quarantined=%d",
        input_rows,
        silver_written,
        quarantine_written,
    )
    logger.info("Run report written to %s", report_path)

    return report


def main() -> int:
    """Run the Silver cohort transformation."""
    configure_logging()
    spark = create_spark_session("AMR-Silver-Cohort")

    try:
        run_transformation(spark)
    except Exception:
        logger.exception("Silver cohort transformation failed")
        return 1
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
