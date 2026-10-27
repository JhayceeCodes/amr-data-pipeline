"""Bronze ingestion for source CSV snapshots."""

import csv
import hashlib
import json
import logging
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql.types import StringType, StructField, StructType

from amr_pipeline.config import PROJECT_ROOT
from amr_pipeline.spark import create_spark_session

logger = logging.getLogger(__name__)


def calculate_sha256(path: Path) -> str:
    """Return the SHA-256 checksum of a file."""
    digest = hashlib.sha256()

    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def count_csv_rows(path: Path) -> int:
    """Count CSV data rows, excluding the header."""
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        next(reader, None)
        return sum(1 for _ in reader)


def relative_or_absolute_path(path: Path) -> str:
    """Return a project-relative path when possible."""
    try:
        return path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path)


def write_bronze_parquet(
    snapshot_path: Path,
    parquet_path: Path,
    expected_rows: int,
) -> int:
    """Write a string-typed Parquet representation of a CSV snapshot."""
    with snapshot_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        columns = next(reader, None)

    if not columns:
        raise ValueError(f"CSV has no header: {snapshot_path}")

    if len(columns) != len(set(columns)):
        raise ValueError("CSV contains duplicate column names.")

    schema = StructType(
        [StructField(column, StringType(), nullable=True) for column in columns]
    )

    spark = create_spark_session("AMR-Bronze-Ingestion")

    try:
        dataframe = (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .option("mode", "FAILFAST")
            .schema(schema)
            .csv(str(snapshot_path))
        )

        actual_rows = dataframe.count()
        if actual_rows != expected_rows:
            raise ValueError(
                "CSV row count differs between Python and Spark: "
                f"{expected_rows} != {actual_rows}"
            )

        dataframe.write.mode("errorifexists").parquet(str(parquet_path))

        parquet_rows = spark.read.parquet(str(parquet_path)).count()
        if parquet_rows != expected_rows:
            raise OSError(
                "Bronze Parquet row count mismatch: "
                f"expected {expected_rows}, got {parquet_rows}"
            )

        return parquet_rows
    finally:
        spark.stop()


def ingest_csv_to_bronze(
    source_path: Path,
    bronze_dir: Path | None = None,
) -> dict[str, object]:
    """Preserve a CSV snapshot and create a Bronze Parquet representation.

    Existing snapshots are reused only when their checksums and manifest
    metadata match the source. Existing Parquet output is never overwritten.
    """
    source_path = source_path.resolve()

    if not source_path.is_file():
        raise FileNotFoundError(f"Source CSV not found: {source_path}")

    if source_path.suffix.lower() != ".csv":
        raise ValueError(f"Expected a CSV source, got: {source_path.name}")

    bronze_dir = (bronze_dir or PROJECT_ROOT / "data" / "bronze").resolve()

    raw_dir = bronze_dir / "raw"
    parquet_root = bronze_dir / "parquet"
    manifests_dir = bronze_dir / "manifests"

    raw_dir.mkdir(parents=True, exist_ok=True)
    parquet_root.mkdir(parents=True, exist_ok=True)
    manifests_dir.mkdir(parents=True, exist_ok=True)

    snapshot_path = raw_dir / source_path.name
    parquet_path = parquet_root / source_path.stem
    manifest_path = manifests_dir / f"{source_path.stem}.manifest.json"

    source_checksum = calculate_sha256(source_path)
    source_row_count = count_csv_rows(source_path)
    source_size_bytes = source_path.stat().st_size

    snapshot_exists = snapshot_path.exists()
    manifest_exists = manifest_path.exists()

    if snapshot_exists != manifest_exists:
        raise FileExistsError(
            "Only one of the Bronze snapshot and manifest exists. "
            "Inspect the existing artifacts before retrying."
        )

    if snapshot_exists:
        if calculate_sha256(snapshot_path) != source_checksum:
            raise ValueError(
                "Existing Bronze snapshot differs from the source. "
                "It will not be overwritten."
            )

        if count_csv_rows(snapshot_path) != source_row_count:
            raise ValueError("Existing snapshot row count differs from source.")

        with manifest_path.open(encoding="utf-8") as source:
            manifest = json.load(source)

        if manifest.get("source_sha256") != source_checksum:
            raise ValueError("Existing manifest checksum does not match source.")

        if manifest.get("snapshot_sha256") != source_checksum:
            raise ValueError("Manifest snapshot checksum does not match source.")

        if manifest.get("source_row_count") != source_row_count:
            raise ValueError("Manifest source row count does not match source.")

        if manifest.get("snapshot_row_count") != source_row_count:
            raise ValueError("Manifest snapshot row count does not match source.")

        if manifest.get("snapshot_path") != relative_or_absolute_path(snapshot_path):
            raise ValueError("Manifest snapshot path does not match expected path.")

    else:
        shutil.copy2(source_path, snapshot_path)

        if calculate_sha256(snapshot_path) != source_checksum:
            snapshot_path.unlink(missing_ok=True)
            raise OSError("Bronze snapshot checksum does not match source.")

        manifest = {
            "source_filename": source_path.name,
            "source_path": relative_or_absolute_path(source_path),
            "snapshot_filename": snapshot_path.name,
            "snapshot_path": relative_or_absolute_path(snapshot_path),
            "ingestion_timestamp_utc": datetime.now(UTC).isoformat(),
            "pipeline_run_id": str(uuid.uuid4()),
            "source_sha256": source_checksum,
            "snapshot_sha256": calculate_sha256(snapshot_path),
            "source_size_bytes": source_size_bytes,
            "source_row_count": source_row_count,
            "snapshot_row_count": count_csv_rows(snapshot_path),
            "ingestion_status": "in_progress",
        }

    parquet_created = not parquet_path.exists()

    try:
        if parquet_created:
            parquet_rows = write_bronze_parquet(
                snapshot_path,
                parquet_path,
                source_row_count,
            )
        else:
            spark = create_spark_session("AMR-Bronze-Validation")
            try:
                parquet_rows = spark.read.parquet(str(parquet_path)).count()
            finally:
                spark.stop()

            if parquet_rows != source_row_count:
                raise ValueError(
                    "Existing Parquet row count does not match source: "
                    f"{parquet_rows} != {source_row_count}"
                )

        manifest.update(
            {
                "parquet_path": relative_or_absolute_path(parquet_path),
                "parquet_row_count": parquet_rows,
                "ingestion_status": "success",
            }
        )

        temporary_manifest = manifest_path.with_name(f".{manifest_path.name}.tmp")

        try:
            with temporary_manifest.open("w", encoding="utf-8") as output:
                json.dump(manifest, output, indent=2)
                output.write("\n")

            temporary_manifest.replace(manifest_path)
        finally:
            temporary_manifest.unlink(missing_ok=True)

    except Exception:
        if parquet_created:
            shutil.rmtree(parquet_path, ignore_errors=True)

        if not snapshot_exists:
            snapshot_path.unlink(missing_ok=True)

        raise

    logger.info(
        "Bronze ingestion complete: source=%s rows=%d parquet_rows=%d",
        source_path.name,
        source_row_count,
        parquet_rows,
    )

    return manifest
