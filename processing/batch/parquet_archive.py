from __future__ import annotations

import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame


PARTITION_COLUMNS = ("year", "month", "day", "source")


def write_partitioned_snapshot(
    records: DataFrame,
    archive_root: str,
    dataset_name: str,
    snapshot_id: str,
    source_manifest: dict,
    record_count: int,
) -> bool:
    """Atomically write an immutable, replay-safe Parquet snapshot."""
    snapshot_directory = Path(archive_root) / dataset_name / snapshot_id
    metadata_path = snapshot_directory / "archive-manifest.json"
    if snapshot_directory.exists():
        if not metadata_path.is_file():
            raise FileExistsError(f"Existing archive snapshot has no manifest: {snapshot_directory}")
        with metadata_path.open(encoding="utf-8") as handle:
            existing_metadata = json.load(handle)
        if (
            existing_metadata.get("source_manifest") != source_manifest
            or existing_metadata.get("record_count") != record_count
            or not (snapshot_directory / "data").is_dir()
        ):
            raise ValueError(f"Existing archive snapshot conflicts with this input: {snapshot_directory}")
        return False

    snapshot_directory.parent.mkdir(parents=True, exist_ok=True)
    temporary_directory = Path(
        tempfile.mkdtemp(prefix=f".{snapshot_id}.", dir=snapshot_directory.parent)
    )
    try:
        (
            records.write
            .mode("errorifexists")
            .partitionBy(*PARTITION_COLUMNS)
            .parquet(str(temporary_directory / "data"))
        )
        metadata = {
            "dataset": dataset_name,
            "snapshot_id": snapshot_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "record_count": record_count,
            "partition_columns": list(PARTITION_COLUMNS),
            "format": "parquet",
            "source_manifest": source_manifest,
        }
        with (temporary_directory / "archive-manifest.json").open(
            "w", encoding="utf-8"
        ) as handle:
            json.dump(metadata, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
        os.rename(temporary_directory, snapshot_directory)
    except Exception:
        if temporary_directory.exists():
            shutil.rmtree(temporary_directory)
        raise
    return True
