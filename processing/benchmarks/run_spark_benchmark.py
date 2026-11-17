from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession

from processing.batch.archive_manifest import validate_export_manifest
from processing.batch.historical_flow import (
    EXPORT_SCHEMA,
    transform_export_rows,
    write_parquet_archives,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure Spark transform, Parquet, optional Mongo, and optional API stages."
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--archive-root", required=True)
    parser.add_argument("--report-dir", required=True)
    parser.add_argument("--write-mongodb", action="store_true")
    parser.add_argument("--api-url")
    parser.add_argument("--api-date")
    parser.add_argument("--api-requests", type=int, default=20)
    return parser.parse_args()


def parquet_file_stats(directory: Path) -> tuple[int, int]:
    parquet_files = list(directory.rglob("*.parquet"))
    return len(parquet_files), sum(path.stat().st_size for path in parquet_files)


def benchmark_history_api(base_url: str, date: str, request_count: int) -> dict:
    encoded_date = urllib.parse.urlencode({"startDate": date, "endDate": date, "limit": 100})
    url = f"{base_url.rstrip('/')}/api/history/hanoi?{encoded_date}"
    durations = []
    errors = []
    returned_records = 0
    for _ in range(request_count):
        started = time.perf_counter()
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                body = json.loads(response.read().decode("utf-8"))
                durations.append(time.perf_counter() - started)
                returned_records = len(body.get("records", []))
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as error:
            errors.append(str(error))
            break
    if not durations:
        return {
            "url": url,
            "requests": request_count,
            "successful_requests": 0,
            "errors": errors[:3],
        }
    sorted_durations = sorted(durations)
    percentile_index = lambda percentile: min(
        len(sorted_durations) - 1,
        max(0, int((len(sorted_durations) * percentile + 0.999999)) - 1),
    )
    return {
        "url": url,
        "requests": request_count,
        "successful_requests": len(durations),
        "returned_records_in_last_response": returned_records,
        "p50_seconds": round(statistics.median(durations), 4),
        "p95_seconds": round(sorted_durations[percentile_index(0.95)], 4),
        "errors": errors[:3],
    }


def main() -> None:
    args = parse_args()
    wall_started = time.perf_counter()
    validation_started = time.perf_counter()
    manifest = validate_export_manifest(args.input, args.manifest)
    validation_seconds = time.perf_counter() - validation_started
    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"

    spark_started = time.perf_counter()
    spark = (
        SparkSession.builder
        .master(os.getenv("SPARK_MASTER", "local[1]"))
        .appName("ClimateSyntheticScaleBenchmark")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark_startup_seconds = time.perf_counter() - spark_started

    input_path = Path(args.input)
    report_path = Path(args.report_dir) / f"spark_benchmark_{manifest['exported_count']}_{run_id}.json"
    archive_path = Path(args.archive_root) / run_id
    report_path.parent.mkdir(parents=True, exist_ok=True)
    transform_seconds = None
    parquet_seconds = None
    readback_seconds = None
    mongo_seconds = None
    result_count = 0
    raw_readback_count = 0
    curated_readback_count = 0

    try:
        read_started = time.perf_counter()
        export_rows = spark.read.schema(EXPORT_SCHEMA).json(str(input_path))
        observations = transform_export_rows(export_rows).persist()
        result_count = observations.count()
        transform_seconds = time.perf_counter() - read_started

        parquet_started = time.perf_counter()
        write_parquet_archives(
            spark,
            str(input_path),
            observations,
            str(archive_path),
            manifest,
        )
        parquet_seconds = time.perf_counter() - parquet_started

        readback_started = time.perf_counter()
        raw_data = next((archive_path / "raw_kafka").glob("*/data"))
        curated_data = next((archive_path / "curated_observations").glob("*/data"))
        raw_readback_count = spark.read.parquet(str(raw_data)).count()
        curated_readback_count = spark.read.parquet(str(curated_data)).count()
        if raw_readback_count != manifest["exported_count"]:
            raise AssertionError("Raw Parquet read-back count does not match the export manifest")
        if curated_readback_count != result_count:
            raise AssertionError("Curated Parquet read-back count does not match the transform count")
        readback_seconds = time.perf_counter() - readback_started

        if args.write_mongodb and result_count:
            mongo_started = time.perf_counter()
            (
                observations.write.format("mongodb")
                .mode("append")
                .option("connection.uri", os.getenv("MONGODB_URI", "mongodb://mongodb:27017"))
                .option("database", os.getenv("BENCHMARK_MONGODB_DATABASE", "climate_benchmark"))
                .option("collection", "synthetic_observations")
                .option("operationType", "replace")
                .option("idFieldList", "_id")
                .option("upsertDocument", "true")
                .save()
            )
            mongo_seconds = time.perf_counter() - mongo_started

        observations.unpersist()
        raw_file_count, raw_bytes = parquet_file_stats(archive_path / "raw_kafka")
        curated_file_count, curated_bytes = parquet_file_stats(archive_path / "curated_observations")
        processing_seconds = transform_seconds + parquet_seconds + readback_seconds
        api_sample = None
        if args.api_url:
            api_date = args.api_date or datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date().isoformat()
            api_sample = benchmark_history_api(args.api_url, api_date, args.api_requests)
        report = {
            "benchmark_version": 1,
            "run_id": run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "dataset": "deterministic_synthetic_kafka_export",
            "machine": {
                "platform": os.getenv("BENCHMARK_PLATFORM", platform.platform()),
                "cpu_count": os.cpu_count(),
                "spark_master": spark.sparkContext.master,
                "spark_version": spark.version,
                "spark_shuffle_partitions": os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"),
            },
            "input": {
                "path": str(input_path),
                "sha256": manifest["sha256"],
                "bytes": manifest["output_size_bytes"],
                "records": manifest["exported_count"],
                "topic_counts": manifest["topic_counts"],
            },
            "results": {
                "curated_records": result_count,
                "raw_readback_records": raw_readback_count,
                "curated_readback_records": curated_readback_count,
                "spark_startup_seconds": round(spark_startup_seconds, 3),
                "manifest_validation_seconds": round(validation_seconds, 3),
                "transform_and_count_seconds": round(transform_seconds, 3),
                "parquet_write_seconds": round(parquet_seconds, 3),
                "parquet_readback_seconds": round(readback_seconds, 3),
                "mongo_upsert_seconds": round(mongo_seconds, 3) if mongo_seconds is not None else None,
                "api_history_sample": api_sample,
                "processing_records_per_second": round(result_count / processing_seconds, 2) if processing_seconds else None,
                "raw_parquet_file_count": raw_file_count,
                "raw_parquet_bytes": raw_bytes,
                "curated_parquet_file_count": curated_file_count,
                "curated_parquet_bytes": curated_bytes,
                "total_seconds": round(time.perf_counter() - wall_started, 3),
            },
            "scope_notes": [
                "Kafka records are synthetic export envelopes; this run measures Spark batch and Parquet, not Kafka broker throughput.",
                "Mongo measurements are isolated in the climate_benchmark.synthetic_observations collection when enabled.",
                "API measurements are a separately labeled serving-query sample, not a synthetic-load test.",
            ],
        }
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(json.dumps({"report": str(report_path), **report["results"]}, indent=2, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
