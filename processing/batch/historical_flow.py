from __future__ import annotations

import argparse
import json
import logging
import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, StringType, StructField, StructType

from processing.batch.archive_manifest import validate_export_manifest
from processing.batch.parquet_archive import write_partitioned_snapshot
from processing.streaming.streaming_flow import (
    DEFAULT_HISTORY_COLLECTION,
    DEFAULT_MONGODB_DATABASE,
    DEFAULT_MONGODB_URI,
    MONGO_CONNECTOR_PACKAGE,
    RECORD_SCHEMA,
    build_feature_frame,
)


LOGGER = logging.getLogger("climate_historical_processor")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

EXPORT_SCHEMA = StructType(
    [
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", LongType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_timestamp_ms", LongType()),
        StructField("exported_at", StringType()),
        StructField("value", RECORD_SCHEMA),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process Kafka JSONL exports into durable MongoDB observations."
    )
    parser.add_argument(
        "--input",
        nargs="+",
        required=True,
        help="One or more Kafka JSONL export files containing kafka_topic and value fields.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Transform and report records without writing to MongoDB.",
    )
    parser.add_argument(
        "--manifest",
        help="Export manifest path; validates completeness and file integrity before processing.",
    )
    parser.add_argument(
        "--parquet-archive-root",
        help="Write immutable raw and curated Parquet snapshots under this directory.",
    )
    return parser.parse_args()


def transform_export_rows(export_rows: DataFrame) -> DataFrame:
    """Apply shared stateless features and compute historical observation windows."""
    observations = (
        build_feature_frame(export_rows)
        .filter("_valid_record")
        .drop("_valid_record")
        .dropDuplicates(["_id"])
    )
    return add_historical_window_features(observations)


def add_historical_window_features(observations: DataFrame) -> DataFrame:
    """Compute source-isolated elapsed-time windows over the complete batch input."""
    weather_window = Window.partitionBy("station_id", "source").orderBy("timestamp", "_id")
    weather_history = (
        Window.partitionBy("station_id", "source")
        .orderBy(F.col("_event_seconds"))
        .rangeBetween(-6 * 60 * 60, 0)
    )
    air_history = (
        Window.partitionBy("station_id", "source")
        .orderBy(F.col("_event_seconds"))
        .rangeBetween(-12 * 60 * 60, 0)
    )
    weather_observations = (
        observations.filter(F.col("temp_c").isNotNull())
        .withColumn("_event_seconds", F.col("timestamp").cast("long"))
    )
    weather_features = (
        weather_observations
        .withColumn("wind_shear_U", F.col("wind_U") - F.lag("wind_U", 1).over(weather_window))
        .withColumn("wind_shear_V", F.col("wind_V") - F.lag("wind_V", 1).over(weather_window))
        .withColumn("temp_mean_6h", F.avg("temp_c").over(weather_history))
        .select(
            "_id",
            "station_id",
            "source",
            "_event_seconds",
            F.col("pressure").alias("_current_pressure"),
            "wind_shear_U",
            "wind_shear_V",
            "temp_mean_6h",
        )
    )
    pressure_candidates = F.collect_list(
        F.when(
            F.col("pressure").isNotNull(),
            F.struct(
                F.col("_event_seconds").alias("timestamp"),
                F.col("_id").alias("id"),
                F.col("pressure").alias("pressure"),
            ),
        )
    ).over(
        Window.partitionBy("station_id", "source")
        .orderBy(F.col("_event_seconds"))
        .rangeBetween(-(3 * 60 * 60 + 30 * 60), -(3 * 60 * 60 - 30 * 60))
    )
    ordered_pressure_candidates = F.array_sort(
        F.transform(
            F.col("_pressure_candidates"),
            lambda candidate: F.struct(
                F.abs(
                    F.col("_event_seconds")
                    - candidate.getField("timestamp")
                    - F.lit(3 * 60 * 60)
                ).alias("distance"),
                candidate.getField("timestamp").alias("timestamp"),
                candidate.getField("id").alias("id"),
                candidate.getField("pressure").alias("pressure"),
            ),
        )
    )
    pressure_deltas = (
        weather_observations
        .withColumn("_pressure_candidates", pressure_candidates)
        .withColumn("_ordered_pressure_candidates", ordered_pressure_candidates)
        .select(
            "_id",
            (
                F.col("pressure")
                - F.element_at(F.col("_ordered_pressure_candidates"), 1).getField("pressure")
            ).alias("pressure_delta_3h"),
        )
    )
    air_features = (
        observations.filter(F.col("pm2_5").isNotNull())
        .withColumn("_event_seconds", F.col("timestamp").cast("long"))
        .withColumn("pm25_mean_12h", F.avg("pm2_5").over(air_history))
        .select("_id", "pm25_mean_12h")
    )
    return (
        observations.drop(
            "pressure_delta_3h",
            "wind_shear_U",
            "wind_shear_V",
            "temp_mean_6h",
            "pm25_mean_12h",
        )
        .join(
            weather_features.select("_id", "wind_shear_U", "wind_shear_V", "temp_mean_6h"),
            on="_id",
            how="left",
        )
        .join(pressure_deltas, on="_id", how="left")
        .join(air_features, on="_id", how="left")
    )


def write_historical_batch(observations: DataFrame, dry_run: bool = False) -> int:
    started = time.perf_counter()
    observations.persist()
    try:
        count = observations.count()
        if dry_run:
            LOGGER.info(
                "historical_batch_metrics %s",
                json.dumps(
                    {
                        "stage": "spark_transform",
                        "records": count,
                        "duration_seconds": round(time.perf_counter() - started, 3),
                        "dry_run": True,
                    },
                    sort_keys=True,
                ),
            )
            return count
        if count == 0:
            LOGGER.info("No valid observations found; MongoDB was not modified")
            return 0

        (
            observations.write.format("mongodb")
            .mode("append")
            .option("connection.uri", os.getenv("MONGODB_URI", DEFAULT_MONGODB_URI))
            .option("database", os.getenv("MONGODB_DATABASE", DEFAULT_MONGODB_DATABASE))
            .option(
                "collection",
                os.getenv("MONGODB_HISTORY_COLLECTION", DEFAULT_HISTORY_COLLECTION),
            )
            .option("operationType", "replace")
            .option("idFieldList", "_id")
            .option("upsertDocument", "true")
            .save()
        )
        duration = time.perf_counter() - started
        LOGGER.info("Upserted %s historical observations", count)
        LOGGER.info(
            "historical_batch_metrics %s",
            json.dumps(
                {
                    "stage": "spark_transform_to_mongo",
                    "records": count,
                    "duration_seconds": round(duration, 3),
                    "records_per_second": round(count / duration, 2) if duration else None,
                    "dry_run": False,
                },
                sort_keys=True,
            ),
        )
        return count
    finally:
        observations.unpersist()


def write_parquet_archives(
    spark: SparkSession,
    input_path: str,
    observations: DataFrame,
    archive_root: str,
    manifest: dict,
) -> None:
    archive_started = time.perf_counter()
    snapshot_id = manifest["sha256"]
    raw_lines = spark.read.text(input_path).select(F.col("value").alias("raw_json"))
    raw_records = (
        raw_lines
        .withColumn("_envelope", F.from_json("raw_json", EXPORT_SCHEMA))
        .withColumn("event_timestamp", F.to_timestamp(F.col("_envelope.value.event_time")))
        .withColumn("source", F.coalesce(F.col("_envelope.value.source"), F.lit("_unknown")))
        .withColumn(
            "event_date",
            F.coalesce(F.date_format("event_timestamp", "yyyy-MM-dd"), F.lit("unknown")),
        )
        .withColumn("year", F.substring("event_date", 1, 4))
        .withColumn("month", F.substring("event_date", 6, 2))
        .withColumn("day", F.substring("event_date", 9, 2))
        .select(
            "raw_json",
            F.col("_envelope.kafka_topic").alias("kafka_topic"),
            F.col("_envelope.kafka_partition").alias("kafka_partition"),
            F.col("_envelope.kafka_offset").alias("kafka_offset"),
            F.timestamp_millis(F.col("_envelope.kafka_timestamp_ms")).alias("kafka_timestamp"),
            "event_timestamp",
            "source",
            "year",
            "month",
            "day",
        )
    )

    curated_records = (
        observations
        .withColumn("year", F.date_format("timestamp", "yyyy"))
        .withColumn("month", F.date_format("timestamp", "MM"))
        .withColumn("day", F.date_format("timestamp", "dd"))
    )

    common_metadata = {
        "snapshot_id": snapshot_id,
        "source_file_sha256": manifest["sha256"],
        "exported_count": manifest["exported_count"],
        "topic_counts": manifest["topic_counts"],
        "partitions": manifest["partitions"],
    }

    write_partitioned_snapshot(
        raw_records,
        archive_root=archive_root,
        dataset_name="raw_kafka",
        snapshot_id=snapshot_id,
        source_manifest=common_metadata,
        record_count=manifest["exported_count"],
    )
    observations.persist()
    try:
        curated_count = observations.count()
        write_partitioned_snapshot(
            curated_records,
            archive_root=archive_root,
            dataset_name="curated_observations",
            snapshot_id=snapshot_id,
            source_manifest=common_metadata,
            record_count=curated_count,
        )
    finally:
        observations.unpersist()
    duration = time.perf_counter() - archive_started
    LOGGER.info(
        "historical_batch_metrics %s",
        json.dumps(
            {
                "stage": "spark_partitioned_parquet",
                "snapshot_id": snapshot_id,
                "raw_records": manifest["exported_count"],
                "curated_records": curated_count,
                "duration_seconds": round(duration, 3),
                "curated_records_per_second": round(curated_count / duration, 2) if duration else None,
            },
            sort_keys=True,
        ),
    )


def main() -> None:
    args = parse_args()
    manifest = None
    if args.manifest:
        if len(args.input) != 1:
            raise ValueError("--manifest can only validate a single input file")
        manifest = validate_export_manifest(args.input[0], args.manifest)
        LOGGER.info(
            "Validated complete Kafka export: %s records across %s topics",
            manifest["exported_count"],
            len(manifest["topics"]),
        )
    else:
        LOGGER.warning(
            "Processing an export without a manifest; completeness and file integrity cannot be verified"
        )
    if args.parquet_archive_root and not manifest:
        raise ValueError("--parquet-archive-root requires a validated --manifest")
    spark = (
        SparkSession.builder.appName("ClimateHistoricalBatch")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
        .config("spark.jars.packages", MONGO_CONNECTOR_PACKAGE)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        LOGGER.info("Reading Kafka export files: %s", args.input)
        export_rows = spark.read.schema(EXPORT_SCHEMA).json(args.input)
        observations = transform_export_rows(export_rows)
        if args.dry_run:
            write_historical_batch(observations, dry_run=True)
            return
        if args.parquet_archive_root and manifest:
            write_parquet_archives(
                spark,
                args.input[0],
                observations,
                args.parquet_archive_root,
                manifest,
            )
        write_historical_batch(observations, dry_run=args.dry_run)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
