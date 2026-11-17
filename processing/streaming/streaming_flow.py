from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from Ingestion.utils.locations import OPEN_METEO_LOCATIONS


ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

LOGGER = logging.getLogger("climate_stream_processor")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DEFAULT_TOPICS = "weather.raw.stream,air_quality.raw.stream"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_MONGODB_URI = "mongodb://localhost:27017"
DEFAULT_MONGODB_DATABASE = "climate_db"
DEFAULT_LIVE_COLLECTION = "realtime_observations"
DEFAULT_HISTORY_COLLECTION = "climate_observations"
DEFAULT_CHECKPOINT_DIR = str(ROOT_DIR / "checkpoint" / "processing_stream")
MONGO_CONNECTOR_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"
SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
    f"{MONGO_CONNECTOR_PACKAGE}"
)
CITY_LOCATIONS = [
    (str(location["city"]), float(location["latitude"]), float(location["longitude"]))
    for location in OPEN_METEO_LOCATIONS
]
CITY_ALIASES = {
    str(name).strip().lower(): city
    for location in OPEN_METEO_LOCATIONS
    for city in [str(location["city"])]
    for name in (city, location.get("alias", city))
}
CITY_ALIASES["ha noi"] = "Hanoi"


def canonical_city(city: str | None, latitude: float | None, longitude: float | None) -> str | None:
    if city:
        canonical = CITY_ALIASES.get(city.strip().lower())
        if canonical:
            return canonical

    if latitude is None or longitude is None:
        return city.strip() if city and city.strip() else None

    nearest_city, nearest_latitude, nearest_longitude = min(
        CITY_LOCATIONS,
        key=lambda location: (location[1] - latitude) ** 2 + (location[2] - longitude) ** 2,
    )
    distance_squared = (nearest_latitude - latitude) ** 2 + (nearest_longitude - longitude) ** 2
    if distance_squared <= 1.0:
        return nearest_city
    return city.strip() if city and city.strip() else None


canonical_city_udf = F.udf(canonical_city, StringType())

RECORD_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("source", StringType()),
        StructField("region", StringType()),
        StructField("city", StringType()),
        StructField("station_id", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("event_time", StringType()),
        StructField("ingestion_time", StringType()),
        StructField("temperature_c", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("pressure_hpa", DoubleType()),
        StructField("surface_pressure_hpa", DoubleType()),
        StructField("wind_speed_mps", DoubleType()),
        StructField("wind_direction_deg", DoubleType()),
        StructField("precipitation_mm", DoubleType()),
        StructField("cloud_cover_pct", DoubleType()),
        StructField("shortwave_radiation_wm2", DoubleType()),
        StructField("soil_temperature_0_to_7cm_c", DoubleType()),
        StructField("weather_main", StringType()),
        StructField("weather_desc", StringType()),
        StructField("aqi", DoubleType()),
        StructField("quality_level", StringType()),
        StructField("pm25", DoubleType()),
        StructField("pm10", DoubleType()),
        StructField("co", DoubleType()),
        StructField("no2", DoubleType()),
        StructField("so2", DoubleType()),
        StructField("o3", DoubleType()),
        StructField("data_version", StringType()),
    ]
)


def build_feature_frame(kafka_stream: DataFrame) -> DataFrame:
    """Parse Kafka messages or exported Kafka envelopes into dashboard observations."""
    topic_column = "topic" if "topic" in kafka_stream.columns else "kafka_topic"
    value_type = kafka_stream.schema["value"].dataType
    value_json = (
        F.to_json(F.col("value"))
        if isinstance(value_type, StructType)
        else F.col("value").cast("string")
    )
    if "timestamp" in kafka_stream.columns:
        kafka_timestamp = F.col("timestamp").cast("timestamp")
    elif "kafka_timestamp_ms" in kafka_stream.columns:
        kafka_timestamp = F.timestamp_millis(F.col("kafka_timestamp_ms"))
    else:
        kafka_timestamp = F.lit(None).cast("timestamp")
    parsed = (
        kafka_stream.select(
            F.col(topic_column).cast("string").alias("kafka_topic"),
            F.from_json(value_json, RECORD_SCHEMA).alias("record"),
            kafka_timestamp.alias("kafka_timestamp"),
        )
        .select("kafka_topic", "kafka_timestamp", "record.*")
        .withColumn("city", canonical_city_udf("city", "latitude", "longitude"))
        .withColumn("station_id", F.lower(F.trim(F.col("city"))))
        .withColumn("timestamp", F.to_timestamp("event_time"))
        .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_time"))
        .withColumn(
            "event_to_ingestion_seconds",
            F.col("ingestion_timestamp").cast("double") - F.col("timestamp").cast("double"),
        )
        .withColumn(
            "ingestion_to_kafka_seconds",
            F.col("kafka_timestamp").cast("double") - F.col("ingestion_timestamp").cast("double"),
        )
        .withColumn(
            "event_to_kafka_seconds",
            F.col("kafka_timestamp").cast("double") - F.col("timestamp").cast("double"),
        )
        .withColumn(
            "is_weather",
            F.col("kafka_topic").isin("weather.raw.stream", "weather.raw.batch"),
        )
        .withColumn("is_air_quality", F.col("kafka_topic") == "air_quality.raw.stream")
        .withColumn("temp_c", F.col("temperature_c"))
        .withColumn("pressure", F.col("pressure_hpa"))
        .withColumn("wind_speed", F.col("wind_speed_mps"))
        .withColumn("wind_dir", F.col("wind_direction_deg"))
        .withColumn("precipitation", F.col("precipitation_mm"))
        .withColumn("cloud_cover", F.col("cloud_cover_pct"))
        .withColumn("soil_temperature", F.col("soil_temperature_0_to_7cm_c"))
        .withColumn("shortwave_radiation", F.col("shortwave_radiation_wm2"))
        .withColumn("pm2_5", F.col("pm25"))
        .withColumn("us_aqi", F.col("aqi"))
        .withColumn(
            "_valid_record",
            F.col("station_id").isNotNull()
            & (F.col("station_id") != "")
            & F.col("timestamp").isNotNull()
            & F.col("source").isNotNull()
            & (
                (F.col("is_weather") & F.col("temp_c").isNotNull())
                | (F.col("is_air_quality") & (F.col("pm2_5").isNotNull() | F.col("us_aqi").isNotNull()))
            )
        )
    )

    radians = F.lit(3.141592653589793) / 180.0
    valid_humidity = F.col("humidity").between(0.1, 100.0)
    gamma = F.log(F.col("humidity") / 100.0) + (
        17.625 * F.col("temp_c") / (243.04 + F.col("temp_c"))
    )
    latitude_radians = F.col("latitude") * radians
    longitude_radians = F.col("longitude") * radians
    vietnam_timestamp = F.from_utc_timestamp(F.col("timestamp"), "Asia/Ho_Chi_Minh")
    valid_air_density = (
        F.col("pressure").isNotNull()
        & (F.col("pressure") > 0)
        & F.col("temp_c").isNotNull()
        & (F.col("temp_c") > -273.15)
    )

    return (
        parsed.withColumn(
            "coord_X",
            F.when(
                F.col("latitude").isNotNull() & F.col("longitude").isNotNull(),
                F.cos(latitude_radians) * F.cos(longitude_radians),
            ),
        )
        .withColumn(
            "coord_Y",
            F.when(
                F.col("latitude").isNotNull() & F.col("longitude").isNotNull(),
                F.cos(latitude_radians) * F.sin(longitude_radians),
            ),
        )
        .withColumn(
            "coord_Z",
            F.when(
                F.col("latitude").isNotNull(),
                F.sin(latitude_radians),
            ),
        )
        .withColumn(
            "air_density",
            F.when(
                valid_air_density,
                (F.col("pressure") * 100.0)
                / (287.05 * (F.col("temp_c") + 273.15)),
            ),
        )
        .withColumn(
            "hour_sin",
            F.sin(2.0 * 3.141592653589793 * F.hour(vietnam_timestamp) / 24.0),
        )
        .withColumn(
            "hour_cos",
            F.cos(2.0 * 3.141592653589793 * F.hour(vietnam_timestamp) / 24.0),
        )
        .withColumn(
            "day_sin",
            F.sin(2.0 * 3.141592653589793 * F.dayofyear(vietnam_timestamp) / 365.0),
        )
        .withColumn(
            "wind_U",
            F.when(
                F.col("is_weather"),
                -F.col("wind_speed") * F.sin(F.col("wind_dir") * radians),
            ),
        )
        .withColumn(
            "wind_V",
            F.when(
                F.col("is_weather"),
                -F.col("wind_speed") * F.cos(F.col("wind_dir") * radians),
            ),
        )
        .withColumn(
            "dew_point",
            F.when(
                F.col("is_weather")
                & valid_humidity
                & F.col("temp_c").isNotNull()
                & (F.col("temp_c") > -243.04),
                243.04 * gamma / (17.625 - gamma),
            ),
        )
        .withColumn(
            "is_stagnant_air",
            F.when(
                F.col("is_weather")
                & F.col("wind_speed").isNotNull()
                & F.col("pressure").isNotNull(),
                (F.col("wind_speed") < 1.0) & (F.col("pressure") > 1010.0),
            ),
        )
        .withColumn(
            "cooling_degree_days",
            F.when(
                F.col("is_weather") & F.col("temp_c").isNotNull(),
                F.greatest(F.col("temp_c") - 24.0, F.lit(0.0)),
            ),
        )
        .withColumn(
            "_id",
            F.concat_ws(
                "|",
                F.col("station_id"),
                F.date_format("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                F.col("source"),
            ),
        )
        .select(
            "_id",
            "station_id",
            "city",
            "region",
            "latitude",
            "longitude",
            "timestamp",
            "ingestion_timestamp",
            "kafka_timestamp",
            "event_to_ingestion_seconds",
            "ingestion_to_kafka_seconds",
            "event_to_kafka_seconds",
            "source",
            F.col("data_version").alias("schema_version"),
            "temp_c",
            "humidity",
            "pressure",
            "wind_speed",
            "wind_dir",
            "precipitation",
            "cloud_cover",
            "soil_temperature",
            "shortwave_radiation",
            "pm2_5",
            "us_aqi",
            "coord_X",
            "coord_Y",
            "coord_Z",
            "air_density",
            "hour_sin",
            "hour_cos",
            "day_sin",
            "dew_point",
            "wind_U",
            "wind_V",
            "is_stagnant_air",
            "cooling_degree_days",
            "_valid_record",
        )
    )


def write_observation_batch(batch: DataFrame, batch_id: int) -> None:
    batch_started = time.perf_counter()
    batch.persist()
    try:
        invalid_count = batch.filter(
            F.coalesce(~F.col("_valid_record"), F.lit(True))
        ).count()
        if invalid_count:
            LOGGER.warning("Discarded %s invalid records in Kafka micro-batch %s", invalid_count, batch_id)

        valid_batch = batch.filter(F.col("_valid_record")).drop("_valid_record")
        valid_count = valid_batch.count()
        if valid_count == 0:
            LOGGER.info("Skipping empty or invalid Kafka micro-batch %s", batch_id)
            return

        latency_row = valid_batch.agg(
            F.expr(
                "percentile_approx(event_to_ingestion_seconds, array(0.5, 0.95), 1000)"
            ).alias("event_to_ingestion_seconds_p50_p95"),
            F.expr(
                "percentile_approx(ingestion_to_kafka_seconds, array(0.5, 0.95), 1000)"
            ).alias("ingestion_to_kafka_seconds_p50_p95"),
            F.expr(
                "percentile_approx(event_to_kafka_seconds, array(0.5, 0.95), 1000)"
            ).alias("event_to_kafka_seconds_p50_p95"),
        ).first().asDict()
        mongo_uri = os.getenv("MONGODB_URI", DEFAULT_MONGODB_URI)
        database = os.getenv("MONGODB_DATABASE", DEFAULT_MONGODB_DATABASE)
        collections = [
            os.getenv("MONGODB_LIVE_COLLECTION", DEFAULT_LIVE_COLLECTION),
            os.getenv("MONGODB_HISTORY_COLLECTION", DEFAULT_HISTORY_COLLECTION),
        ]

        for collection in collections:
            (
                valid_batch.write.format("mongodb")
                .mode("append")
                .option("connection.uri", mongo_uri)
                .option("database", database)
                .option("collection", collection)
                .option("operationType", "replace")
                .option("idFieldList", "_id")
                .option("upsertDocument", "true")
                .save()
            )

        LOGGER.info(
            "Wrote %s observations from Kafka micro-batch %s to %s",
            valid_count,
            batch_id,
            ", ".join(f"{database}.{collection}" for collection in collections),
        )
        LOGGER.info(
            "pipeline_metrics %s",
            json.dumps(
                {
                    "stage": "spark_to_mongo",
                    "batch_id": batch_id,
                    "records": valid_count,
                    "duration_seconds": round(time.perf_counter() - batch_started, 3),
                    **latency_row,
                },
                sort_keys=True,
                default=str,
            ),
        )
    finally:
        batch.unpersist()


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS)
    topics = [topic.strip() for topic in os.getenv("KAFKA_TOPICS", DEFAULT_TOPICS).split(",") if topic.strip()]
    if not topics:
        raise ValueError("KAFKA_TOPICS must contain at least one Kafka topic")

    checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", DEFAULT_CHECKPOINT_DIR)
    spark = (
        SparkSession.builder.appName("ClimateObservationStream")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    LOGGER.info(
        "Starting Kafka-to-MongoDB processing: brokers=%s topics=%s checkpoint=%s",
        bootstrap_servers,
        topics,
        checkpoint_dir,
    )
    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", ",".join(topics))
        .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "latest"))
        .option("failOnDataLoss", "false")
        .load()
    )
    observations = build_feature_frame(kafka_stream)
    query = (
        observations.writeStream.foreachBatch(write_observation_batch)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
