import os
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGODB_DATABASE", "climate_db")
MONGO_BATCH_COLLECTION = os.getenv("MONGODB_HISTORY_COLLECTION", "climate_observations")
MONGO_STREAM_COLLECTION = os.getenv("MONGODB_LIVE_COLLECTION", "realtime_observations")
MONGO_BATCH_URI = f"{MONGO_URI}/{MONGO_DB}.{MONGO_BATCH_COLLECTION}"
MONGO_STREAM_OBS_URI = f"{MONGO_URI}/{MONGO_DB}.{MONGO_STREAM_COLLECTION}"
MONGO_STREAM_ALERTS_URI = f"{MONGO_URI}/{MONGO_DB}.realtime_alerts"

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPICS", "weather.raw.stream,air_quality.raw.stream")
INPUT_HDFS = os.getenv(
    "INPUT_HDFS",
    "hdfs://namenode:8020/user/data_engineer/raw_jsonl/*.jsonl",
)
OUTPUT_HDFS = os.getenv(
    "OUTPUT_HDFS",
    "hdfs://namenode:8020/user/data_engineer/feature_store/feature/",
)
SPARK_LOAD_MODE = os.getenv("SPARK_LOAD_MODE", "append")
CHECKPOINT_DIR = os.getenv(
    "SPARK_CHECKPOINT_DIR",
    str(ROOT_DIR / "checkpoint" / "processing_stream"),
)

SERVING_COLS = [
    "station_id",
    "timestamp",
    "latitude",
    "longitude",
    "temp_c",
    "humidity",
    "pressure",
    "wind_speed",
    "wind_dir",
    "precipitation",
    "cloud_cover",
    "shortwave_radiation",
    "soil_temperature",
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
    "pressure_delta_3h",
    "wind_shear_U",
    "wind_shear_V",
    "temp_mean_6h",
    "pm25_mean_12h",
    "source",
    "schema_version",
    "city",
    "region",
]

OBSERVATION_COLS = SERVING_COLS

STREAMING_ALERT_COLS = [
    "station_id",
    "timestamp",
    "target_feature",
    "forecast_horizon",
    "predicted_value",
]

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5001")
