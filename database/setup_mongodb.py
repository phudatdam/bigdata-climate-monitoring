import os
from pathlib import Path

from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING, MongoClient


ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")


def setup_database() -> None:
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    database_name = os.getenv("MONGODB_DATABASE", "climate_db")

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        database = client[database_name]
        for collection_name in (
            os.getenv("MONGODB_HISTORY_COLLECTION", "climate_observations"),
            os.getenv("MONGODB_LIVE_COLLECTION", "realtime_observations"),
            os.getenv("COLLECTION_ALERTS", "realtime_alerts"),
        ):
            database[collection_name].create_index(
                [("station_id", ASCENDING), ("timestamp", DESCENDING)]
            )
        database[os.getenv("MONGODB_LIVE_COLLECTION", "realtime_observations")].create_index(
            "timestamp", expireAfterSeconds=172800
        )
        print(f"MongoDB indexes are ready in {database_name}.")
    finally:
        client.close()


if __name__ == "__main__":
    setup_database()
