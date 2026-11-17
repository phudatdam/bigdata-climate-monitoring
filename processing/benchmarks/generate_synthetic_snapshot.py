from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from Ingestion.utils.locations import OPEN_METEO_LOCATIONS


TOPICS = ("weather.raw.stream", "air_quality.raw.stream")
PARTITION_COUNT = 3


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a deterministic, manifest-validated synthetic Kafka export."
    )
    parser.add_argument("--records", type=int, default=10_000)
    parser.add_argument("--output-dir", default="exports")
    parser.add_argument("--start", default="2026-01-01T00:00:00Z")
    parser.add_argument("--interval-minutes", type=int, default=15)
    return parser.parse_args()


def record_value(
    index: int,
    event_time: datetime,
    ingestion_time: datetime,
    city: dict,
    weather: bool,
) -> dict:
    city_name = str(city["city"])
    latitude = float(city["latitude"])
    longitude = float(city["longitude"])
    value = {
        "event_id": f"synthetic-{index:09d}",
        "source": "open-meteo" if weather else "aqicn",
        "region": str(city.get("region", "synthetic")),
        "city": city_name,
        "station_id": city_name,
        "latitude": latitude,
        "longitude": longitude,
        "event_time": event_time.isoformat(),
        "ingestion_time": ingestion_time.isoformat(),
        "data_version": "synthetic-v1",
    }
    if weather:
        value.update(
            {
                "temperature_c": 15.0 + (index % 220) / 10.0,
                "humidity": 35.0 + (index % 650) / 10.0,
                "pressure_hpa": 990.0 + (index % 350) / 10.0,
                "wind_speed_mps": (index % 150) / 10.0,
                "wind_direction_deg": float(index % 360),
                "precipitation_mm": float(index % 12) / 10.0,
                "cloud_cover_pct": float(index % 101),
                "weather_main": "Synthetic",
                "weather_desc": "Synthetic benchmark observation",
            }
        )
    else:
        value.update(
            {
                "aqi": float(index % 250),
                "quality_level": "Synthetic",
                "pm25": float(index % 100),
                "pm10": float(index % 150),
                "no2": float(index % 80),
                "o3": float(index % 90),
            }
        )
    return value


def generate_snapshot(
    record_count: int,
    output_dir: Path,
    start_time: datetime,
    interval_minutes: int,
) -> tuple[Path, dict]:
    if record_count < 2:
        raise ValueError("records must be at least 2 so both record types are represented")
    if interval_minutes < 1:
        raise ValueError("interval-minutes must be positive")
    if start_time.tzinfo is None:
        raise ValueError("start must include a timezone")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"synthetic_snapshot_{record_count}.jsonl"
    manifest_path = output_path.with_suffix(output_path.suffix + ".manifest.json")
    location_count = len(OPEN_METEO_LOCATIONS)
    offsets = {topic: [0] * PARTITION_COUNT for topic in TOPICS}
    topic_counts = {topic: 0 for topic in TOPICS}
    partition_counts = {
        (topic, partition): 0
        for topic in TOPICS
        for partition in range(PARTITION_COUNT)
    }

    with output_path.open("w", encoding="utf-8", newline="\n") as output:
        for index in range(record_count):
            weather = index % 2 == 0
            topic = TOPICS[0] if weather else TOPICS[1]
            city_index = (index // 2) % location_count
            interval_index = index // (2 * location_count)
            event_time = start_time.astimezone(timezone.utc) + timedelta(
                minutes=interval_index * interval_minutes
            )
            ingestion_time = event_time + timedelta(minutes=5)
            kafka_timestamp = ingestion_time + timedelta(seconds=2)
            partition = city_index % PARTITION_COUNT
            offset = offsets[topic][partition]
            offsets[topic][partition] += 1
            partition_counts[(topic, partition)] += 1
            topic_counts[topic] += 1
            record = {
                "kafka_topic": topic,
                "kafka_partition": partition,
                "kafka_offset": offset,
                "kafka_timestamp_ms": int(kafka_timestamp.timestamp() * 1000),
                "value": record_value(
                    index,
                    event_time,
                    ingestion_time,
                    OPEN_METEO_LOCATIONS[city_index],
                    weather,
                ),
            }
            output.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")

    digest = sha256_file(output_path)
    partitions = []
    for topic in TOPICS:
        for partition in range(PARTITION_COUNT):
            count = partition_counts[(topic, partition)]
            partitions.append(
                {
                    "topic": topic,
                    "partition": partition,
                    "start_offset": 0,
                    "end_offset_exclusive": count,
                    "first_exported_offset": 0 if count else None,
                    "last_exported_offset": count - 1 if count else None,
                    "exported_count": count,
                    "end_reached": True,
                }
            )

    manifest = {
        "manifest_version": 1,
        "export_started_at": datetime.now(timezone.utc).isoformat(),
        "export_finished_at": datetime.now(timezone.utc).isoformat(),
        "topics": list(TOPICS),
        "topic_counts": topic_counts,
        "exported_count": record_count,
        "complete": True,
        "completion_reason": "synthetic_deterministic_snapshot",
        "requested_max_messages": 0,
        "partitions": partitions,
        "output_file": output_path.name,
        "output_size_bytes": output_path.stat().st_size,
        "sha256": digest,
        "synthetic": True,
        "start_time": start_time.astimezone(timezone.utc).isoformat(),
        "interval_minutes": interval_minutes,
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path, manifest


def main() -> None:
    args = parse_args()
    start_time = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
    output_path, manifest = generate_snapshot(
        args.records,
        Path(args.output_dir),
        start_time,
        args.interval_minutes,
    )
    print(
        json.dumps(
            {
                "output": str(output_path.resolve()),
                "manifest": str(output_path.with_suffix(output_path.suffix + ".manifest.json").resolve()),
                "records": manifest["exported_count"],
                "bytes": manifest["output_size_bytes"],
                "sha256": manifest["sha256"],
                "topics": manifest["topic_counts"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
