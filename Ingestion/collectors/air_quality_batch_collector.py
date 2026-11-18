from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from producers.dlq_producer import build_dlq_message, send_to_dlq
from producers.kafka_producer import build_producer, enqueue_json_message
from utils.logger import get_logger, log_event
from utils.locations import OPEN_METEO_LOCATIONS, filter_locations, location_names
from utils.metadata import build_batch_id, enrich_ingestion_metadata
from utils.retry import retry_call
from utils.serialization import serialize_record
from validators.air_validator import validate_air_record
from validators.normalized_schema import normalize_air_quality


LOGGER = get_logger("air_quality_batch_collector")
AIR_QUALITY_BATCH_TOPIC = "air_quality.raw.batch"
AIR_QUALITY_DLQ_TOPIC = "air_quality.raw.dlq"
OPEN_METEO_AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
PIPELINE_NAME = "air_quality_batch_ingestion"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch ingest historical Open-Meteo air quality data to Kafka.")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--topic", default=AIR_QUALITY_BATCH_TOPIC)
    parser.add_argument("--dlq-topic", default=AIR_QUALITY_DLQ_TOPIC)
    parser.add_argument("--locations", nargs="*", default=location_names(OPEN_METEO_LOCATIONS))
    return parser.parse_args()


def fetch_air_quality_history(location: dict[str, Any], start_date: str, end_date: str) -> dict[str, Any]:
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,us_aqi",
        "timezone": "UTC",
    }

    def make_request() -> dict[str, Any]:
        response = requests.get(OPEN_METEO_AIR_QUALITY_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    return retry_call(
        make_request,
        retries=3,
        base_delay_seconds=1.5,
        retryable_exceptions=(requests.RequestException,),
    )


def iter_hourly_air_quality_records(payload: dict[str, Any], city: str) -> list[dict[str, Any]]:
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    records: list[dict[str, Any]] = []
    for index, event_time in enumerate(times):
        records.append(
            {
                "source": "open-meteo",
                "city": city,
                "latitude": payload.get("latitude"),
                "longitude": payload.get("longitude"),
                "current": {
                    "time": event_time,
                    "pm10": _safe_array_value(hourly.get("pm10"), index),
                    "pm2_5": _safe_array_value(hourly.get("pm2_5"), index),
                    "carbon_monoxide": _safe_array_value(hourly.get("carbon_monoxide"), index),
                    "nitrogen_dioxide": _safe_array_value(hourly.get("nitrogen_dioxide"), index),
                    "sulphur_dioxide": _safe_array_value(hourly.get("sulphur_dioxide"), index),
                    "ozone": _safe_array_value(hourly.get("ozone"), index),
                    "us_aqi": _safe_array_value(hourly.get("us_aqi"), index),
                },
            }
        )
    return records


def _safe_array_value(values: Any, index: int) -> Any:
    if not isinstance(values, list) or index >= len(values):
        return None
    return values[index]


def _send_dlq_event(
    producer: Any,
    dlq_topic: str,
    batch_id: str,
    error_type: str,
    error_message: str,
    raw_payload: dict[str, Any],
    city: str,
    topic: str,
    normalized_payload: dict[str, Any] | None = None,
    event_time: str | None = None,
) -> None:
    message = build_dlq_message(
        batch_id=batch_id,
        original_topic=topic,
        error_type=error_type,
        error_message=error_message,
        raw_payload=raw_payload,
        normalized_payload=normalized_payload,
    )
    send_to_dlq(producer, dlq_topic=dlq_topic, message=message)


def main() -> None:
    args = parse_args()
    locations = filter_locations(OPEN_METEO_LOCATIONS, args.locations)
    if not locations:
        raise ValueError("No matching locations were selected")

    producer = build_producer()
    seen_keys: set[tuple[str, str, str]] = set()
    success_count = 0
    failed_count = 0
    skipped_duplicates = 0
    batch_id = build_batch_id("air_quality_batch", args.start_date, args.end_date)
    delivered_by_city: dict[str, int] = {}

    def handle_delivery(error: Any, kafka_message: Any, city: str) -> None:
        nonlocal success_count, failed_count
        if error is not None:
            failed_count += 1
            return
        success_count += 1
        delivered_by_city[city] = delivered_by_city.get(city, 0) + 1

    log_event(
        LOGGER,
        "air_quality_batch_started",
        topic=args.topic,
        start_date=args.start_date,
        end_date=args.end_date,
        locations=[location["city"] for location in locations],
        batch_id=batch_id,
        started_at=datetime.now(UTC).isoformat(),
    )

    try:
        for location in locations:
            try:
                payload = fetch_air_quality_history(location, args.start_date, args.end_date)
            except Exception as exc:
                failed_count += 1
                log_event(LOGGER, "air_quality_fetch_failed", city=location["city"], error=str(exc))
                _send_dlq_event(
                    producer,
                    dlq_topic=args.dlq_topic,
                    batch_id=batch_id,
                    error_type="fetch_failed",
                    error_message=str(exc),
                    raw_payload={"location": location, "start_date": args.start_date, "end_date": args.end_date},
                    city=location["city"],
                    topic=args.topic,
                )
                continue

            for raw_record in iter_hourly_air_quality_records(payload, city=location["city"]):
                try:
                    normalized = normalize_air_quality(raw_record, source="open-meteo").dict()
                    dedup_key = (normalized["source"], normalized["station_id"], normalized["event_time"].isoformat())
                    if dedup_key in seen_keys:
                        skipped_duplicates += 1
                        continue
                    seen_keys.add(dedup_key)

                    errors = validate_air_record(normalized)
                    if errors:
                        failed_count += 1
                        _send_dlq_event(
                            producer,
                            dlq_topic=args.dlq_topic,
                            batch_id=batch_id,
                            error_type="validation_failed",
                            error_message="; ".join(errors),
                            raw_payload=raw_record,
                            normalized_payload=serialize_record(normalized),
                            city=location["city"],
                            event_time=normalized["event_time"].isoformat(),
                            topic=args.topic,
                        )
                        continue

                    payload_message = serialize_record(normalized)
                    enqueue_json_message(
                        producer,
                        topic=args.topic,
                        key=f"region_{normalized['region']}",
                        message=enrich_ingestion_metadata(
                            payload_message,
                            pipeline_name=PIPELINE_NAME,
                            batch_id=batch_id,
                            source_endpoint=OPEN_METEO_AIR_QUALITY_URL,
                        ),
                        on_delivery=lambda error, kafka_message, city=normalized["city"]: handle_delivery(error, kafka_message, city),
                    )
                except Exception as exc:
                    failed_count += 1
                    log_event(LOGGER, "air_quality_record_failed", city=location["city"], error=str(exc))

            producer.flush(timeout=30.0)

        log_event(
            LOGGER,
            "air_quality_batch_completed",
            topic=args.topic,
            start_date=args.start_date,
            end_date=args.end_date,
            success_count=success_count,
            failed_count=failed_count,
            skipped_duplicates=skipped_duplicates,
            delivered_by_city=delivered_by_city,
            finished_at=datetime.now(UTC).isoformat(),
        )
        if failed_count:
            raise SystemExit(1)
    finally:
        producer.flush(timeout=30.0)


if __name__ == "__main__":
    main()
