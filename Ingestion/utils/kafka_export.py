from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, TopicPartition


def build_consumer(bootstrap_servers: list[str], group_id: str, from_beginning: bool) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": ",".join(bootstrap_servers),
            "group.id": group_id,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        }
    )


def decode_message(raw_value: bytes | None) -> Any:
    if raw_value is None:
        return None
    text = raw_value.decode("utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    temporary_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary_path.replace(path)


def export_messages(
    topics: list[str],
    output_path: Path,
    bootstrap_servers: list[str],
    group_id: str,
    max_messages: int,
    poll_timeout_seconds: float,
    idle_timeout_seconds: float,
    from_beginning: bool,
    include_kafka_meta: bool,
) -> dict[str, Any]:
    if max_messages < 0:
        raise ValueError("max_messages must be zero (unlimited) or a positive integer")
    if poll_timeout_seconds <= 0 or idle_timeout_seconds <= 0:
        raise ValueError("poll and idle timeouts must be positive")
    if not topics:
        raise ValueError("At least one Kafka topic is required")
    topics = list(dict.fromkeys(topics))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("", encoding="utf-8")
    manifest_path = output_path.with_suffix(output_path.suffix + ".manifest.json")
    consumer = build_consumer(bootstrap_servers, group_id, from_beginning)
    started_at = datetime.now(timezone.utc).isoformat()
    partition_ranges: dict[tuple[str, int], dict[str, Any]] = {}
    topic_counts = {topic: 0 for topic in topics}
    exported_count = 0
    completion_reason = "export_failed"
    last_activity = time.monotonic()

    try:
        assignments: list[TopicPartition] = []
        for topic in topics:
            metadata = consumer.list_topics(topic, timeout=10)
            topic_metadata = metadata.topics.get(topic)
            if topic_metadata is None or topic_metadata.error is not None:
                raise RuntimeError(f"Unable to read metadata for Kafka topic {topic}")
            for partition in sorted(topic_metadata.partitions):
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition),
                    timeout=10,
                    cached=False,
                )
                partition_ranges[(topic, partition)] = {
                    "topic": topic,
                    "partition": partition,
                    "start_offset": low if from_beginning else high,
                    "end_offset_exclusive": high,
                    "first_exported_offset": None,
                    "last_exported_offset": None,
                    "exported_count": 0,
                    "end_reached": False,
                }
                assignments.append(
                    TopicPartition(topic, partition, low if from_beginning else high)
                )

        if not assignments:
            raise RuntimeError("No Kafka partitions found for the requested topics")
        consumer.assign(assignments)

        with output_path.open("w", encoding="utf-8", newline="\n") as handle:
            while not all(item["end_reached"] for item in partition_ranges.values()):
                message = consumer.poll(poll_timeout_seconds)
                if message is None:
                    if time.monotonic() - last_activity >= idle_timeout_seconds:
                        completion_reason = "idle_timeout_before_all_partition_ends"
                        break
                    continue

                last_activity = time.monotonic()
                error = message.error()
                if error is not None:
                    if error.code() == KafkaError._PARTITION_EOF:
                        state = partition_ranges.get((message.topic(), message.partition()))
                        if state is not None:
                            state["end_reached"] = message.offset() >= state["end_offset_exclusive"]
                        continue
                    raise RuntimeError(f"Kafka consume failed: {error}")

                if max_messages and exported_count >= max_messages:
                    completion_reason = "max_messages_reached"
                    break

                state = partition_ranges.get((message.topic(), message.partition()))
                if state is None:
                    raise RuntimeError(
                        f"Received a message from an unexpected partition: "
                        f"{message.topic()}[{message.partition()}]"
                    )
                if message.offset() >= state["end_offset_exclusive"]:
                    continue

                decoded = decode_message(message.value())
                message_timestamp = message.timestamp()
                kafka_timestamp_ms = (
                    message_timestamp[1]
                    if message_timestamp is not None and message_timestamp[1] >= 0
                    else None
                )
                record = (
                    {
                        "kafka_topic": message.topic(),
                        "kafka_partition": message.partition(),
                        "kafka_offset": message.offset(),
                        "kafka_timestamp_ms": kafka_timestamp_ms,
                        "value": decoded,
                    }
                    if include_kafka_meta
                    else decoded
                )
                handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                exported_count += 1
                topic_counts[message.topic()] += 1
                state["first_exported_offset"] = (
                    message.offset()
                    if state["first_exported_offset"] is None
                    else state["first_exported_offset"]
                )
                state["last_exported_offset"] = message.offset()
                state["exported_count"] += 1
            else:
                completion_reason = "all_partition_offsets_reached"
    except Exception:
        completion_reason = "export_failed"
        raise
    finally:
        consumer.close()
        if output_path.exists():
            partitions = list(partition_ranges.values())
            complete = (
                completion_reason == "all_partition_offsets_reached"
                and all(partition["end_reached"] for partition in partitions)
            )
            manifest = {
                "manifest_version": 1,
                "export_started_at": started_at,
                "export_finished_at": datetime.now(timezone.utc).isoformat(),
                "topics": topics,
                "topic_counts": topic_counts,
                "exported_count": exported_count,
                "complete": complete,
                "completion_reason": completion_reason,
                "requested_max_messages": max_messages,
                "partitions": partitions,
                "output_file": output_path.name,
                "output_size_bytes": output_path.stat().st_size,
                "sha256": _sha256(output_path),
            }
            _write_manifest(manifest_path, manifest)

    return manifest
