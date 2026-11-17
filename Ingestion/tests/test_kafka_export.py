import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from confluent_kafka import KafkaError

from Ingestion.utils.kafka_export import export_messages


class FakeError:
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code


class FakeMessage:
    def __init__(self, topic, partition, offset, value=None, error=None, timestamp_ms=1_794_215_000_000):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._value = value
        self._error = error
        self._timestamp_ms = timestamp_ms

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def error(self):
        return self._error

    def timestamp(self):
        return (1, self._timestamp_ms)


class FakeConsumer:
    def __init__(self, messages):
        self.messages = iter(messages)
        self.assigned = []
        self.closed = False

    def list_topics(self, topic, timeout):
        return SimpleNamespace(
            topics={topic: SimpleNamespace(error=None, partitions={0: None})}
        )

    def get_watermark_offsets(self, partition, timeout, cached):
        return (0, 2)

    def assign(self, assignments):
        self.assigned = assignments

    def poll(self, timeout):
        return next(self.messages, None)

    def close(self):
        self.closed = True


class KafkaExportTests(unittest.TestCase):
    def _run_export(self, messages, max_messages):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        output_path = Path(temporary_directory.name) / "export.jsonl"
        consumer = FakeConsumer(messages)
        with patch("Ingestion.utils.kafka_export.build_consumer", return_value=consumer):
            manifest = export_messages(
                topics=["weather.raw.stream"],
                output_path=output_path,
                bootstrap_servers=["localhost:9092"],
                group_id="export-test",
                max_messages=max_messages,
                poll_timeout_seconds=0.01,
                idle_timeout_seconds=0.01,
                from_beginning=True,
                include_kafka_meta=True,
            )
        return output_path, manifest, consumer

    def test_complete_export_records_counts_offsets_and_hash(self):
        output_path, manifest, consumer = self._run_export(
            [
                FakeMessage("weather.raw.stream", 0, 0, b'{"value":1}'),
                FakeMessage("weather.raw.stream", 0, 1, b'{"value":2}'),
                FakeMessage(
                    "weather.raw.stream",
                    0,
                    2,
                    error=FakeError(KafkaError._PARTITION_EOF),
                ),
            ],
            max_messages=10,
        )

        self.assertTrue(manifest["complete"])
        self.assertEqual(manifest["completion_reason"], "all_partition_offsets_reached")
        self.assertEqual(manifest["topic_counts"], {"weather.raw.stream": 2})
        self.assertEqual(manifest["partitions"][0]["first_exported_offset"], 0)
        self.assertEqual(manifest["partitions"][0]["last_exported_offset"], 1)
        self.assertEqual(manifest["partitions"][0]["end_offset_exclusive"], 2)
        self.assertEqual(manifest["exported_count"], 2)
        self.assertTrue(consumer.closed)
        self.assertEqual(
            [json.loads(line)["kafka_offset"] for line in output_path.read_text().splitlines()],
            [0, 1],
        )
        self.assertEqual(
            [json.loads(line)["kafka_timestamp_ms"] for line in output_path.read_text().splitlines()],
            [1_794_215_000_000, 1_794_215_000_000],
        )

    def test_message_limit_marks_a_truncated_export_incomplete(self):
        output_path, manifest, _ = self._run_export(
            [
                FakeMessage("weather.raw.stream", 0, 0, b'{"value":1}'),
                FakeMessage("weather.raw.stream", 0, 1, b'{"value":2}'),
                FakeMessage(
                    "weather.raw.stream",
                    0,
                    2,
                    error=FakeError(KafkaError._PARTITION_EOF),
                ),
            ],
            max_messages=1,
        )

        self.assertFalse(manifest["complete"])
        self.assertEqual(manifest["completion_reason"], "max_messages_reached")
        self.assertEqual(manifest["exported_count"], 1)
        self.assertEqual(len(output_path.read_text().splitlines()), 1)

    def test_identical_kafka_snapshot_has_stable_jsonl_bytes(self):
        messages = [
            FakeMessage("weather.raw.stream", 0, 0, b'{"value":1}'),
            FakeMessage(
                "weather.raw.stream",
                0,
                1,
                error=FakeError(KafkaError._PARTITION_EOF),
            ),
        ]
        first_path, _, _ = self._run_export(messages, max_messages=10)
        second_path, _, _ = self._run_export(messages, max_messages=10)

        self.assertEqual(first_path.read_bytes(), second_path.read_bytes())


if __name__ == "__main__":
    unittest.main()
