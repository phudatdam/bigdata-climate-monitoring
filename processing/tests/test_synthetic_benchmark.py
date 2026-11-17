import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from processing.benchmarks.generate_synthetic_snapshot import generate_snapshot


class SyntheticBenchmarkTests(unittest.TestCase):
    def test_snapshot_is_reproducible_complete_and_covers_both_topics(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as output_dir:
            first_path, first_manifest = generate_snapshot(128, Path(output_dir), start, 15)
            first_bytes = first_path.read_bytes()
            second_path, second_manifest = generate_snapshot(128, Path(output_dir), start, 15)

            self.assertEqual(second_path.read_bytes(), first_bytes)
            self.assertEqual(first_manifest["sha256"], second_manifest["sha256"])
            self.assertTrue(first_manifest["complete"])
            self.assertTrue(first_manifest["synthetic"])
            self.assertEqual(first_manifest["exported_count"], 128)
            self.assertEqual(sum(first_manifest["topic_counts"].values()), 128)
            self.assertTrue(all(row["end_reached"] for row in first_manifest["partitions"]))
            self.assertEqual(
                {row["kafka_timestamp_ms"] for row in map(json.loads, first_bytes.splitlines())},
                set(
                    int(
                        datetime.fromisoformat(record["value"]["ingestion_time"]).timestamp()
                        * 1000
                        + 2_000
                    )
                    for record in map(json.loads, first_bytes.splitlines())
                ),
            )


if __name__ == "__main__":
    unittest.main()
