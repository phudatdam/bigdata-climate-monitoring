import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from processing.batch.archive_manifest import validate_export_manifest


class ExportManifestTests(unittest.TestCase):
    def test_valid_complete_manifest_accepts_matching_export(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            export_path = Path(temporary_directory) / "weather.jsonl"
            payload = b'{"kafka_topic":"weather.raw.stream"}\n'
            export_path.write_bytes(payload)
            manifest_path = Path(temporary_directory) / "weather.jsonl.manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "complete": True,
                        "topics": ["weather.raw.stream"],
                        "exported_count": 1,
                        "output_file": export_path.name,
                        "output_size_bytes": len(payload),
                        "sha256": hashlib.sha256(payload).hexdigest(),
                    }
                ),
                encoding="utf-8",
            )

            result = validate_export_manifest(str(export_path), str(manifest_path))

            self.assertTrue(result["complete"])

    def test_truncated_or_modified_export_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            export_path = Path(temporary_directory) / "weather.jsonl"
            export_path.write_text('{"value":1}\n', encoding="utf-8")
            manifest_path = Path(temporary_directory) / "weather.jsonl.manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "complete": False,
                        "exported_count": 1,
                        "output_file": export_path.name,
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "incomplete"):
                validate_export_manifest(str(export_path), str(manifest_path))

            manifest_path.write_text(
                json.dumps(
                    {
                        "complete": True,
                        "exported_count": 1,
                        "output_file": export_path.name,
                        "output_size_bytes": export_path.stat().st_size,
                        "sha256": "wrong-hash",
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "SHA-256"):
                validate_export_manifest(str(export_path), str(manifest_path))


if __name__ == "__main__":
    unittest.main()
