from __future__ import annotations

import hashlib
import json
import os


def validate_export_manifest(input_path: str, manifest_path: str) -> dict:
    source_path = os.path.abspath(input_path)
    with open(manifest_path, encoding="utf-8") as handle:
        manifest = json.load(handle)
    if manifest.get("complete") is not True:
        raise ValueError("Kafka export is incomplete; refusing historical processing")
    if manifest.get("output_file") != os.path.basename(source_path):
        raise ValueError("Kafka export manifest does not describe the input file")

    digest = hashlib.sha256()
    line_count = 0
    with open(source_path, "rb") as handle:
        for line in handle:
            digest.update(line)
            line_count += 1
    if digest.hexdigest() != manifest.get("sha256"):
        raise ValueError("Kafka export SHA-256 does not match its manifest")
    if line_count != manifest.get("exported_count"):
        raise ValueError("Kafka export line count does not match its manifest")
    if manifest.get("output_size_bytes") != os.path.getsize(source_path):
        raise ValueError("Kafka export byte size does not match its manifest")
    return manifest
