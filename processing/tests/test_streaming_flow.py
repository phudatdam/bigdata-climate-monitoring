import json
import math
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from processing.batch.historical_flow import EXPORT_SCHEMA, transform_export_rows
from processing.batch.parquet_archive import write_partitioned_snapshot
from processing.streaming.streaming_flow import build_feature_frame, canonical_city


class StreamingFlowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("climate-streaming-flow-tests")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_canonical_city_prefers_alias_then_coordinates(self):
        self.assertEqual(canonical_city("Ha Noi", None, None), "Hanoi")
        self.assertEqual(
            canonical_city(
                "United Nations International School of Hanoi, Vietnam",
                21.0285,
                105.8542,
            ),
            "Hanoi",
        )
        self.assertIsNone(canonical_city(None, 0.0, 0.0))

    def test_parses_weather_and_air_quality_independently_and_keeps_nulls(self):
        event_time = "2026-11-07T10:00:00Z"
        weather = {
            "source": "open-meteo",
            "region": "bac",
            "city": "Ha Noi",
            "station_id": "Ha Noi",
            "latitude": 21.0285,
            "longitude": 105.8542,
            "event_time": event_time,
            "data_version": "v1",
            "temperature_c": 24.5,
            "humidity": 70.0,
            "pressure_hpa": 1018.0,
        }
        air_quality = {
            "source": "aqicn",
            "region": "bac",
            "city": "United Nations International School of Hanoi, Vietnam",
            "station_id": "United Nations International School of Hanoi, Vietnam",
            "latitude": 21.0285,
            "longitude": 105.8542,
            "event_time": event_time,
            "data_version": "v1",
            "aqi": 119.0,
            "pm25": 42.0,
        }
        rows = [
            ("weather.raw.stream", json.dumps(weather)),
            ("air_quality.raw.stream", json.dumps(air_quality)),
            ("weather.raw.stream", "{malformed"),
        ]
        kafka_rows = self.spark.createDataFrame(rows, ["topic", "value"])

        result = build_feature_frame(kafka_rows).collect()
        valid = [row for row in result if row["_valid_record"]]
        invalid = [row for row in result if not row["_valid_record"]]

        self.assertEqual(len(valid), 2)
        self.assertEqual(len(invalid), 1)
        by_source = {row["source"]: row for row in valid}
        weather_row = by_source["open-meteo"]
        air_row = by_source["aqicn"]

        self.assertEqual(weather_row["station_id"], "hanoi")
        self.assertEqual(weather_row["schema_version"], "v1")
        self.assertEqual(weather_row["temp_c"], 24.5)
        self.assertIsNone(weather_row["wind_speed"])
        self.assertIsNone(weather_row["pm2_5"])
        self.assertAlmostEqual(weather_row["air_density"], 1.18, places=1)
        self.assertAlmostEqual(
            weather_row["hour_sin"],
            math.sin(2.0 * math.pi * 17.0 / 24.0),
            places=5,
        )
        self.assertAlmostEqual(
            weather_row["coord_X"] ** 2
            + weather_row["coord_Y"] ** 2
            + weather_row["coord_Z"] ** 2,
            1.0,
            places=5,
        )
        self.assertEqual(air_row["station_id"], "hanoi")
        self.assertEqual(air_row["schema_version"], "v1")
        self.assertIsNone(air_row["temp_c"])
        self.assertEqual(air_row["pm2_5"], 42.0)
        self.assertEqual(air_row["us_aqi"], 119.0)
        self.assertEqual(weather_row["timestamp"].isoformat(), "2026-11-07T10:00:00")
        self.assertIn("open-meteo", weather_row["_id"])
        self.assertIn("aqicn", air_row["_id"])

        replayed = {
            row["source"]: row["_id"]
            for row in build_feature_frame(kafka_rows).collect()
            if row["_valid_record"]
        }
        self.assertEqual(replayed["open-meteo"], weather_row["_id"])
        self.assertEqual(replayed["aqicn"], air_row["_id"])

    def test_weather_batch_topic_uses_the_weather_contract(self):
        record = {
            "source": "open-meteo",
            "region": "bac",
            "city": "Hanoi",
            "latitude": 21.0285,
            "longitude": 105.8542,
            "event_time": "2026-11-01T00:00:00",
            "data_version": "v1",
            "temperature_c": 25.0,
            "humidity": 70.0,
        }

        observations = build_feature_frame(
            self.spark.createDataFrame(
                [
                    ("weather.raw.batch", json.dumps(record)),
                    ("weather.raw.stream", json.dumps(record)),
                ],
                ["topic", "value"],
            )
        ).collect()

        self.assertEqual(len(observations), 2)
        self.assertTrue(all(row["_valid_record"] for row in observations))
        self.assertTrue(all(row["timestamp"].isoformat() == "2026-11-01T00:00:00" for row in observations))
        self.assertTrue(all(row["station_id"] == "hanoi" for row in observations))
        self.assertTrue(all(row["temp_c"] == 25.0 for row in observations))
        self.assertEqual(observations[0]["_id"], observations[1]["_id"])

    def test_air_quality_batch_topic_uses_the_air_quality_contract(self):
        record = {
            "source": "open-meteo",
            "region": "bac",
            "city": "Hanoi",
            "latitude": 21.0285,
            "longitude": 105.8542,
            "event_time": "2026-11-01T00:00:00Z",
            "data_version": "v1",
            "pm25": 42.5,
            "pm10": 55.0,
            "aqi": 119.0,
        }

        observations = build_feature_frame(
            self.spark.createDataFrame(
                [
                    ("air_quality.raw.batch", json.dumps(record)),
                    ("air_quality.raw.stream", json.dumps(record)),
                ],
                ["topic", "value"],
            )
        ).collect()

        self.assertEqual(len(observations), 2)
        self.assertTrue(all(row["_valid_record"] for row in observations))
        self.assertTrue(all(row["station_id"] == "hanoi" for row in observations))
        self.assertTrue(all(row["pm2_5"] == 42.5 for row in observations))
        self.assertTrue(all(row["us_aqi"] == 119.0 for row in observations))
        self.assertEqual(observations[0]["_id"], observations[1]["_id"])

    def test_weather_and_air_quality_ids_do_not_collide(self):
        weather = {
            "source": "open-meteo",
            "region": "bac",
            "city": "Hanoi",
            "latitude": 21.0285,
            "longitude": 105.8542,
            "event_time": "2026-11-01T00:00:00Z",
            "temperature_c": 25.0,
        }
        air = {
            "source": "open-meteo",
            "region": "bac",
            "city": "Hanoi",
            "latitude": 21.0285,
            "longitude": 105.8542,
            "event_time": "2026-11-01T00:00:00Z",
            "pm25": 42.5,
            "aqi": 119.0,
        }

        observations = build_feature_frame(
            self.spark.createDataFrame(
                [
                    ("weather.raw.batch", json.dumps(weather)),
                    ("air_quality.raw.batch", json.dumps(air)),
                ],
                ["topic", "value"],
            )
        ).filter("_valid_record").collect()

        self.assertEqual(len(observations), 2)
        self.assertNotEqual(observations[0]["_id"], observations[1]["_id"])

    def test_pipeline_latency_uses_event_ingestion_and_kafka_timestamps(self):
        record = {
            "source": "open-meteo",
            "region": "bac",
            "city": "Hanoi",
            "latitude": 21.0285,
            "longitude": 105.8542,
            "event_time": "2026-11-07T10:00:00Z",
            "ingestion_time": "2026-11-07T10:05:00Z",
            "temperature_c": 24.5,
        }
        kafka_time_ms = int(
            datetime(2026, 11, 7, 10, 5, 2, tzinfo=timezone.utc).timestamp() * 1000
        )
        observation = build_feature_frame(
            self.spark.createDataFrame(
                [
                    ("weather.raw.stream", json.dumps(record), kafka_time_ms),
                ],
                ["topic", "value", "kafka_timestamp_ms"],
            )
        ).first()

        self.assertEqual(observation["event_to_ingestion_seconds"], 300.0)
        self.assertEqual(observation["ingestion_to_kafka_seconds"], 2.0)
        self.assertEqual(observation["event_to_kafka_seconds"], 302.0)
        self.assertEqual(
            observation["kafka_timestamp"].isoformat(),
            "2026-11-07T10:05:02",
        )

    def test_batch_processor_reads_export_envelopes_and_deduplicates(self):
        record = {
            "source": "open-meteo",
            "region": "bac",
            "city": "Hanoi",
            "latitude": 21.0285,
            "longitude": 105.8542,
            "event_time": "2026-11-07T10:00:00Z",
            "temperature_c": 24.5,
            "humidity": 70.0,
            "pressure_hpa": 1018.0,
        }
        export_rows = self.spark.createDataFrame(
            [
                {
                    "kafka_topic": "weather.raw.stream",
                    "kafka_partition": 1,
                    "kafka_offset": 0,
                    "value": record,
                },
                {
                    "kafka_topic": "weather.raw.stream",
                    "kafka_partition": 1,
                    "kafka_offset": 0,
                    "value": record,
                },
            ],
            schema=EXPORT_SCHEMA,
        )

        observations = transform_export_rows(export_rows).collect()

        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0]["station_id"], "hanoi")
        self.assertIsNotNone(observations[0]["dew_point"])
        self.assertIsNotNone(observations[0]["air_density"])
        self.assertAlmostEqual(observations[0]["temp_mean_6h"], 24.5)

    def test_parquet_snapshot_is_partitioned_and_safe_to_replay(self):
        with tempfile.TemporaryDirectory() as archive_root:
            records = self.spark.createDataFrame(
                [
                    ("weather-1", "2026", "11", "08", "open-meteo"),
                    ("air-1", "2026", "11", "08", "aqicn"),
                ],
                ["event_id", "year", "month", "day", "source"],
            )
            source_manifest = {
                "source_file_sha256": "snapshot-hash",
                "partitions": [{"topic": "weather.raw.stream", "partition": 0}],
            }

            first_write = write_partitioned_snapshot(
                records,
                archive_root,
                "curated_observations",
                "snapshot-hash",
                source_manifest,
                2,
            )
            replay_write = write_partitioned_snapshot(
                records,
                archive_root,
                "curated_observations",
                "snapshot-hash",
                source_manifest,
                2,
            )
            snapshot_path = (
                Path(archive_root)
                / "curated_observations"
                / "snapshot-hash"
            )
            archived = self.spark.read.parquet(str(snapshot_path / "data"))

            self.assertTrue(first_write)
            self.assertFalse(replay_write)
            self.assertEqual(archived.count(), 2)
            self.assertEqual(
                {row["source"] for row in archived.select("source").collect()},
                {"open-meteo", "aqicn"},
            )
            self.assertTrue(
                (
                    snapshot_path
                    / "data"
                    / "year=2026"
                    / "month=11"
                    / "day=08"
                    / "source=aqicn"
                ).is_dir()
            )

    def test_batch_windows_use_elapsed_time_not_record_counts(self):
        base_time = "2026-11-08T12:00:00Z"
        fixtures = [
            ("weather.raw.stream", "2026-11-08T05:59:59Z", 10.0, None, 1000.0),
            ("weather.raw.stream", "2026-11-08T06:00:00Z", 20.0, None, 1015.0),
            ("weather.raw.stream", "2026-11-08T09:00:00Z", 30.0, None, 1010.0),
            ("weather.raw.stream", base_time, 40.0, None, 1000.0),
            ("air_quality.raw.stream", "2026-11-07T23:59:59Z", None, 100.0, None),
            ("air_quality.raw.stream", "2026-11-08T00:00:00Z", None, 12.0, None),
            ("air_quality.raw.stream", "2026-11-08T06:00:00Z", None, 24.0, None),
            ("air_quality.raw.stream", base_time, None, 36.0, None),
        ]
        rows = []
        for index, (topic, event_time, temperature, pm25, pressure) in enumerate(fixtures):
            is_weather = topic == "weather.raw.stream"
            record = {
                "event_id": f"window-test-{index}",
                "source": "open-meteo" if is_weather else "aqicn",
                "region": "bac",
                "city": "Hanoi",
                "latitude": 21.0285,
                "longitude": 105.8542,
                "event_time": event_time,
                "temperature_c": temperature,
                "humidity": 70.0 if is_weather else None,
                "pressure_hpa": pressure,
                "aqi": pm25 if not is_weather else None,
                "pm25": pm25,
            }
            rows.append(
                {
                    "kafka_topic": topic,
                    "kafka_partition": 0,
                    "kafka_offset": index,
                    "value": record,
                }
            )

        rows = [
            rows[3],
            rows[1],
            rows[0],
            rows[2],
            rows[7],
            rows[5],
            rows[4],
            rows[6],
            rows[7],
        ]
        export_rows = self.spark.createDataFrame(rows, schema=EXPORT_SCHEMA)
        observations = transform_export_rows(export_rows).collect()
        by_source = {
            (row["source"], row["timestamp"].isoformat()): row
            for row in observations
        }
        weather_result = by_source[("open-meteo", "2026-11-08T12:00:00")]
        aqi_result = by_source[("aqicn", "2026-11-08T12:00:00")]

        self.assertAlmostEqual(weather_result["temp_mean_6h"], 30.0)
        self.assertAlmostEqual(weather_result["pressure_delta_3h"], -10.0)
        self.assertAlmostEqual(aqi_result["pm25_mean_12h"], 24.0)

    def test_windows_are_source_isolated_and_recompute_after_late_events(self):
        def exported_row(topic, source, timestamp, temperature=None, pm25=None, pressure=None, offset=0):
            return {
                "kafka_topic": topic,
                "kafka_partition": 0,
                "kafka_offset": offset,
                "value": {
                    "event_id": f"{source}-{timestamp}",
                    "source": source,
                    "region": "bac",
                    "city": "Hanoi",
                    "latitude": 21.0285,
                    "longitude": 105.8542,
                    "event_time": timestamp,
                    "temperature_c": temperature,
                    "humidity": 70.0 if temperature is not None else None,
                    "pressure_hpa": pressure,
                    "wind_speed_mps": temperature,
                    "wind_direction_deg": 0.0,
                    "aqi": pm25,
                    "pm25": pm25,
                },
            }

        base_rows = [
            exported_row("weather.raw.stream", "open-meteo", "2026-11-08T06:00:00Z", 10.0, pressure=1010.0, offset=0),
            exported_row("weather.raw.stream", "open-meteo", "2026-11-08T12:00:00Z", 30.0, pressure=1000.0, offset=2),
            exported_row("weather.raw.stream", "other-weather", "2026-11-08T12:00:00Z", 90.0, pressure=990.0, offset=3),
            exported_row("air_quality.raw.stream", "aqicn", "2026-11-08T00:00:00Z", pm25=10.0, offset=4),
            exported_row("air_quality.raw.stream", "aqicn", "2026-11-08T12:00:00Z", pm25=30.0, offset=5),
            exported_row("air_quality.raw.stream", "iqair", "2026-11-08T00:00:00Z", pm25=100.0, offset=6),
            exported_row("air_quality.raw.stream", "iqair", "2026-11-08T12:00:00Z", pm25=200.0, offset=7),
        ]
        late_weather = exported_row(
            "weather.raw.stream",
            "open-meteo",
            "2026-11-08T09:00:00Z",
            50.0,
            pressure=1012.0,
            offset=1,
        )

        def transform(rows):
            return transform_export_rows(
                self.spark.createDataFrame(rows, schema=EXPORT_SCHEMA)
            ).collect()

        earlier_result = transform(base_rows)
        open_meteo_at_noon = next(
            row for row in earlier_result
            if row["source"] == "open-meteo" and row["timestamp"].hour == 12
        )
        self.assertAlmostEqual(open_meteo_at_noon["temp_mean_6h"], 20.0)
        self.assertIsNone(open_meteo_at_noon["pressure_delta_3h"])

        replay_rows = [base_rows[1], late_weather, *base_rows[:1], *base_rows[2:], late_weather]
        replay_result = transform(replay_rows)
        replayed_noon = next(
            row for row in replay_result
            if row["source"] == "open-meteo" and row["timestamp"].hour == 12
        )
        alternate_weather = next(
            row for row in replay_result
            if row["source"] == "other-weather" and row["timestamp"].hour == 12
        )
        aqicn_noon = next(
            row for row in replay_result
            if row["source"] == "aqicn" and row["timestamp"].hour == 12
        )
        iqair_noon = next(
            row for row in replay_result
            if row["source"] == "iqair" and row["timestamp"].hour == 12
        )

        self.assertEqual(replayed_noon["_id"], open_meteo_at_noon["_id"])
        self.assertAlmostEqual(replayed_noon["temp_mean_6h"], 30.0)
        self.assertAlmostEqual(replayed_noon["pressure_delta_3h"], -12.0)
        self.assertAlmostEqual(alternate_weather["temp_mean_6h"], 90.0)
        self.assertAlmostEqual(aqicn_noon["pm25_mean_12h"], 20.0)
        self.assertAlmostEqual(iqair_noon["pm25_mean_12h"], 150.0)


if __name__ == "__main__":
    unittest.main()
