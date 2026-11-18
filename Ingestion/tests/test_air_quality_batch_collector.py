import unittest
from unittest.mock import Mock, patch

from Ingestion.collectors.air_quality_batch_collector import (
    fetch_air_quality_history,
    iter_hourly_air_quality_records,
)
from Ingestion.validators.normalized_schema import normalize_air_quality


class AirQualityBatchCollectorTests(unittest.TestCase):
    def test_archive_request_uses_utc_and_historical_variables(self):
        response = Mock()
        response.json.return_value = {"hourly": {"time": []}}
        location = {"city": "Hanoi", "latitude": 21.0285, "longitude": 105.8542}

        with patch(
            "Ingestion.collectors.air_quality_batch_collector.requests.get",
            return_value=response,
        ) as get:
            fetch_air_quality_history(location, "2026-11-01", "2026-11-01")

        params = get.call_args.kwargs["params"]
        self.assertEqual(params["timezone"], "UTC")
        self.assertIn("pm2_5", params["hourly"])
        self.assertIn("us_aqi", params["hourly"])

    def test_archive_hourly_values_normalize_as_utc_events(self):
        payload = {
            "latitude": 21.0285,
            "longitude": 105.8542,
            "hourly": {
                "time": ["2026-11-01T00:00"],
                "pm2_5": [42.5],
                "pm10": [55.0],
                "us_aqi": [119],
            },
        }

        raw_record = iter_hourly_air_quality_records(payload, "Hanoi")[0]
        normalized = normalize_air_quality(raw_record, source="open-meteo")

        self.assertEqual(normalized.event_time.isoformat(), "2026-11-01T00:00:00+00:00")
        self.assertEqual(normalized.city, "Hanoi")
        self.assertEqual(normalized.station_id, "open-meteo_hanoi")
        self.assertEqual(normalized.pm25, 42.5)
        self.assertEqual(normalized.aqi, 119)


if __name__ == "__main__":
    unittest.main()
