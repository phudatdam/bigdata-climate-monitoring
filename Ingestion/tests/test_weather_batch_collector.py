import unittest
from unittest.mock import Mock, patch

from Ingestion.collectors.weather_batch_collector import (
    fetch_weather_history,
    iter_hourly_weather_records,
)
from Ingestion.validators.normalized_schema import normalize_weather


class WeatherBatchCollectorTests(unittest.TestCase):
    def test_archive_request_uses_utc_for_normalized_event_timestamps(self):
        response = Mock()
        response.json.return_value = {"hourly": {"time": []}}
        location = {"city": "Hanoi", "latitude": 21.0285, "longitude": 105.8542}

        with patch("Ingestion.collectors.weather_batch_collector.requests.get", return_value=response) as get:
            fetch_weather_history(location, "2026-11-01", "2026-11-01")

        self.assertEqual(get.call_args.kwargs["params"]["timezone"], "UTC")

    def test_archive_hourly_values_normalize_as_utc_events(self):
        payload = {
            "latitude": 21.0285,
            "longitude": 105.8542,
            "hourly": {
                "time": ["2026-11-01T00:00"],
                "temperature_2m": [25.0],
                "relative_humidity_2m": [70],
                "wind_speed_10m": [18.0],
                "weather_code": [1],
            },
        }

        raw_record = iter_hourly_weather_records(payload, "Hanoi")[0]
        normalized = normalize_weather(raw_record, source="open-meteo")

        self.assertEqual(normalized.event_time.isoformat(), "2026-11-01T00:00:00+00:00")
        self.assertEqual(normalized.temperature_c, 25.0)
        self.assertAlmostEqual(normalized.wind_speed_mps, 5.00004, places=4)


if __name__ == "__main__":
    unittest.main()
