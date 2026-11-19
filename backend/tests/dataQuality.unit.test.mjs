import assert from 'node:assert/strict';
import { test } from 'node:test';
import { computeDataQualityReport } from '../dist/dataQuality.js';

test('computeDataQualityReport detects intermittent gaps between successful records', () => {
  const windowStart = new Date('2026-11-10T00:00:00.000Z');
  const windowEnd = new Date('2026-11-10T23:59:59.999Z');

  // Synthetic records: continuous from 00:00 to 03:00, GAP from 03:00 to 09:00 (5 missing hours), continuous from 09:00 to 12:00
  const docs = [
    {
      _id: '1',
      station_id: 'hanoi',
      source: 'open-meteo',
      timestamp: new Date('2026-11-10T00:00:00.000Z'),
      temp_c: 24,
      humidity: 75,
      pressure: 1012,
      wind_speed: 3.5,
      event_to_ingestion_seconds: 120,
      ingestion_to_kafka_seconds: 2,
      event_to_kafka_seconds: 122,
    },
    {
      _id: '2',
      station_id: 'hanoi',
      source: 'open-meteo',
      timestamp: new Date('2026-11-10T01:00:00.000Z'),
      temp_c: 23.5,
      humidity: 78,
      pressure: 1012,
      wind_speed: 3.2,
      event_to_ingestion_seconds: 110,
      ingestion_to_kafka_seconds: 1,
      event_to_kafka_seconds: 111,
    },
    {
      _id: '3',
      station_id: 'hanoi',
      source: 'open-meteo',
      timestamp: new Date('2026-11-10T02:00:00.000Z'),
      temp_c: 23.0,
      humidity: 80,
      pressure: 1011,
      wind_speed: 2.8,
      event_to_ingestion_seconds: 115,
      ingestion_to_kafka_seconds: 2,
      event_to_kafka_seconds: 117,
    },
    // NOTE: 03:00, 04:00, 05:00, 06:00, 07:00, 08:00 are MISSING!
    {
      _id: '4',
      station_id: 'hanoi',
      source: 'open-meteo',
      timestamp: new Date('2026-11-10T09:00:00.000Z'),
      temp_c: 26.0,
      humidity: 70,
      pressure: 1013,
      wind_speed: 4.1,
      event_to_ingestion_seconds: 130,
      ingestion_to_kafka_seconds: 2,
      event_to_kafka_seconds: 132,
    },
    {
      _id: '5',
      station_id: 'hanoi',
      source: 'open-meteo',
      timestamp: new Date('2026-11-10T10:00:00.000Z'),
      temp_c: 27.5,
      humidity: 68,
      pressure: 1013,
      wind_speed: 4.5,
      event_to_ingestion_seconds: 125,
      ingestion_to_kafka_seconds: 1,
      event_to_kafka_seconds: 126,
    },
  ];

  const report = computeDataQualityReport({
    stationId: 'hanoi',
    locationKey: 'hanoi',
    collection: 'climate_observations',
    docs,
    windowStart,
    windowEnd,
    mode: 'single_day',
    startDateStr: '2026-11-10',
    endDateStr: '2026-11-10',
  });

  // Verify internal gap is detected between 02:00 and 09:00
  assert.equal(report.stationId, 'hanoi');
  assert.ok(report.completeness.gaps.length >= 1);

  const internalGap = report.completeness.gaps.find((g) => g.type === 'internal_gap');
  assert.ok(internalGap, 'Internal gap must be found');
  assert.equal(internalGap.stream, 'weather');
  assert.equal(internalGap.gapStart, '2026-11-10T02:00:00.000Z');
  assert.equal(internalGap.gapEnd, '2026-11-10T09:00:00.000Z');
  assert.equal(internalGap.missingHours, 6);

  // Suggested backfill
  assert.ok(report.diagnostics.suggestedBackfills.length >= 1);
  const backfill = report.diagnostics.suggestedBackfills[0];
  assert.match(backfill.suggestedCommand, /run_weather_backfill\.ps1/);
  assert.match(backfill.suggestedCommand, /2026-11-10/);

  // All 5 records are physically valid
  assert.equal(report.plausibility.scorePct, 100);
  assert.equal(report.plausibility.violationCount, 0);

  // Latencies
  assert.equal(report.flow.latencySeconds.eventToIngestion.avg, 120);
});

test('computeDataQualityReport flags physical domain violations and anomalies', () => {
  const windowStart = new Date('2026-11-10T00:00:00.000Z');
  const windowEnd = new Date('2026-11-10T12:00:00.000Z');

  const docs = [
    {
      _id: 'valid',
      station_id: 'hanoi',
      source: 'open-meteo',
      timestamp: new Date('2026-11-10T01:00:00.000Z'),
      temp_c: 25.0,
      humidity: 70,
      pressure: 1012,
    },
    {
      _id: 'invalid-temp',
      station_id: 'hanoi',
      source: 'open-meteo',
      timestamp: new Date('2026-11-10T02:00:00.000Z'),
      temp_c: 62.0, // Impassible terrestrial temperature (> 55°C)
      humidity: 125, // Impossible humidity (> 100%)
      pressure: 750, // Impossible pressure (< 870 hPa)
    },
    {
      _id: 'invalid-air',
      station_id: 'hanoi',
      source: 'aqicn',
      timestamp: new Date('2026-11-10T02:00:00.000Z'),
      pm2_5: -5.0, // Negative concentration
      us_aqi: 650, // Exceeds 500 EPA scale
    },
  ];

  const report = computeDataQualityReport({
    stationId: 'hanoi',
    locationKey: 'hanoi',
    collection: 'climate_observations',
    docs,
    windowStart,
    windowEnd,
    mode: 'single_day',
    startDateStr: '2026-11-10',
    endDateStr: '2026-11-10',
  });

  assert.equal(report.plausibility.totalRecordsEvaluated, 3);
  assert.ok(report.plausibility.violationCount >= 4);
  assert.ok(report.plausibility.scorePct < 100);

  const tempViolation = report.plausibility.violations.find((v) => v.rule === 'TEMP_RANGE');
  assert.ok(tempViolation);
  assert.equal(tempViolation.value, 62);

  const humidityViolation = report.plausibility.violations.find((v) => v.rule === 'HUMIDITY_RANGE');
  assert.ok(humidityViolation);
  assert.equal(humidityViolation.value, 125);

  const pm25Violation = report.plausibility.violations.find((v) => v.rule === 'PM25_RANGE');
  assert.ok(pm25Violation);
  assert.equal(pm25Violation.value, -5);
});
