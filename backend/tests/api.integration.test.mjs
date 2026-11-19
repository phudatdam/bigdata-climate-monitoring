import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:net';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { after, before, test } from 'node:test';
import { MongoClient } from 'mongodb';

const backendDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const mongoUri = process.env.TEST_MONGODB_URI ?? 'mongodb://localhost:27017';
const testDatabaseName = 'climate_api_integration_test';
let mongo;
let db;
let apiProcess;
let baseUrl;

async function getFreePort() {
  const server = createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address();
  await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  return port;
}

async function waitForApi() {
  const deadline = Date.now() + 30_000;
  let lastError;
  while (Date.now() < deadline) {
    if (apiProcess.exitCode !== null) {
      throw new Error(`Test API exited early with code ${apiProcess.exitCode}`);
    }
    try {
      const response = await fetch(`${baseUrl}/api/health`);
      if (response.ok) return;
      lastError = new Error(`API health returned ${response.status}`);
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`Test API did not become ready: ${lastError}`);
}

before(async () => {
  mongo = new MongoClient(mongoUri, { serverSelectionTimeoutMS: 5_000 });
  await mongo.connect();
  db = mongo.db(testDatabaseName);
  await db.dropDatabase();

  const port = await getFreePort();
  baseUrl = `http://127.0.0.1:${port}`;
  apiProcess = spawn(process.execPath, [path.join(backendDir, 'dist', 'index.js')], {
    cwd: backendDir,
    env: {
      ...process.env,
      MONGODB_URI: mongoUri,
      DB_NAME: testDatabaseName,
      PORT: String(port),
      COLLECTION: 'climate_observations',
      COLLECTION_REALTIME: 'realtime_observations',
      COLLECTION_ALERTS: 'realtime_alerts',
    },
    stdio: 'ignore',
  });
  await waitForApi();

  await db.collection('climate_observations').insertMany([
    {
      _id: 'api-it-weather-day-start',
      station_id: 'hanoi',
      source: 'test',
      schema_version: 'v1',
      timestamp: new Date('2026-11-06T17:00:00.000Z'),
      temp_c: 25,
      humidity: null,
      cloud_cover: null,
      pressure: null,
    },
    {
      _id: 'api-it-weather-day-end',
      station_id: 'hanoi',
      source: 'test',
      schema_version: 'v1',
      timestamp: new Date('2026-11-07T16:59:59.999Z'),
      temp_c: 26,
      humidity: 80,
      cloud_cover: 45,
      pressure: 1012,
    },
    {
      _id: 'api-it-weather-next-day',
      station_id: 'hanoi',
      source: 'test',
      schema_version: 'v1',
      timestamp: new Date('2026-11-07T17:00:00.000Z'),
      temp_c: 27,
    },
    {
      _id: 'api-it-aqi',
      station_id: 'hanoi',
      source: 'test',
      schema_version: 'v1',
      timestamp: new Date('2026-11-07T10:00:00.000Z'),
      pm2_5: 42.5,
    },
    {
      _id: 'api-it-weather-tie',
      station_id: 'hanoi',
      city: 'Hanoi',
      source: 'open-meteo',
      schema_version: 'v1',
      timestamp: new Date('2026-11-07T10:00:00.000Z'),
      ingestion_timestamp: new Date('2026-11-07T10:05:00.000Z'),
      kafka_timestamp: new Date('2026-11-07T10:05:02.000Z'),
      event_to_ingestion_seconds: 300,
      ingestion_to_kafka_seconds: 2,
      event_to_kafka_seconds: 302,
      temp_c: 24,
      humidity: null,
    },
    {
      _id: 'api-it-irrelevant',
      station_id: 'hanoi',
      timestamp: new Date('2026-11-07T11:00:00.000Z'),
    },
  ]);
});

after(async () => {
  if (apiProcess && apiProcess.exitCode === null) {
    apiProcess.kill();
    await Promise.race([
      once(apiProcess, 'exit'),
      new Promise((resolve) => setTimeout(resolve, 3_000)),
    ]);
  }
  if (db) await db.dropDatabase();
  if (mongo) await mongo.close();
});

test('health and unknown locations have stable responses', async () => {
  const health = await fetch(`${baseUrl}/api/health`);
  assert.equal(health.status, 200);
  assert.deepEqual(await health.json(), { status: 'ok' });

  const unknownLocation = await fetch(`${baseUrl}/api/weather/hourly/unknown-city`);
  assert.equal(unknownLocation.status, 400);
  assert.deepEqual(await unknownLocation.json(), { error: 'Unknown location' });
});

test('date parameters must be real YYYY-MM-DD calendar dates', async () => {
  for (const endpoint of [
    '/api/weather/hourly/hanoi',
    '/api/air-quality/hanoi',
    '/api/forecast/hanoi',
  ]) {
    const response = await fetch(`${baseUrl}${endpoint}?date=2026-02-30`);
    assert.equal(response.status, 400, endpoint);
    assert.match((await response.json()).error, /real YYYY-MM-DD date/);
  }
});

test('Vietnam calendar day includes its final millisecond and excludes next midnight', async () => {
  const response = await fetch(
    `${baseUrl}/api/weather/hourly/hanoi?date=2026-11-07`,
  );
  assert.equal(response.status, 200);
  const records = await response.json();
  assert.deepEqual(
    records.map((record) => record.timestamp),
    [
      '2026-11-06T17:00:00.000Z',
      '2026-11-07T10:00:00.000Z',
      '2026-11-07T16:59:59.999Z',
    ],
  );
  assert.equal(records[0].humidity, null);
  assert.equal(records[0].pressure, null);
  assert.equal(records[0].condition, 'Unavailable');
  assert.equal(records[0].source, 'test');
  assert.equal(records[0].schemaVersion, 'v1');
  assert.equal(records[0].time, '00:00');
  assert.equal(records[0].temperature, 25);
});

test('AQI response preserves nullable derived values', async () => {
  const response = await fetch(
    `${baseUrl}/api/air-quality/hanoi?date=2026-11-07`,
  );
  assert.equal(response.status, 200);
  const [record] = await response.json();
  assert.equal(record.pm25, 42.5);
  assert.equal(record.aqi, null);
  assert.equal(record.pm25Mean12h, null);
  assert.equal(record.isStagnantAir, null);
  assert.equal(record.timestamp, '2026-11-07T10:00:00.000Z');
  assert.equal(record.pm25, 42.5);
  assert.equal(record.source, 'test');
  assert.equal(record.schemaVersion, 'v1');
});

test('forecast remains unavailable until forecast values exist', async () => {
  const unavailable = await fetch(
    `${baseUrl}/api/forecast/hanoi?date=2026-11-07`,
  );
  assert.equal(unavailable.status, 404);
  assert.deepEqual(await unavailable.json(), {
    error: 'Forecast data is not available for this location and time.',
  });

  const invalidHour = await fetch(
    `${baseUrl}/api/forecast/hanoi?hour=24`,
  );
  assert.equal(invalidHour.status, 400);
});

test('valid date with no observations returns an empty array', async () => {
  for (const endpoint of [
    '/api/weather/hourly/hanoi',
    '/api/air-quality/hanoi',
  ]) {
    const response = await fetch(`${baseUrl}${endpoint}?date=2000-01-01`);
    assert.equal(response.status, 200, endpoint);
    assert.deepEqual(await response.json(), []);
  }
});

test('history requires an ordered, bounded, real date range', async () => {
  const cases = [
    ['?startDate=2026-11-07', /startDate and endDate/],
    ['?startDate=2026-02-30&endDate=2026-03-01', /real YYYY-MM-DD/],
    ['?startDate=2026-11-08&endDate=2026-11-07', /on or before/],
    ['?startDate=2026-01-01&endDate=2026-02-01', /cannot exceed 31/],
  ];
  for (const [query, message] of cases) {
    const response = await fetch(`${baseUrl}/api/history/hanoi${query}`);
    assert.equal(response.status, 400, query);
    assert.match((await response.json()).error, message);
  }
});

test('history pages are stable for equal timestamps and preserve nullable observations', async () => {
  const firstResponse = await fetch(
    `${baseUrl}/api/history/hanoi?startDate=2026-11-07&endDate=2026-11-07&limit=2`,
  );
  assert.equal(firstResponse.status, 200);
  const firstPage = await firstResponse.json();
  assert.equal(firstPage.pageSize, 2);
  assert.ok(firstPage.nextCursor);
  assert.deepEqual(
    firstPage.records.map((record) => record.id),
    ['api-it-weather-day-start', 'api-it-aqi'],
  );
  assert.equal(firstPage.records[1].recordType, 'air_quality');
  assert.equal(firstPage.records[1].temperature, null);
  assert.equal(firstPage.records[1].pm25, 42.5);

  const secondResponse = await fetch(
    `${baseUrl}/api/history/hanoi?startDate=2026-11-07&endDate=2026-11-07&limit=2&cursor=${encodeURIComponent(firstPage.nextCursor)}`,
  );
  assert.equal(secondResponse.status, 200);
  const secondPage = await secondResponse.json();
  assert.equal(secondPage.nextCursor, null);
  assert.deepEqual(
    secondPage.records.map((record) => record.id),
    ['api-it-weather-tie', 'api-it-weather-day-end'],
  );
  assert.equal(secondPage.records[0].recordType, 'weather');
  assert.equal(secondPage.records[0].humidity, null);
  assert.equal(secondPage.records[0].ingestionTimestamp, '2026-11-07T10:05:00.000Z');
  assert.equal(secondPage.records[0].kafkaTimestamp, '2026-11-07T10:05:02.000Z');
  assert.equal(secondPage.records[0].eventToKafkaSeconds, 302);

  const invalidCursor = await fetch(
    `${baseUrl}/api/history/hanoi?startDate=2026-11-07&endDate=2026-11-07&cursor=invalid`,
  );
  assert.equal(invalidCursor.status, 400);
  assert.deepEqual(await invalidCursor.json(), { error: 'Invalid history cursor.' });
});

test('history page limits are validated and unrelated documents are excluded', async () => {
  for (const limit of ['0', '201', '1.5', 'many']) {
    const response = await fetch(
      `${baseUrl}/api/history/hanoi?startDate=2026-11-07&endDate=2026-11-07&limit=${limit}`,
    );
    assert.equal(response.status, 400, limit);
  }
  const oneRecord = await fetch(
    `${baseUrl}/api/history/hanoi?startDate=2026-11-07&endDate=2026-11-07&limit=1`,
  );
  const page = await oneRecord.json();
  assert.equal(page.records.length, 1);
  assert.equal(page.records[0].id, 'api-it-weather-day-start');
});

test('data-quality endpoint returns complete quality report with flow, completeness, and plausibility', async () => {
  const response = await fetch(
    `${baseUrl}/api/data-quality/hanoi?startDate=2026-11-07&endDate=2026-11-07`,
  );
  assert.equal(response.status, 200);
  const report = await response.json();

  assert.equal(report.stationId, 'hanoi');
  assert.equal(report.locationName, 'hanoi');
  assert.equal(report.window.startDate, '2026-11-07');
  assert.equal(report.window.endDate, '2026-11-07');
  assert.equal(report.window.expectedHours, 24);

  // Flow metrics
  assert.ok(typeof report.flow.isFlowing === 'boolean');
  assert.ok(report.flow.weather.count >= 1);
  assert.ok(report.flow.airQuality.count >= 1);

  // Completeness & attributes
  assert.ok(typeof report.completeness.overallCompletenessPct === 'number');
  assert.ok(typeof report.completeness.attributes.temperaturePct === 'number');
  assert.ok(typeof report.completeness.attributes.pm25Pct === 'number');
  assert.ok(Array.isArray(report.completeness.gaps));

  // Plausibility
  assert.ok(typeof report.plausibility.scorePct === 'number');
  assert.ok(report.plausibility.totalRecordsEvaluated >= 3);
  assert.ok(Array.isArray(report.plausibility.violations));

  // Diagnostics
  assert.ok(['A', 'B', 'C', 'D', 'F'].includes(report.diagnostics.healthGrade));
  assert.ok(Array.isArray(report.diagnostics.summary));
  assert.ok(Array.isArray(report.diagnostics.suggestedBackfills));
});

test('data-quality endpoint validates location and date bounds', async () => {
  const unknownLoc = await fetch(`${baseUrl}/api/data-quality/unknown_city`);
  assert.equal(unknownLoc.status, 400);

  const invalidDates = await fetch(
    `${baseUrl}/api/data-quality/hanoi?startDate=2026-11-08&endDate=2026-11-07`,
  );
  assert.equal(invalidDates.status, 400);
});

test('data-quality-summary returns fleet-wide coverage', async () => {
  const response = await fetch(`${baseUrl}/api/data-quality-summary`);
  assert.equal(response.status, 200);
  const summary = await response.json();
  assert.ok(summary.totalConfiguredLocations >= 60);
  assert.ok(Array.isArray(summary.locations));
  const hanoiSummary = summary.locations.find((l) => l.stationId === 'hanoi');
  assert.ok(hanoiSummary);
});
