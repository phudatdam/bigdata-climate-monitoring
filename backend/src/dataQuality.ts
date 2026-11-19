import type { Document } from 'mongodb';

export interface LatencyMetric {
  avg: number | null;
  p95: number | null;
  max: number | null;
}

export interface StreamFlowStatus {
  count: number;
  latestTimestamp: string | null;
  freshnessMinutes: number | null;
  status: 'fresh' | 'stale' | 'degraded' | 'inactive';
}

export interface InternalGap {
  stream: 'weather' | 'air_quality';
  gapStart: string;
  gapEnd: string;
  missingHours: number;
  type: 'internal_gap' | 'leading_gap' | 'trailing_gap';
  description: string;
}

export interface PlausibilityViolation {
  timestamp: string;
  stream: 'weather' | 'air_quality';
  source: string | null;
  rule: string;
  field: string;
  value: number | string | null;
  expected: string;
  severity: 'warning' | 'error';
  message: string;
}

export interface SuggestedBackfill {
  startDate: string;
  endDate: string;
  missingHours: number;
  streams: Array<'weather' | 'air_quality'>;
  locationKey: string;
  stationId: string;
  reason: string;
  suggestedCommand: string;
}

export interface DataQualityReport {
  stationId: string;
  locationName: string;
  collection: string;
  window: {
    startDate: string;
    endDate: string;
    startIso: string;
    endIso: string;
    expectedHours: number;
    mode: 'live' | 'range' | 'single_day';
  };
  flow: {
    isFlowing: boolean;
    status: 'optimal' | 'lagging' | 'stale' | 'inactive';
    weather: StreamFlowStatus;
    airQuality: StreamFlowStatus;
    latencySeconds: {
      eventToIngestion: LatencyMetric;
      ingestionToKafka: LatencyMetric;
      eventToKafka: LatencyMetric;
      clockSkewDetected: boolean;
    };
  };
  completeness: {
    overallCompletenessPct: number;
    weatherCompletenessPct: number;
    airQualityCompletenessPct: number;
    weatherHoursPresent: number;
    airQualityHoursPresent: number;
    attributes: {
      temperaturePct: number;
      humidityPct: number;
      pressurePct: number;
      windSpeedPct: number;
      precipitationPct: number;
      pm25Pct: number;
      aqiPct: number;
    };
    gaps: InternalGap[];
  };
  plausibility: {
    scorePct: number;
    totalRecordsEvaluated: number;
    validRecordsCount: number;
    violationCount: number;
    violations: PlausibilityViolation[];
  };
  diagnostics: {
    healthGrade: 'A' | 'B' | 'C' | 'D' | 'F';
    summary: string[];
    suggestedBackfills: SuggestedBackfill[];
  };
}

function calculateMetric(values: number[]): LatencyMetric {
  if (values.length === 0) {
    return { avg: null, p95: null, max: null };
  }
  const sum = values.reduce((acc, v) => acc + v, 0);
  const avg = Math.round((sum / values.length) * 10) / 10;
  const sorted = [...values].sort((a, b) => a - b);
  const p95Index = Math.min(sorted.length - 1, Math.floor(sorted.length * 0.95));
  const p95 = Math.round(sorted[p95Index] * 10) / 10;
  const max = Math.round(sorted[sorted.length - 1] * 10) / 10;
  return { avg, p95, max };
}

function toVietnamIsoDate(date: Date): string {
  // Format as YYYY-MM-DD in UTC+7
  const utc7 = new Date(date.getTime() + 7 * 60 * 60 * 1000);
  return utc7.toISOString().slice(0, 10);
}

export function computeDataQualityReport(params: {
  stationId: string;
  locationKey: string;
  collection: string;
  docs: Document[];
  windowStart: Date;
  windowEnd: Date;
  mode: 'live' | 'range' | 'single_day';
  startDateStr: string;
  endDateStr: string;
}): DataQualityReport {
  const {
    stationId,
    locationKey,
    collection,
    docs,
    windowStart,
    windowEnd,
    mode,
    startDateStr,
    endDateStr,
  } = params;

  const now = Date.now();
  const windowDurationMs = windowEnd.getTime() - windowStart.getTime();
  const expectedHours = Math.max(1, Math.round(windowDurationMs / (3600 * 1000)));

  // Separate records into weather and air quality
  const weatherDocs: Document[] = [];
  const airDocs: Document[] = [];

  for (const doc of docs) {
    const isWeather =
      typeof doc.temp_c === 'number' ||
      doc.source === 'open-meteo' ||
      doc._record_type === 'weather' ||
      doc.kafka_topic === 'weather.raw.stream' ||
      doc.kafka_topic === 'weather.raw.batch';

    const isAir =
      typeof doc.pm2_5 === 'number' ||
      typeof doc.us_aqi === 'number' ||
      doc.source === 'aqicn' ||
      doc._record_type === 'air_quality' ||
      doc.kafka_topic === 'air_quality.raw.stream' ||
      doc.kafka_topic === 'air_quality.raw.batch';

    if (isWeather) weatherDocs.push(doc);
    if (isAir) airDocs.push(doc);
  }

  // 1. Flow & Latencies
  const eventToIngestionArr: number[] = [];
  const ingestionToKafkaArr: number[] = [];
  const eventToKafkaArr: number[] = [];
  let clockSkewDetected = false;

  for (const doc of docs) {
    if (typeof doc.event_to_ingestion_seconds === 'number') {
      eventToIngestionArr.push(doc.event_to_ingestion_seconds);
      if (doc.event_to_ingestion_seconds < 0) clockSkewDetected = true;
    }
    if (typeof doc.ingestion_to_kafka_seconds === 'number') {
      ingestionToKafkaArr.push(doc.ingestion_to_kafka_seconds);
      if (doc.ingestion_to_kafka_seconds < 0) clockSkewDetected = true;
    }
    if (typeof doc.event_to_kafka_seconds === 'number') {
      eventToKafkaArr.push(doc.event_to_kafka_seconds);
      if (doc.event_to_kafka_seconds < 0) clockSkewDetected = true;
    }
  }

  const latestWeatherDoc = weatherDocs.length > 0
    ? weatherDocs.reduce((a, b) => (new Date(a.timestamp) > new Date(b.timestamp) ? a : b))
    : null;
  const latestAirDoc = airDocs.length > 0
    ? airDocs.reduce((a, b) => (new Date(a.timestamp) > new Date(b.timestamp) ? a : b))
    : null;

  const weatherLatestTime = latestWeatherDoc ? new Date(latestWeatherDoc.timestamp).getTime() : null;
  const airLatestTime = latestAirDoc ? new Date(latestAirDoc.timestamp).getTime() : null;

  const weatherFreshnessMinutes = weatherLatestTime ? Math.round((now - weatherLatestTime) / 60000) : null;
  const airFreshnessMinutes = airLatestTime ? Math.round((now - airLatestTime) / 60000) : null;

  function deriveStreamStatus(freshness: number | null, count: number): StreamFlowStatus['status'] {
    if (count === 0 || freshness === null) return 'inactive';
    if (freshness <= 180) return 'fresh';
    if (freshness <= 360) return 'stale';
    return 'degraded';
  }

  const weatherFlowStatus: StreamFlowStatus = {
    count: weatherDocs.length,
    latestTimestamp: latestWeatherDoc ? new Date(latestWeatherDoc.timestamp).toISOString() : null,
    freshnessMinutes: weatherFreshnessMinutes,
    status: deriveStreamStatus(weatherFreshnessMinutes, weatherDocs.length),
  };

  const airFlowStatus: StreamFlowStatus = {
    count: airDocs.length,
    latestTimestamp: latestAirDoc ? new Date(latestAirDoc.timestamp).toISOString() : null,
    freshnessMinutes: airFreshnessMinutes,
    status: deriveStreamStatus(airFreshnessMinutes, airDocs.length),
  };

  const isFlowing = weatherDocs.length > 0 || airDocs.length > 0;
  let overallFlowStatus: DataQualityReport['flow']['status'] = 'inactive';
  if (weatherFlowStatus.status === 'fresh' || airFlowStatus.status === 'fresh') {
    overallFlowStatus = 'optimal';
  } else if (weatherFlowStatus.status === 'stale' || airFlowStatus.status === 'stale') {
    overallFlowStatus = 'lagging';
  } else if (weatherFlowStatus.status === 'degraded' || airFlowStatus.status === 'degraded') {
    overallFlowStatus = 'stale';
  }

  // 2. Completeness & Gap Detection
  const gaps: InternalGap[] = [];

  function detectStreamGaps(
    streamName: 'weather' | 'air_quality',
    streamDocs: Document[],
  ): { hoursPresent: number; pct: number } {
    if (streamDocs.length === 0) {
      if (expectedHours >= 2) {
        gaps.push({
          stream: streamName,
          gapStart: windowStart.toISOString(),
          gapEnd: windowEnd.toISOString(),
          missingHours: expectedHours,
          type: 'internal_gap',
          description: `No ${streamName} records found in entire ${expectedHours}-hour window.`,
        });
      }
      return { hoursPresent: 0, pct: 0 };
    }

    // Sort distinct timestamps
    const timestamps = Array.from(
      new Set(streamDocs.map((d) => new Date(d.timestamp).getTime())),
    ).sort((a, b) => a - b);

    // Hourly buckets (count distinct hour slots)
    const distinctHours = new Set(
      timestamps.map((t) => Math.floor(t / (3600 * 1000))),
    );
    const hoursPresent = distinctHours.size;
    const pct = Math.min(100, Math.round((hoursPresent / expectedHours) * 1000) / 10);

    // Leading gap (between windowStart and first observation)
    const firstTs = timestamps[0];
    if (firstTs - windowStart.getTime() > 2 * 3600 * 1000) {
      const missingLeading = Math.round((firstTs - windowStart.getTime()) / (3600 * 1000));
      gaps.push({
        stream: streamName,
        gapStart: windowStart.toISOString(),
        gapEnd: new Date(firstTs).toISOString(),
        missingHours: missingLeading,
        type: 'leading_gap',
        description: `Delay at window start: ${missingLeading} missing hour(s) before first record.`,
      });
    }

    // Internal gaps between consecutive observations
    // Threshold: > 105 minutes (1.75 hours)
    for (let i = 0; i < timestamps.length - 1; i++) {
      const cur = timestamps[i];
      const next = timestamps[i + 1];
      const gapMs = next - cur;
      if (gapMs > 105 * 60 * 1000) {
        const missingCount = Math.max(1, Math.round(gapMs / (3600 * 1000)) - 1);
        gaps.push({
          stream: streamName,
          gapStart: new Date(cur).toISOString(),
          gapEnd: new Date(next).toISOString(),
          missingHours: missingCount,
          type: 'internal_gap',
          description: `Internal gap: ${missingCount} missing hour(s) between consecutive records.`,
        });
      }
    }

    // Trailing gap (between last observation and windowEnd)
    // Only flag trailing gap for historical ranges or when live trailing lag > 3h
    const lastTs = timestamps[timestamps.length - 1];
    const trailingGapMs = (mode === 'live' ? Math.min(windowEnd.getTime(), now) : windowEnd.getTime()) - lastTs;
    if (trailingGapMs > 3 * 3600 * 1000) {
      const missingTrailing = Math.round(trailingGapMs / (3600 * 1000));
      gaps.push({
        stream: streamName,
        gapStart: new Date(lastTs).toISOString(),
        gapEnd: (mode === 'live' ? new Date(now) : windowEnd).toISOString(),
        missingHours: missingTrailing,
        type: 'trailing_gap',
        description: `Trailing gap: ${missingTrailing} missing hour(s) since last record.`,
      });
    }

    return { hoursPresent, pct };
  }

  const weatherCoverage = detectStreamGaps('weather', weatherDocs);
  const airCoverage = detectStreamGaps('air_quality', airDocs);

  const overallCompletenessPct =
    weatherDocs.length > 0 && airDocs.length > 0
      ? Math.round(((weatherCoverage.pct + airCoverage.pct) / 2) * 10) / 10
      : weatherDocs.length > 0
        ? weatherCoverage.pct
        : airDocs.length > 0
          ? airCoverage.pct
          : 0;

  // Attribute-level completeness
  function calcAttributePct(docList: Document[], field: string): number {
    if (docList.length === 0) return 0;
    const nonNullCount = docList.filter(
      (d) => d[field] !== null && d[field] !== undefined,
    ).length;
    return Math.round((nonNullCount / docList.length) * 1000) / 10;
  }

  const attributes = {
    temperaturePct: calcAttributePct(weatherDocs, 'temp_c'),
    humidityPct: calcAttributePct(weatherDocs, 'humidity'),
    pressurePct: calcAttributePct(weatherDocs, 'pressure'),
    windSpeedPct: calcAttributePct(weatherDocs, 'wind_speed'),
    precipitationPct: calcAttributePct(weatherDocs, 'precipitation'),
    pm25Pct: calcAttributePct(airDocs, 'pm2_5'),
    aqiPct: calcAttributePct(airDocs, 'us_aqi'),
  };

  // 3. Physical Plausibility & Sanity Checks
  const violations: PlausibilityViolation[] = [];
  const recordsWithViolations = new Set<string>();

  // Sort all docs chronologically for sequence checks
  const sortedDocs = [...docs].sort(
    (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime(),
  );

  let flatlineTempCount = 1;
  let flatlinePressureCount = 1;
  let prevTemp: number | null = null;
  let prevPressure: number | null = null;
  let prevWeatherTs: number | null = null;

  for (const doc of sortedDocs) {
    const docId = String(doc._id ?? `${doc.station_id}|${doc.timestamp}`);
    const ts = new Date(doc.timestamp).getTime();
    const tsIso = new Date(doc.timestamp).toISOString();
    const source = doc.source ?? null;

    // Check Temperature
    if (typeof doc.temp_c === 'number') {
      if (doc.temp_c < -10 || doc.temp_c > 55) {
        recordsWithViolations.add(docId);
        violations.push({
          timestamp: tsIso,
          stream: 'weather',
          source,
          rule: 'TEMP_RANGE',
          field: 'temp_c',
          value: doc.temp_c,
          expected: '-10°C to 55°C',
          severity: 'error',
          message: `Temperature ${doc.temp_c}°C is outside plausible meteorological limits (-10 to 55°C).`,
        });
      }

      // Spike check
      if (prevTemp !== null && prevWeatherTs !== null) {
        const deltaHours = (ts - prevWeatherTs) / (3600 * 1000);
        if (deltaHours > 0 && deltaHours <= 2) {
          const deltaTemp = Math.abs(doc.temp_c - prevTemp);
          if (deltaTemp > 15) {
            recordsWithViolations.add(docId);
            violations.push({
              timestamp: tsIso,
              stream: 'weather',
              source,
              rule: 'TEMP_SPIKE',
              field: 'temp_c',
              value: doc.temp_c,
              expected: 'ΔTemp ≤ 15°C in ≤2h',
              severity: 'warning',
              message: `Abrupt temperature shift: ${prevTemp}°C → ${doc.temp_c}°C (Δ${deltaTemp.toFixed(1)}°C) in ${deltaHours.toFixed(1)}h.`,
            });
          }
        }
      }

      // Flatline check
      if (prevTemp !== null && Math.abs(doc.temp_c - prevTemp) < 0.001) {
        flatlineTempCount++;
        if (flatlineTempCount === 6) {
          recordsWithViolations.add(docId);
          violations.push({
            timestamp: tsIso,
            stream: 'weather',
            source,
            rule: 'SENSOR_FLATLINE',
            field: 'temp_c',
            value: doc.temp_c,
            expected: 'Natural variation over 6h',
            severity: 'warning',
            message: `Temperature frozen at ${doc.temp_c}°C across 6+ consecutive readings (potential crawler lock or stuck sensor).`,
          });
        }
      } else {
        flatlineTempCount = 1;
      }
      prevTemp = doc.temp_c;
      prevWeatherTs = ts;
    }

    // Check Humidity
    if (typeof doc.humidity === 'number') {
      if (doc.humidity < 0 || doc.humidity > 100) {
        recordsWithViolations.add(docId);
        violations.push({
          timestamp: tsIso,
          stream: 'weather',
          source,
          rule: 'HUMIDITY_RANGE',
          field: 'humidity',
          value: doc.humidity,
          expected: '0% to 100%',
          severity: 'error',
          message: `Relative humidity ${doc.humidity}% is physically impossible (must be 0-100%).`,
        });
      }
    }

    // Check Pressure
    if (typeof doc.pressure === 'number') {
      if (doc.pressure < 870 || doc.pressure > 1085) {
        recordsWithViolations.add(docId);
        violations.push({
          timestamp: tsIso,
          stream: 'weather',
          source,
          rule: 'PRESSURE_RANGE',
          field: 'pressure',
          value: doc.pressure,
          expected: '870 hPa to 1085 hPa',
          severity: 'error',
          message: `Barometric pressure ${doc.pressure} hPa is outside valid terrestrial atmospheric limits.`,
        });
      }

      if (prevPressure !== null && Math.abs(doc.pressure - prevPressure) < 0.001) {
        flatlinePressureCount++;
        if (flatlinePressureCount === 8) {
          recordsWithViolations.add(docId);
          violations.push({
            timestamp: tsIso,
            stream: 'weather',
            source,
            rule: 'SENSOR_FLATLINE',
            field: 'pressure',
            value: doc.pressure,
            expected: 'Atmospheric pressure variation over 8h',
            severity: 'warning',
            message: `Pressure unchanged at ${doc.pressure} hPa across 8+ consecutive readings.`,
          });
        }
      } else {
        flatlinePressureCount = 1;
      }
      prevPressure = doc.pressure;
    }

    // Check Wind Speed
    if (typeof doc.wind_speed === 'number') {
      if (doc.wind_speed < 0 || doc.wind_speed > 60) {
        recordsWithViolations.add(docId);
        violations.push({
          timestamp: tsIso,
          stream: 'weather',
          source,
          rule: 'WIND_SPEED_RANGE',
          field: 'wind_speed',
          value: doc.wind_speed,
          expected: '0 m/s to 60 m/s',
          severity: 'error',
          message: `Wind speed ${doc.wind_speed} m/s is anomalous (negative or super-typhoon scale >60 m/s).`,
        });
      }
    }

    // Check Precipitation
    if (typeof doc.precipitation === 'number') {
      if (doc.precipitation < 0 || doc.precipitation > 300) {
        recordsWithViolations.add(docId);
        violations.push({
          timestamp: tsIso,
          stream: 'weather',
          source,
          rule: 'PRECIPITATION_RANGE',
          field: 'precipitation',
          value: doc.precipitation,
          expected: '0 mm to 300 mm',
          severity: 'error',
          message: `Precipitation ${doc.precipitation} mm is out of range.`,
        });
      }
    }

    // Check PM2.5
    if (typeof doc.pm2_5 === 'number') {
      if (doc.pm2_5 < 0 || doc.pm2_5 > 500) {
        recordsWithViolations.add(docId);
        violations.push({
          timestamp: tsIso,
          stream: 'air_quality',
          source,
          rule: 'PM25_RANGE',
          field: 'pm2_5',
          value: doc.pm2_5,
          expected: '0 µg/m³ to 500 µg/m³',
          severity: doc.pm2_5 < 0 ? 'error' : 'warning',
          message: `PM2.5 value ${doc.pm2_5} µg/m³ indicates extreme outlier or sensor saturation (>500 µg/m³).`,
        });
      }
    }

    // Check US AQI
    if (typeof doc.us_aqi === 'number') {
      if (doc.us_aqi < 0 || doc.us_aqi > 500) {
        recordsWithViolations.add(docId);
        violations.push({
          timestamp: tsIso,
          stream: 'air_quality',
          source,
          rule: 'AQI_RANGE',
          field: 'us_aqi',
          value: doc.us_aqi,
          expected: '0 to 500',
          severity: 'error',
          message: `US AQI value ${doc.us_aqi} exceeds standard EPA index scale (0-500).`,
        });
      }
    }
  }

  const totalRecordsEvaluated = docs.length;
  const validRecordsCount = totalRecordsEvaluated - recordsWithViolations.size;
  const plausibilityScorePct =
    totalRecordsEvaluated > 0
      ? Math.round((validRecordsCount / totalRecordsEvaluated) * 1000) / 10
      : 100;

  // 4. Actionable Diagnostics & Smart Backfill Plan
  const summary: string[] = [];

  if (overallCompletenessPct >= 95) {
    summary.push(`High temporal completeness (${overallCompletenessPct}% of expected observations captured).`);
  } else if (overallCompletenessPct >= 75) {
    summary.push(`Moderate completeness (${overallCompletenessPct}%); intermittent missing hours observed.`);
  } else {
    summary.push(`Low completeness (${overallCompletenessPct}%); substantial gaps detected.`);
  }

  if (violations.length === 0) {
    summary.push('100% physical plausibility; all sensor metrics satisfy atmospheric boundary constraints.');
  } else {
    const errorCount = violations.filter((v) => v.severity === 'error').length;
    summary.push(
      `${violations.length} physical anomaly flag(s) detected (${errorCount} hard violation(s)).`,
    );
  }

  if (gaps.length > 0) {
    const totalMissing = gaps.reduce((acc, g) => acc + g.missingHours, 0);
    summary.push(
      `${gaps.length} gap interval(s) found totaling ~${totalMissing} missing observation hour(s).`,
    );
  } else {
    summary.push('No missing interval gaps detected between successful observations.');
  }

  let healthGrade: DataQualityReport['diagnostics']['healthGrade'] = 'A';
  if (overallCompletenessPct < 50 || !isFlowing) {
    healthGrade = 'F';
  } else if (overallCompletenessPct < 70 || violations.filter((v) => v.severity === 'error').length > 5) {
    healthGrade = 'D';
  } else if (overallCompletenessPct < 85 || violations.length > 5) {
    healthGrade = 'C';
  } else if (overallCompletenessPct < 95 || violations.length > 0) {
    healthGrade = 'B';
  }

  // Generate Suggested Backfills from detected gaps
  const suggestedBackfills: SuggestedBackfill[] = [];

  // Group gaps by calendar date range
  const dateGapMap = new Map<string, { missingHours: number; streams: Set<'weather' | 'air_quality'>; reasons: string[] }>();

  for (const gap of gaps) {
    const startDate = toVietnamIsoDate(new Date(gap.gapStart));
    const endDate = toVietnamIsoDate(new Date(gap.gapEnd));
    const key = `${startDate}__${endDate}`;

    const existing = dateGapMap.get(key) ?? {
      missingHours: 0,
      streams: new Set<'weather' | 'air_quality'>(),
      reasons: [],
    };
    existing.missingHours += gap.missingHours;
    existing.streams.add(gap.stream);
    existing.reasons.push(`${gap.stream} ${gap.type}: ${gap.missingHours}h`);
    dateGapMap.set(key, existing);
  }

  for (const [key, item] of dateGapMap.entries()) {
    const [startDate, endDate] = key.split('__');
    const streams = Array.from(item.streams);
    suggestedBackfills.push({
      startDate,
      endDate,
      missingHours: item.missingHours,
      streams,
      locationKey,
      stationId,
      reason: item.reasons.join('; '),
      suggestedCommand: `.\\scripts\\run_weather_backfill.ps1 -StartDate ${startDate} -EndDate ${endDate} -Locations ${locationKey.charAt(0).toUpperCase() + locationKey.slice(1)}`,
    });
  }

  return {
    stationId,
    locationName: locationKey,
    collection,
    window: {
      startDate: startDateStr,
      endDate: endDateStr,
      startIso: windowStart.toISOString(),
      endIso: windowEnd.toISOString(),
      expectedHours,
      mode,
    },
    flow: {
      isFlowing,
      status: overallFlowStatus,
      weather: weatherFlowStatus,
      airQuality: airFlowStatus,
      latencySeconds: {
        eventToIngestion: calculateMetric(eventToIngestionArr),
        ingestionToKafka: calculateMetric(ingestionToKafkaArr),
        eventToKafka: calculateMetric(eventToKafkaArr),
        clockSkewDetected,
      },
    },
    completeness: {
      overallCompletenessPct,
      weatherCompletenessPct: weatherCoverage.pct,
      airQualityCompletenessPct: airCoverage.pct,
      weatherHoursPresent: weatherCoverage.hoursPresent,
      airQualityHoursPresent: airCoverage.hoursPresent,
      attributes,
      gaps,
    },
    plausibility: {
      scorePct: plausibilityScorePct,
      totalRecordsEvaluated,
      validRecordsCount,
      violationCount: violations.length,
      violations,
    },
    diagnostics: {
      healthGrade,
      summary,
      suggestedBackfills,
    },
  };
}
