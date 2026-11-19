// Base URL for the Express proxy server (server/index.ts)
// Run the server with: cd server && npm install && npx ts-node index.ts
// Or set the VITE_API_URL env var to point to a deployed instance.
const API_BASE = import.meta.env.VITE_API_URL ?? 'http://localhost:3001';

async function get<T>(path: string, signal?: AbortSignal): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { signal });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`API error ${res.status}: ${body}`);
  }
  return res.json() as Promise<T>;
}

// ─── Types ────────────────────────────────────────────────────────────────────

interface HourlyWeatherResponse {
  source: string | null;
  schemaVersion: string | null;
  timestamp: string;
  time: string;
  temperature: number | null;
  humidity: number | null;
  windSpeed: number | null;
  windDir: number | null;
  pressure: number | null;
  precipitation: number | null;
  cloudCover: number | null;
  soilTemperature: number | null;
  shortwaveRadiation: number | null;
  condition: string;
}

interface AirQualityResponse {
  source: string | null;
  schemaVersion: string | null;
  timestamp: string;
  time: string;
  aqi: number | null;
  pm25: number | null;
  pm25Mean12h: number | null;
  isStagnantAir: boolean | null;
}

export interface HistoricalRecord {
  id: string;
  stationId: string | null;
  city: string | null;
  source: string | null;
  schemaVersion: string | null;
  timestamp: string;
  ingestionTimestamp: string | null;
  kafkaTimestamp: string | null;
  eventToIngestionSeconds: number | null;
  ingestionToKafkaSeconds: number | null;
  eventToKafkaSeconds: number | null;
  recordType: 'weather' | 'air_quality';
  temperature: number | null;
  humidity: number | null;
  pressure: number | null;
  windSpeed: number | null;
  precipitation: number | null;
  cloudCover: number | null;
  aqi: number | null;
  pm25: number | null;
  pm25Mean12h: number | null;
}

export interface HistoricalPage {
  records: HistoricalRecord[];
  pageSize: number;
  nextCursor: string | null;
}

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

export interface FleetLocationSummary {
  locationKey: string;
  stationId: string;
  realtime48hCount: number;
  historicalTotalCount: number;
  latestTimestamp: string | null;
  freshnessMinutes: number | null;
  status: 'fresh' | 'stale' | 'degraded' | 'no_data';
}

export interface FleetSummaryReport {
  timestamp: string;
  activeLocationsCount: number;
  totalConfiguredLocations: number;
  locations: FleetLocationSummary[];
}

// ─── Public API ───────────────────────────────────────────────────────────────

export async function fetchHourlyWeather(locationId: string, date?: string): Promise<HourlyWeatherResponse[]> {
  const q = date ? `?date=${date}` : '';
  return get<HourlyWeatherResponse[]>(`/api/weather/hourly/${locationId}${q}`);
}

export async function fetchAirQuality(locationId: string, date?: string): Promise<AirQualityResponse[]> {
  const q = date ? `?date=${date}` : '';
  return get<AirQualityResponse[]>(`/api/air-quality/${locationId}${q}`);
}

export async function fetchHistoricalRecords(
  locationId: string,
  startDate: string,
  endDate: string,
  limit = 100,
  cursor?: string,
  signal?: AbortSignal,
): Promise<HistoricalPage> {
  const params = new URLSearchParams({ startDate, endDate, limit: String(limit) });
  if (cursor) params.set('cursor', cursor);
  return get<HistoricalPage>(`/api/history/${locationId}?${params}`, signal);
}

export async function fetchForecast(locationId: string, date?: string, hour?: number) {
  const params = new URLSearchParams();
  if (date) params.set('date', date);
  if (hour !== undefined) params.set('hour', String(hour));
  const q = params.toString() ? `?${params}` : '';
  return get<import('../types/weather').ForecastData>(`/api/forecast/${locationId}${q}`);
}

export async function fetchDataQuality(
  locationId: string,
  options?: {
    startDate?: string;
    endDate?: string;
    date?: string;
    collection?: 'realtime' | 'historical';
  },
  signal?: AbortSignal,
): Promise<DataQualityReport> {
  const params = new URLSearchParams();
  if (options?.startDate && options?.endDate) {
    params.set('startDate', options.startDate);
    params.set('endDate', options.endDate);
  } else if (options?.date) {
    params.set('date', options.date);
  }
  if (options?.collection) {
    params.set('collection', options.collection);
  }
  const q = params.toString() ? `?${params}` : '';
  return get<DataQualityReport>(`/api/data-quality/${locationId}${q}`, signal);
}

export async function fetchFleetQualitySummary(signal?: AbortSignal): Promise<FleetSummaryReport> {
  return get<FleetSummaryReport>('/api/data-quality-summary', signal);
}
