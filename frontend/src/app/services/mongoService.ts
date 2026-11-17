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
