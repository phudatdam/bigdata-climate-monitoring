import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import type { WeatherData, HourlyData, AirQualityData, CurrentWeather } from '../types/weather';
import { fetchHourlyWeather, fetchAirQuality } from '../services/mongoService';

interface UseWeatherDataResult {
  data: WeatherData | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

function hourlyToCurrentWeather(h: HourlyData): CurrentWeather {
  return {
    source: h.source,
    schemaVersion: h.schemaVersion,
    temperature:        h.temperature,
    humidity:           h.humidity,
    windSpeed:          h.windSpeed,
    windDir:            h.windDir,
    pressure:           h.pressure,
    precipitation:      h.precipitation,
    cloudCover:         h.cloudCover,
    soilTemperature:    h.soilTemperature,
    shortwaveRadiation: h.shortwaveRadiation,
    condition:          h.condition,
    timestamp:          h.timestamp,
  };
}

// Finds the hourly entry whose time label is closest to `hour` (0-23).
function pickHour(hourly: HourlyData[], hour: number, date?: string): HourlyData | null {
  if (!hourly.length) return null;
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Bangkok',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(new Date());
  const todayInVietnam = `${parts.find((part) => part.type === 'year')?.value}-${parts.find((part) => part.type === 'month')?.value}-${parts.find((part) => part.type === 'day')?.value}`;
  const targetDate = date ?? todayInVietnam;
  const target = new Date(`${targetDate}T${String(hour).padStart(2, '0')}:00:00+07:00`).getTime();
  return hourly.reduce((best, entry) => {
    const distance = Math.abs(new Date(entry.timestamp).getTime() - target);
    const bestDistance = Math.abs(new Date(best.timestamp).getTime() - target);
    return distance < bestDistance ? entry : best;
  });
}

// date: "YYYY-MM-DD" for a specific day, or undefined for the live last-24h window.
// hour: 0-23 — selects which hour's snapshot to show as "current conditions".
//       Changing hour does NOT re-fetch; data is derived from the cached hourly array.
export function useWeatherData(locationId: string, date?: string, hour?: number): UseWeatherDataResult {
  const [hourlyCache, setHourlyCache]         = useState<HourlyData[] | null>(null);
  const [airQualityCache, setAirQualityCache] = useState<AirQualityData[] | null>(null);
  const [loading, setLoading]                 = useState(true);
  const [error, setError]                     = useState<string | null>(null);
  const abortRef                              = useRef<AbortController | null>(null);

  // Only re-fetch when location or date changes — NOT when hour changes.
  const load = useCallback(async () => {
    // Cancel any in-flight request so stale responses can't overwrite new ones.
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    // Clear stale cache immediately so old data never bleeds into the new view.
    setHourlyCache(null);
    setAirQualityCache(null);
    setLoading(true);
    setError(null);

    try {
      const [hourly, airQuality] = await Promise.all([
        fetchHourlyWeather(locationId, date),
        fetchAirQuality(locationId, date),
      ]);
      if (controller.signal.aborted) return;
      setHourlyCache(hourly);
      setAirQualityCache(airQuality);
    } catch (err) {
      if (controller.signal.aborted) return;
      setError(err instanceof Error ? err.message : 'Failed to load weather data');
    } finally {
      if (!controller.signal.aborted) setLoading(false);
    }
  }, [locationId, date]);

  useEffect(() => { load(); }, [load]);

  // Derive the current-conditions snapshot from the cached array — zero latency.
  const data = useMemo<WeatherData | null>(() => {
    if (!hourlyCache || !airQualityCache) return null;

    const selectedHourly =
      hour !== undefined
        ? pickHour(hourlyCache, hour, date)
        : hourlyCache[hourlyCache.length - 1] ?? null;

    if (!selectedHourly) return null;

    return {
      current:    hourlyToCurrentWeather(selectedHourly),
      hourly:     hourlyCache,
      airQuality: airQualityCache,
    };
  }, [hourlyCache, airQualityCache, date, hour]);

  return { data, loading, error, refetch: load };
}
