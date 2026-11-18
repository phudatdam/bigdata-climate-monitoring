import type { AirQualityData } from '../types/weather';

export type ObservationFreshness = 'recent' | 'stale' | 'future' | 'unknown';

export function getVietnamDateAndHour(now = new Date()): { date: string; hour: number } {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Ho_Chi_Minh',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(now);
  const part = (type: Intl.DateTimeFormatPartTypes) =>
    parts.find((item) => item.type === type)?.value ?? '00';
  return {
    date: `${part('year')}-${part('month')}-${part('day')}`,
    hour: Number(part('hour')),
  };
}

export function getObservationFreshness(
  timestamp: string,
  now = Date.now(),
  staleAfterMinutes = 180,
): ObservationFreshness {
  const observedAt = new Date(timestamp).getTime();
  if (!Number.isFinite(observedAt)) return 'unknown';

  const ageMinutes = (now - observedAt) / 60_000;
  if (ageMinutes < -5) return 'future';
  return ageMinutes > staleAfterMinutes ? 'stale' : 'recent';
}

export function formatVietnamTimestamp(timestamp: string | number | Date): string {
  const date = timestamp instanceof Date ? timestamp : new Date(timestamp);
  if (!Number.isFinite(date.getTime())) return 'Unknown time';
  return new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Ho_Chi_Minh',
    day: '2-digit',
    month: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
  }).format(date);
}

export function closestAirQualityObservation(
  observations: AirQualityData[],
  targetTimestamp: string,
  toleranceMinutes = 90,
): AirQualityData | null {
  const target = new Date(targetTimestamp).getTime();
  if (!Number.isFinite(target)) return null;

  const nearest = observations.reduce<AirQualityData | null>((best, observation) => {
    const observedAt = new Date(observation.timestamp).getTime();
    if (!Number.isFinite(observedAt)) return best;
    if (
      best === null ||
      Math.abs(observedAt - target) < Math.abs(new Date(best.timestamp).getTime() - target)
    ) {
      return observation;
    }
    return best;
  }, null);

  if (nearest === null) return null;
  const distance = Math.abs(new Date(nearest.timestamp).getTime() - target);
  return distance <= toleranceMinutes * 60_000 ? nearest : null;
}
