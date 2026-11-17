import { describe, expect, it } from 'vitest';
import type { AirQualityData } from '../types/weather';
import {
  closestAirQualityObservation,
  getVietnamDateAndHour,
  getObservationFreshness,
} from './observationTime';

const airQualityAt = (timestamp: string): AirQualityData => ({
  source: 'aqicn',
  schemaVersion: 'v1',
  timestamp,
  time: '10:00',
  aqi: null,
  pm25: 20,
  pm25Mean12h: null,
  isStagnantAir: null,
});

describe('observation time semantics', () => {
  it('marks stale, future, and malformed timestamps distinctly', () => {
    const now = Date.parse('2026-11-08T12:00:00.000Z');
    expect(getObservationFreshness('2026-11-08T08:00:00.000Z', now)).toBe('stale');
    expect(getObservationFreshness('2026-11-08T13:00:00.000Z', now)).toBe('future');
    expect(getObservationFreshness('invalid', now)).toBe('unknown');
  });

  it('uses Vietnam-local date and hour rather than the browser timezone', () => {
    expect(
      getVietnamDateAndHour(new Date('2026-11-08T03:30:00.000Z')),
    ).toEqual({ date: '2026-11-08', hour: 10 });
  });

  it('matches AQI by timestamp within the configured tolerance only', () => {
    const nearest = airQualityAt('2026-11-08T11:30:00.000Z');
    const older = airQualityAt('2026-11-08T09:00:00.000Z');
    expect(
      closestAirQualityObservation(
        [older, nearest],
        '2026-11-08T12:00:00.000Z',
      ),
    ).toBe(nearest);
    expect(
      closestAirQualityObservation(
        [older],
        '2026-11-08T12:00:00.000Z',
      ),
    ).toBeNull();
  });
});
