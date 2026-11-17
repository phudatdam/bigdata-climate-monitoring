import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, render, screen } from '@testing-library/react';
import { CurrentWeather } from './CurrentWeather';
import type { CurrentWeather as CurrentWeatherData } from '../types/weather';

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

const current: CurrentWeatherData = {
  source: 'open-meteo',
  schemaVersion: 'v1',
  temperature: null,
  humidity: null,
  windSpeed: null,
  windDir: null,
  pressure: null,
  precipitation: null,
  cloudCover: null,
  soilTemperature: null,
  shortwaveRadiation: null,
  condition: 'Unavailable',
  timestamp: '2026-11-08T01:00:00.000Z',
};

describe('CurrentWeather', () => {
  it('shows missing measurements as unavailable and labels an old source reading stale', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-11-08T05:00:00.000Z'));

    render(<CurrentWeather current={current} locationName="Ha Noi" />);

    expect(screen.getByRole('heading', { level: 2 }).textContent).toBe('—');
    expect(screen.getByText('Humidity')).toBeTruthy();
    expect(screen.getByText(/Stale · open-meteo/)).toBeTruthy();
    expect(screen.getByText(/stale reading/)).toBeTruthy();
  });

  it('does not present an invalid timestamp as fresh', () => {
    render(
      <CurrentWeather
        current={{ ...current, timestamp: 'not-a-timestamp' }}
        locationName="Ha Noi"
      />,
    );

    expect(screen.getByText(/Stale · open-meteo/)).toBeTruthy();
    expect(screen.getByText(/Unknown time/)).toBeTruthy();
    expect(screen.getByText(/freshness unknown/)).toBeTruthy();
  });
});
