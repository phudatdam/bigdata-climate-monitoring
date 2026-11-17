import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { HistoryBrowser, historicalRecordsCsv } from './HistoryBrowser';
import { fetchHistoricalRecords, type HistoricalRecord } from '../services/mongoService';

vi.mock('../services/mongoService', () => ({
  fetchHistoricalRecords: vi.fn(),
}));

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

const weatherRecord: HistoricalRecord = {
  id: 'weather-1',
  stationId: 'hanoi',
  city: 'Hanoi',
  source: 'open-meteo',
  schemaVersion: 'v1',
  timestamp: '2026-11-09T10:00:00.000Z',
  ingestionTimestamp: '2026-11-09T10:05:00.000Z',
  kafkaTimestamp: '2026-11-09T10:05:02.000Z',
  eventToIngestionSeconds: 300,
  ingestionToKafkaSeconds: 2,
  eventToKafkaSeconds: 302,
  recordType: 'weather',
  temperature: 24.5,
  humidity: null,
  pressure: 1012,
  windSpeed: null,
  precipitation: null,
  cloudCover: null,
  aqi: null,
  pm25: null,
  pm25Mean12h: null,
};

describe('HistoryBrowser', () => {
  it('displays source timestamps and unavailable fields, and requests the next cursor page', async () => {
    vi.mocked(fetchHistoricalRecords)
      .mockResolvedValueOnce({ records: [weatherRecord], pageSize: 1, nextCursor: 'next-page' })
      .mockResolvedValueOnce({
        records: [{ ...weatherRecord, id: 'aqi-1', recordType: 'air_quality', temperature: null, pm25: 18 }],
        pageSize: 1,
        nextCursor: null,
      });

    render(<HistoryBrowser locationId="hanoi" locationName="Ha Noi" />);

    expect(await screen.findByText(/open-meteo · v1/)).toBeTruthy();
    expect(screen.getAllByText('—').length).toBeGreaterThan(0);
    expect(screen.getByText(/Nov 2026, 17:00/)).toBeTruthy();
    fireEvent.click(screen.getByRole('button', { name: 'Next' }));
    expect(await screen.findByText(/Air quality/)).toBeTruthy();
    expect(fetchHistoricalRecords).toHaveBeenLastCalledWith(
      'hanoi',
      expect.any(String),
      expect.any(String),
      100,
      'next-page',
      expect.any(AbortSignal),
    );
  });

  it('rejects ranges over the API limit without issuing a new request', async () => {
    vi.mocked(fetchHistoricalRecords).mockResolvedValue({
      records: [],
      pageSize: 100,
      nextCursor: null,
    });
    render(<HistoryBrowser locationId="hanoi" locationName="Ha Noi" />);
    await screen.findByText(/No observations/);
    const callsBefore = vi.mocked(fetchHistoricalRecords).mock.calls.length;
    fireEvent.change(screen.getByLabelText('From'), { target: { value: '2026-01-01' } });
    fireEvent.change(screen.getByLabelText('To'), { target: { value: '2026-02-01' } });
    fireEvent.click(screen.getByRole('button', { name: 'Search' }));
    expect(screen.getByRole('alert').textContent).toMatch(/31 days or fewer/);
    await waitFor(() => expect(fetchHistoricalRecords).toHaveBeenCalledTimes(callsBefore));
  });

  it('escapes CSV cells and includes null measurements as empty cells', () => {
    const csv = historicalRecordsCsv([{ ...weatherRecord, city: 'Hanoi, "Capital"' }]);
    expect(csv).toContain('"Hanoi, ""Capital"""');
    expect(csv.split('\r\n')[1]).toContain(',"24.5","","1012"');
  });
});
