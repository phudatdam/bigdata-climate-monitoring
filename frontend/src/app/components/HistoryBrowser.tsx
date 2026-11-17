import { useCallback, useEffect, useState, type FormEvent } from 'react';
import {
  fetchHistoricalRecords,
  type HistoricalPage,
  type HistoricalRecord,
} from '../services/mongoService';
import { getVietnamDateAndHour } from '../utils/observationTime';

const PAGE_SIZE = 100;
const MAX_RANGE_DAYS = 31;

function shiftDate(date: string, dayOffset: number): string {
  const [year, month, day] = date.split('-').map(Number);
  const shifted = new Date(Date.UTC(year, month - 1, day + dayOffset));
  return shifted.toISOString().slice(0, 10);
}

function displayValue(value: number | null): string {
  return value === null ? '—' : String(value);
}

function formatTimestamp(timestamp: string): string {
  const date = new Date(timestamp);
  return Number.isFinite(date.getTime())
    ? new Intl.DateTimeFormat('en-GB', {
        timeZone: 'Asia/Ho_Chi_Minh',
        dateStyle: 'medium',
        timeStyle: 'short',
      }).format(date)
    : 'Unknown time';
}

function csvCell(value: string | number | null): string {
  const text = value === null ? '' : String(value);
  return `"${text.replaceAll('"', '""')}"`;
}

export function historicalRecordsCsv(records: HistoricalRecord[]): string {
  const columns: Array<keyof HistoricalRecord> = [
    'timestamp',
    'ingestionTimestamp',
    'kafkaTimestamp',
    'eventToIngestionSeconds',
    'ingestionToKafkaSeconds',
    'eventToKafkaSeconds',
    'city',
    'stationId',
    'recordType',
    'source',
    'schemaVersion',
    'temperature',
    'humidity',
    'pressure',
    'windSpeed',
    'precipitation',
    'cloudCover',
    'aqi',
    'pm25',
    'pm25Mean12h',
    'id',
  ];
  return [
    columns.join(','),
    ...records.map((record) => columns.map((column) => csvCell(record[column])).join(',')),
  ].join('\r\n');
}

interface HistoryBrowserProps {
  locationId: string;
  locationName: string;
}

export function HistoryBrowser({ locationId, locationName }: HistoryBrowserProps) {
  const today = getVietnamDateAndHour().date;
  const [startDate, setStartDate] = useState(() => shiftDate(today, -6));
  const [endDate, setEndDate] = useState(today);
  const [submittedRange, setSubmittedRange] = useState({ startDate: shiftDate(today, -6), endDate: today });
  const [cursorStack, setCursorStack] = useState<Array<string | undefined>>([undefined]);
  const [page, setPage] = useState<HistoricalPage | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [rangeError, setRangeError] = useState<string | null>(null);
  const cursor = cursorStack[cursorStack.length - 1];

  useEffect(() => {
    setCursorStack([undefined]);
  }, [locationId]);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    fetchHistoricalRecords(
      locationId,
      submittedRange.startDate,
      submittedRange.endDate,
      PAGE_SIZE,
      cursor,
      controller.signal,
    )
      .then(setPage)
      .catch((cause: unknown) => {
        if (!controller.signal.aborted) {
          setError(cause instanceof Error ? cause.message : 'Failed to load historical observations.');
          setPage(null);
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [locationId, submittedRange, cursor]);

  const submitRange = useCallback((event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!startDate || !endDate || startDate > endDate) {
      setRangeError('Choose a start date on or before the end date.');
      return;
    }
    const days = (Date.parse(`${endDate}T00:00:00Z`) - Date.parse(`${startDate}T00:00:00Z`)) / 86_400_000 + 1;
    if (days > MAX_RANGE_DAYS) {
      setRangeError(`Choose a range of ${MAX_RANGE_DAYS} days or fewer.`);
      return;
    }
    setRangeError(null);
    setCursorStack([undefined]);
    setSubmittedRange({ startDate, endDate });
  }, [endDate, startDate]);

  const downloadCurrentPage = useCallback(() => {
    if (!page?.records.length) return;
    const blob = new Blob([historicalRecordsCsv(page.records)], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `climate-history-${locationId}-${submittedRange.startDate}-to-${submittedRange.endDate}-page-${cursorStack.length}.csv`;
    link.click();
    URL.revokeObjectURL(url);
  }, [cursorStack.length, locationId, page, submittedRange]);

  const goToNextPage = () => {
    if (page?.nextCursor) setCursorStack((current) => [...current, page.nextCursor!]);
  };
  const goToPreviousPage = () => {
    if (cursorStack.length > 1) setCursorStack((current) => current.slice(0, -1));
  };

  return (
    <section className="space-y-4 rounded-lg border border-gray-200 bg-white p-5 shadow-sm" aria-label="Historical observations">
      <div>
        <h2 className="text-xl font-semibold text-gray-900">Historical observations</h2>
        <p className="mt-1 text-sm text-gray-600">
          Browse timestamped weather and air-quality records for {locationName}. Times are Vietnam time; missing measurements are shown as unavailable.
        </p>
      </div>

      <form className="flex flex-wrap items-end gap-3" onSubmit={submitRange}>
        <div className="flex flex-col gap-1">
          <label className="text-sm text-gray-600" htmlFor="history-start-date">From</label>
          <input id="history-start-date" type="date" value={startDate} max={today} onChange={(event) => setStartDate(event.target.value)} className="rounded-md border border-gray-300 px-3 py-2 text-sm" />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-sm text-gray-600" htmlFor="history-end-date">To</label>
          <input id="history-end-date" type="date" value={endDate} max={today} onChange={(event) => setEndDate(event.target.value)} className="rounded-md border border-gray-300 px-3 py-2 text-sm" />
        </div>
        <button type="submit" className="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700">Search</button>
        <button type="button" disabled={!page?.records.length || loading} onClick={downloadCurrentPage} className="rounded-md border border-gray-300 px-4 py-2 text-sm text-gray-700 disabled:cursor-not-allowed disabled:opacity-50">Download page CSV</button>
        <span className="text-xs text-gray-500">Maximum range: {MAX_RANGE_DAYS} calendar days.</span>
      </form>

      {rangeError && <p role="alert" className="text-sm text-red-700">{rangeError}</p>}
      {error && <p role="alert" className="rounded-md bg-red-50 p-3 text-sm text-red-700">{error}</p>}
      {loading && <p role="status" className="text-sm text-gray-600">Loading observations…</p>}
      {!loading && !error && page && (
        <>
          <p role="status" className="text-sm text-gray-600">
            {page.records.length === 0
              ? `No observations from ${submittedRange.startDate} through ${submittedRange.endDate}.`
              : `Page ${cursorStack.length} · ${page.records.length} records · ${submittedRange.startDate} through ${submittedRange.endDate}`}
          </p>
          {page.records.length > 0 && (
            <div className="overflow-x-auto">
              <table className="min-w-full border-collapse text-left text-sm">
                <thead>
                  <tr className="border-b border-gray-200 text-xs uppercase text-gray-500">
                    <th className="px-3 py-2">Observed (ICT)</th><th className="px-3 py-2">Type</th><th className="px-3 py-2">Source / schema</th>
                    <th className="px-3 py-2">Event → ingestion (s)</th><th className="px-3 py-2">Ingestion → Kafka (s)</th>
                    <th className="px-3 py-2">Temperature °C</th><th className="px-3 py-2">Humidity %</th><th className="px-3 py-2">Pressure hPa</th>
                    <th className="px-3 py-2">AQI</th><th className="px-3 py-2">PM2.5</th>
                  </tr>
                </thead>
                <tbody>
                  {page.records.map((record) => (
                    <tr key={record.id} className="border-b border-gray-100 text-gray-800">
                      <td className="whitespace-nowrap px-3 py-2">{formatTimestamp(record.timestamp)}</td>
                      <td className="px-3 py-2">{record.recordType === 'weather' ? 'Weather' : 'Air quality'}</td>
                      <td className="px-3 py-2">{record.source ?? 'Unknown'} · {record.schemaVersion ?? 'unknown schema'}</td>
                      <td className="px-3 py-2">{displayValue(record.eventToIngestionSeconds)}</td>
                      <td className="px-3 py-2">{displayValue(record.ingestionToKafkaSeconds)}</td>
                      <td className="px-3 py-2">{displayValue(record.temperature)}</td>
                      <td className="px-3 py-2">{displayValue(record.humidity)}</td>
                      <td className="px-3 py-2">{displayValue(record.pressure)}</td>
                      <td className="px-3 py-2">{displayValue(record.aqi)}</td>
                      <td className="px-3 py-2">{displayValue(record.pm25)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <div className="flex justify-between">
            <button type="button" onClick={goToPreviousPage} disabled={cursorStack.length === 1 || loading} className="rounded-md border border-gray-300 px-3 py-2 text-sm disabled:cursor-not-allowed disabled:opacity-50">Previous</button>
            <button type="button" onClick={goToNextPage} disabled={!page.nextCursor || loading} className="rounded-md border border-gray-300 px-3 py-2 text-sm disabled:cursor-not-allowed disabled:opacity-50">Next</button>
          </div>
        </>
      )}
    </section>
  );
}
