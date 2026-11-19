import { useState, useEffect, useCallback } from 'react';
import {
  fetchDataQuality,
  fetchFleetQualitySummary,
  type DataQualityReport,
  type FleetSummaryReport,
  type InternalGap,
  type PlausibilityViolation,
  type SuggestedBackfill,
} from '../services/mongoService';
import {
  Activity,
  AlertOctagon,
  AlertTriangle,
  ArrowRight,
  CheckCircle2,
  Clock,
  Copy,
  Database,
  ExternalLink,
  Layers,
  RefreshCw,
  Server,
  ShieldAlert,
  ShieldCheck,
  Zap,
} from 'lucide-react';

interface DataQualityDashboardProps {
  locationId: string;
  locationName: string;
  selectedDate?: string;
  isLive?: boolean;
}

type TimeWindowPreset = 'live' | '7d' | '14d' | '30d';

function formatVietnamTime(iso: string | null): string {
  if (!iso) return '—';
  const date = new Date(iso);
  if (!Number.isFinite(date.getTime())) return '—';
  return new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Ho_Chi_Minh',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date);
}

function formatMinutesAgo(minutes: number | null): string {
  if (minutes === null) return 'No data';
  if (minutes < 0) return 'Just now (clock skew)';
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  const remMinutes = minutes % 60;
  return `${hours}h ${remMinutes}m ago`;
}

function getGradeColor(grade: string): { bg: string; text: string; border: string } {
  switch (grade) {
    case 'A':
      return { bg: 'bg-emerald-50', text: 'text-emerald-700', border: 'border-emerald-300' };
    case 'B':
      return { bg: 'bg-blue-50', text: 'text-blue-700', border: 'border-blue-300' };
    case 'C':
      return { bg: 'bg-amber-50', text: 'text-amber-700', border: 'border-amber-300' };
    case 'D':
      return { bg: 'bg-orange-50', text: 'text-orange-700', border: 'border-orange-300' };
    default:
      return { bg: 'bg-rose-50', text: 'text-rose-700', border: 'border-rose-300' };
  }
}

export function DataQualityDashboard({
  locationId,
  locationName,
  selectedDate,
  isLive,
}: DataQualityDashboardProps) {
  const [windowPreset, setWindowPreset] = useState<TimeWindowPreset>(isLive ? 'live' : '7d');
  const [report, setReport] = useState<DataQualityReport | null>(null);
  const [fleetSummary, setFleetSummary] = useState<FleetSummaryReport | null>(null);
  const [showFleet, setShowFleet] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [copiedIndex, setCopiedIndex] = useState<number | null>(null);

  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      let options: { startDate?: string; endDate?: string; date?: string; collection?: 'realtime' | 'historical' } = {};

      if (windowPreset === 'live') {
        // live mode (last 48 hours)
        options = {};
      } else {
        const now = new Date();
        const days = windowPreset === '7d' ? 7 : windowPreset === '14d' ? 14 : 30;
        const start = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);
        options = {
          startDate: start.toISOString().slice(0, 10),
          endDate: now.toISOString().slice(0, 10),
          collection: 'historical',
        };
      }

      const qualityReport = await fetchDataQuality(locationId, options);
      setReport(qualityReport);

      // Also fetch fleet summary in background if showing fleet
      if (showFleet) {
        const fleet = await fetchFleetQualitySummary();
        setFleetSummary(fleet);
      }
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  }, [locationId, windowPreset, showFleet]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const copyToClipboard = (text: string, index: number) => {
    navigator.clipboard.writeText(text);
    setCopiedIndex(index);
    setTimeout(() => setCopiedIndex(null), 2500);
  };

  const gradeStyle = report ? getGradeColor(report.diagnostics.healthGrade) : getGradeColor('F');

  return (
    <div className="space-y-6">
      {/* ── Top Bar: Title, Window Presets & Actions ── */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 bg-white p-5 rounded-xl border border-gray-200 shadow-sm">
        <div>
          <div className="flex items-center gap-3">
            <h2 className="text-xl font-bold text-gray-900">
              Data Quality & Ingestion Observability
            </h2>
            <span className="text-xs px-2.5 py-1 rounded-full font-semibold uppercase tracking-wider bg-gray-100 text-gray-700">
              {locationName}
            </span>
          </div>
          <p className="text-sm text-gray-500 mt-1">
            Real-time pipeline flow, temporal completeness, physical sanity validation, and gap detection.
          </p>
        </div>

        <div className="flex items-center gap-2 flex-wrap">
          <div className="inline-flex rounded-lg border border-gray-300 p-1 bg-gray-50 text-xs font-medium">
            <button
              onClick={() => { setWindowPreset('live'); setShowFleet(false); }}
              className={`px-3 py-1.5 rounded-md transition-colors ${
                windowPreset === 'live' && !showFleet ? 'bg-white text-blue-600 shadow-sm font-semibold' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Live (48h)
            </button>
            <button
              onClick={() => { setWindowPreset('7d'); setShowFleet(false); }}
              className={`px-3 py-1.5 rounded-md transition-colors ${
                windowPreset === '7d' && !showFleet ? 'bg-white text-blue-600 shadow-sm font-semibold' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Past 7 Days
            </button>
            <button
              onClick={() => { setWindowPreset('14d'); setShowFleet(false); }}
              className={`px-3 py-1.5 rounded-md transition-colors ${
                windowPreset === '14d' && !showFleet ? 'bg-white text-blue-600 shadow-sm font-semibold' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Past 14 Days
            </button>
            <button
              onClick={() => { setWindowPreset('30d'); setShowFleet(false); }}
              className={`px-3 py-1.5 rounded-md transition-colors ${
                windowPreset === '30d' && !showFleet ? 'bg-white text-blue-600 shadow-sm font-semibold' : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              Past 30 Days
            </button>
          </div>

          <button
            onClick={() => {
              const next = !showFleet;
              setShowFleet(next);
              if (next && !fleetSummary) {
                fetchFleetQualitySummary().then(setFleetSummary).catch(console.error);
              }
            }}
            className={`px-3 py-2 text-xs font-medium rounded-lg border transition-colors flex items-center gap-1.5 ${
              showFleet
                ? 'bg-indigo-50 border-indigo-300 text-indigo-700 font-semibold'
                : 'bg-white border-gray-300 text-gray-700 hover:bg-gray-50'
            }`}
          >
            <Database className="w-3.5 h-3.5" />
            {showFleet ? 'Close Fleet Matrix' : '63 Cities Matrix'}
          </button>

          <button
            onClick={loadData}
            disabled={loading}
            className="p-2 text-gray-600 hover:text-gray-900 rounded-lg border border-gray-300 hover:bg-gray-50 transition-colors disabled:opacity-50"
            title="Refresh Data Quality"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin text-blue-600' : ''}`} />
          </button>
        </div>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 p-4 rounded-xl flex items-start gap-3">
          <AlertOctagon className="w-5 h-5 text-red-600 shrink-0 mt-0.5" />
          <div>
            <div className="font-semibold text-sm">Failed to evaluate data quality</div>
            <div className="text-xs mt-1 text-red-600 font-mono">{error}</div>
          </div>
        </div>
      )}

      {/* ── Fleet Matrix View (when toggled) ── */}
      {showFleet && fleetSummary && (
        <div className="bg-white rounded-xl border border-gray-200 p-6 shadow-sm space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <h3 className="text-lg font-bold text-gray-900 flex items-center gap-2">
                <Database className="w-5 h-5 text-indigo-600" />
                Fleet-Wide City Ingestion Matrix
              </h3>
              <p className="text-sm text-gray-500 mt-0.5">
                Overview across all {fleetSummary.totalConfiguredLocations} configured locations. Active:{' '}
                <span className="font-semibold text-emerald-600">{fleetSummary.activeLocationsCount}</span> / {fleetSummary.totalConfiguredLocations}
              </p>
            </div>
            <span className="text-xs text-gray-400">
              Evaluated {formatVietnamTime(fleetSummary.timestamp)}
            </span>
          </div>

          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-2.5 max-h-80 overflow-y-auto pr-1">
            {fleetSummary.locations.map((loc) => {
              const isSelected = loc.locationKey === locationId;
              const statusColors = {
                fresh: 'bg-emerald-50 text-emerald-700 border-emerald-200',
                stale: 'bg-amber-50 text-amber-700 border-amber-200',
                degraded: 'bg-orange-50 text-orange-700 border-orange-200',
                no_data: 'bg-gray-50 text-gray-500 border-gray-200',
              }[loc.status];

              return (
                <div
                  key={loc.locationKey}
                  className={`p-2.5 rounded-lg border text-xs flex flex-col justify-between transition-all ${
                    isSelected ? 'ring-2 ring-blue-500 border-blue-400' : 'hover:border-gray-300'
                  } ${statusColors}`}
                >
                  <div className="font-semibold capitalize truncate">{loc.stationId}</div>
                  <div className="mt-1 flex items-center justify-between text-[11px] opacity-80">
                    <span>{loc.realtime48hCount > 0 ? `${loc.realtime48hCount} live` : `${loc.historicalTotalCount} hist`}</span>
                    <span className="font-mono">{formatMinutesAgo(loc.freshnessMinutes)}</span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {report && (
        <>
          {/* ── Top Quality Scorecard & Health Grade ── */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            {/* Grade Card */}
            <div className={`p-5 rounded-xl border ${gradeStyle.border} ${gradeStyle.bg} flex items-center gap-4`}>
              <div className="w-14 h-14 rounded-2xl flex items-center justify-center font-black text-2xl shadow-sm bg-white border border-current">
                <span className={gradeStyle.text}>{report.diagnostics.healthGrade}</span>
              </div>
              <div>
                <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Health Grade</div>
                <div className={`text-lg font-bold ${gradeStyle.text}`}>
                  {report.diagnostics.healthGrade === 'A' && 'Optimal Pipeline'}
                  {report.diagnostics.healthGrade === 'B' && 'Good Health'}
                  {report.diagnostics.healthGrade === 'C' && 'Noticeable Gaps'}
                  {report.diagnostics.healthGrade === 'D' && 'Degraded Stream'}
                  {report.diagnostics.healthGrade === 'F' && 'Inactive / Broken'}
                </div>
                <div className="text-xs text-gray-500 mt-0.5">
                  {report.window.expectedHours}h window ({report.window.startDate} → {report.window.endDate})
                </div>
              </div>
            </div>

            {/* Pipeline Flow Status */}
            <div className="bg-white p-5 rounded-xl border border-gray-200 shadow-sm flex items-center gap-4">
              <div className={`w-12 h-12 rounded-xl flex items-center justify-center ${
                report.flow.isFlowing ? 'bg-emerald-50 text-emerald-600' : 'bg-rose-50 text-rose-600'
              }`}>
                <Activity className="w-6 h-6" />
              </div>
              <div>
                <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Flow Activity</div>
                <div className="text-lg font-bold text-gray-900 flex items-center gap-2">
                  <span className={`w-2.5 h-2.5 rounded-full ${
                    report.flow.isFlowing ? 'bg-emerald-500 animate-pulse' : 'bg-rose-500'
                  }`} />
                  {report.flow.isFlowing ? 'Active Flow' : 'Stream Halted'}
                </div>
                <div className="text-xs text-gray-500 mt-0.5">
                  Weather: {formatMinutesAgo(report.flow.weather.freshnessMinutes)}
                </div>
              </div>
            </div>

            {/* Completeness Card */}
            <div className="bg-white p-5 rounded-xl border border-gray-200 shadow-sm flex items-center gap-4">
              <div className={`w-12 h-12 rounded-xl flex items-center justify-center ${
                report.completeness.overallCompletenessPct >= 90
                  ? 'bg-blue-50 text-blue-600'
                  : 'bg-amber-50 text-amber-600'
              }`}>
                <Layers className="w-6 h-6" />
              </div>
              <div>
                <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Completeness</div>
                <div className="text-lg font-bold text-gray-900">
                  {report.completeness.overallCompletenessPct}%
                </div>
                <div className="text-xs text-gray-500 mt-0.5">
                  {report.completeness.weatherHoursPresent} of {report.window.expectedHours} expected hours
                </div>
              </div>
            </div>

            {/* Physical Sanity Score */}
            <div className="bg-white p-5 rounded-xl border border-gray-200 shadow-sm flex items-center gap-4">
              <div className={`w-12 h-12 rounded-xl flex items-center justify-center ${
                report.plausibility.violationCount === 0
                  ? 'bg-purple-50 text-purple-600'
                  : 'bg-rose-50 text-rose-600'
              }`}>
                <ShieldCheck className="w-6 h-6" />
              </div>
              <div>
                <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Physical Sanity</div>
                <div className="text-lg font-bold text-gray-900">
                  {report.plausibility.scorePct}% Valid
                </div>
                <div className="text-xs text-gray-500 mt-0.5">
                  {report.plausibility.violationCount === 0
                    ? '0 sensor anomalies'
                    : `${report.plausibility.violationCount} anomalous flags`}
                </div>
              </div>
            </div>
          </div>

          {/* ── Section 1: Ingestion Flow & Pipeline Stage Latency ── */}
          <div className="bg-white rounded-xl border border-gray-200 p-6 shadow-sm space-y-5">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-base font-bold text-gray-900 flex items-center gap-2">
                  <Zap className="w-5 h-5 text-amber-500" />
                  Pipeline Flow & Stage Latencies (Where is it breaking down?)
                </h3>
                <p className="text-xs text-gray-500 mt-0.5">
                  Hop-by-hop latency metrics computed across all {report.plausibility.totalRecordsEvaluated} observations in window.
                </p>
              </div>
              <span className="text-xs px-2.5 py-1 rounded-full bg-gray-100 font-mono text-gray-700">
                Source: {report.collection}
              </span>
            </div>

            {/* Visual Stage Flow */}
            <div className="grid grid-cols-1 sm:grid-cols-4 gap-3">
              <div className="p-3.5 rounded-lg border border-gray-200 bg-gray-50 text-xs">
                <div className="text-gray-400 font-semibold uppercase">Stage 1: Sources</div>
                <div className="font-bold text-gray-900 text-sm mt-1">Open-Meteo & AQICN</div>
                <div className="text-gray-500 mt-1">Weather: {report.flow.weather.count} records</div>
                <div className="text-gray-500">Air: {report.flow.airQuality.count} records</div>
              </div>

              <div className="p-3.5 rounded-lg border border-gray-200 bg-gray-50 text-xs">
                <div className="text-gray-400 font-semibold uppercase">Stage 2: Poll Delay</div>
                <div className="font-bold text-gray-900 text-sm mt-1">
                  Event → Ingestion
                </div>
                <div className="text-gray-700 mt-1 font-mono">
                  avg: {report.flow.latencySeconds.eventToIngestion.avg !== null ? `${report.flow.latencySeconds.eventToIngestion.avg}s` : '—'}
                </div>
                <div className="text-gray-500">
                  p95: {report.flow.latencySeconds.eventToIngestion.p95 !== null ? `${report.flow.latencySeconds.eventToIngestion.p95}s` : '—'}
                </div>
              </div>

              <div className="p-3.5 rounded-lg border border-gray-200 bg-gray-50 text-xs">
                <div className="text-gray-400 font-semibold uppercase">Stage 3: Kafka Broker</div>
                <div className="font-bold text-gray-900 text-sm mt-1">
                  Ingestion → Kafka
                </div>
                <div className="text-gray-700 mt-1 font-mono">
                  avg: {report.flow.latencySeconds.ingestionToKafka.avg !== null ? `${report.flow.latencySeconds.ingestionToKafka.avg}s` : '—'}
                </div>
                <div className="text-gray-500">
                  p95: {report.flow.latencySeconds.ingestionToKafka.p95 !== null ? `${report.flow.latencySeconds.ingestionToKafka.p95}s` : '—'}
                </div>
              </div>

              <div className="p-3.5 rounded-lg border border-gray-200 bg-gray-50 text-xs">
                <div className="text-gray-400 font-semibold uppercase">Stage 4: End-to-End Lag</div>
                <div className="font-bold text-gray-900 text-sm mt-1">
                  Event → Kafka Broker
                </div>
                <div className="text-gray-700 mt-1 font-mono">
                  p95: {report.flow.latencySeconds.eventToKafka.p95 !== null ? `${report.flow.latencySeconds.eventToKafka.p95}s` : '—'}
                </div>
                <div className="text-gray-500">
                  max: {report.flow.latencySeconds.eventToKafka.max !== null ? `${report.flow.latencySeconds.eventToKafka.max}s` : '—'}
                </div>
              </div>
            </div>

            {report.flow.latencySeconds.clockSkewDetected && (
              <div className="text-xs text-amber-700 bg-amber-50 border border-amber-200 px-3 py-2 rounded-lg flex items-center gap-2">
                <AlertTriangle className="w-4 h-4 shrink-0 text-amber-600" />
                <span>
                  <strong>Clock Skew Detected:</strong> Small negative deltas exist due to microsecond offsets between the host Windows OS and the Docker container clocks. This is an environment timing artifact, not negative duration.
                </span>
              </div>
            )}
          </div>

          {/* ── Section 2: Temporal Completeness & Internal Gap Detection ── */}
          <div className="bg-white rounded-xl border border-gray-200 p-6 shadow-sm space-y-5">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-base font-bold text-gray-900 flex items-center gap-2">
                  <Clock className="w-5 h-5 text-blue-600" />
                  Time-Series Completeness & Intermittent Gaps
                </h3>
                <p className="text-xs text-gray-500 mt-0.5">
                  Pinpointing temporal missing intervals between successful observations (internal holes, leading delays, trailing gaps).
                </p>
              </div>
              <div className="text-xs font-semibold text-gray-600">
                {report.completeness.gaps.length} gap interval(s) found
              </div>
            </div>

            {/* Sensor Attributes Completeness Progress Bars */}
            <div className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-7 gap-3">
              {[
                { label: 'Temperature', pct: report.completeness.attributes.temperaturePct },
                { label: 'Humidity', pct: report.completeness.attributes.humidityPct },
                { label: 'Pressure', pct: report.completeness.attributes.pressurePct },
                { label: 'Wind Speed', pct: report.completeness.attributes.windSpeedPct },
                { label: 'Precipitation', pct: report.completeness.attributes.precipitationPct },
                { label: 'PM2.5', pct: report.completeness.attributes.pm25Pct },
                { label: 'US AQI', pct: report.completeness.attributes.aqiPct },
              ].map((attr) => (
                <div key={attr.label} className="p-3 rounded-lg border border-gray-100 bg-gray-50 text-xs">
                  <div className="text-gray-500 truncate">{attr.label}</div>
                  <div className="font-bold text-sm text-gray-900 mt-0.5">{attr.pct}%</div>
                  <div className="w-full bg-gray-200 rounded-full h-1.5 mt-2 overflow-hidden">
                    <div
                      className={`h-1.5 rounded-full ${attr.pct >= 90 ? 'bg-emerald-500' : attr.pct >= 70 ? 'bg-amber-500' : 'bg-rose-500'}`}
                      style={{ width: `${Math.min(100, Math.max(0, attr.pct))}%` }}
                    />
                  </div>
                </div>
              ))}
            </div>

            {/* Detected Gaps List */}
            {report.completeness.gaps.length === 0 ? (
              <div className="p-4 rounded-xl bg-emerald-50 border border-emerald-200 text-emerald-800 flex items-center gap-3">
                <CheckCircle2 className="w-5 h-5 text-emerald-600 shrink-0" />
                <div className="text-sm">
                  <strong>Continuous Time-Series:</strong> Zero internal or trailing observation gaps detected in this window. Data is arriving uninterrupted.
                </div>
              </div>
            ) : (
              <div className="overflow-x-auto border border-gray-200 rounded-lg">
                <table className="min-w-full divide-y divide-gray-200 text-xs">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-4 py-2.5 text-left font-semibold text-gray-600 uppercase">Stream</th>
                      <th className="px-4 py-2.5 text-left font-semibold text-gray-600 uppercase">Gap Type</th>
                      <th className="px-4 py-2.5 text-left font-semibold text-gray-600 uppercase">From (UTC+7)</th>
                      <th className="px-4 py-2.5 text-left font-semibold text-gray-600 uppercase">To (UTC+7)</th>
                      <th className="px-4 py-2.5 text-right font-semibold text-gray-600 uppercase">Missing Duration</th>
                      <th className="px-4 py-2.5 text-left font-semibold text-gray-600 uppercase">Description</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200 bg-white">
                    {report.completeness.gaps.map((gap, i) => (
                      <tr key={i} className="hover:bg-gray-50">
                        <td className="px-4 py-2.5 whitespace-nowrap">
                          <span className={`px-2 py-0.5 rounded text-[11px] font-semibold uppercase ${
                            gap.stream === 'weather' ? 'bg-blue-100 text-blue-700' : 'bg-purple-100 text-purple-700'
                          }`}>
                            {gap.stream}
                          </span>
                        </td>
                        <td className="px-4 py-2.5 whitespace-nowrap font-medium text-gray-800">
                          {gap.type === 'internal_gap' && <span className="text-amber-600 font-semibold">Intermittent Hole</span>}
                          {gap.type === 'leading_gap' && <span className="text-gray-500">Leading Offset</span>}
                          {gap.type === 'trailing_gap' && <span className="text-rose-600 font-semibold">Trailing Lag</span>}
                        </td>
                        <td className="px-4 py-2.5 whitespace-nowrap font-mono text-gray-600">
                          {formatVietnamTime(gap.gapStart)}
                        </td>
                        <td className="px-4 py-2.5 whitespace-nowrap font-mono text-gray-600">
                          {formatVietnamTime(gap.gapEnd)}
                        </td>
                        <td className="px-4 py-2.5 whitespace-nowrap text-right font-bold text-gray-900">
                          ~{gap.missingHours} hour(s)
                        </td>
                        <td className="px-4 py-2.5 text-gray-600">
                          {gap.description}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {/* ── Section 3: Physical Plausibility & Sanity Validation ── */}
          <div className="bg-white rounded-xl border border-gray-200 p-6 shadow-sm space-y-5">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-base font-bold text-gray-900 flex items-center gap-2">
                  <ShieldAlert className="w-5 h-5 text-indigo-600" />
                  Physical Plausibility & Sensor Sanity (Do values make physical sense?)
                </h3>
                <p className="text-xs text-gray-500 mt-0.5">
                  Meteorological constraints, terrestrial boundary limits, frozen sensor detection, and impossible rate-of-change checks.
                </p>
              </div>
              <span className={`text-xs px-2.5 py-1 rounded-full font-semibold ${
                report.plausibility.violationCount === 0 ? 'bg-emerald-100 text-emerald-800' : 'bg-rose-100 text-rose-800'
              }`}>
                {report.plausibility.scorePct}% Valid
              </span>
            </div>

            {/* Rules Matrix */}
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
              <div className="p-3 rounded-lg border border-gray-200 bg-gray-50">
                <div className="font-semibold text-gray-700">Temperature Bounds</div>
                <div className="font-mono text-gray-500 mt-1">-10°C ≤ Temp ≤ 55°C</div>
                <div className="text-[11px] text-gray-400 mt-0.5">Flags polar/extreme furnace values</div>
              </div>
              <div className="p-3 rounded-lg border border-gray-200 bg-gray-50">
                <div className="font-semibold text-gray-700">Relative Humidity</div>
                <div className="font-mono text-gray-500 mt-1">0% ≤ Humidity ≤ 100%</div>
                <div className="text-[11px] text-gray-400 mt-0.5">Enforces physical moisture limits</div>
              </div>
              <div className="p-3 rounded-lg border border-gray-200 bg-gray-50">
                <div className="font-semibold text-gray-700">Barometric Pressure</div>
                <div className="font-mono text-gray-500 mt-1">870 hPa ≤ P ≤ 1085 hPa</div>
                <div className="text-[11px] text-gray-400 mt-0.5">Terrestrial sea-level bounds</div>
              </div>
              <div className="p-3 rounded-lg border border-gray-200 bg-gray-50">
                <div className="font-semibold text-gray-700">PM2.5 & Air Quality</div>
                <div className="font-mono text-gray-500 mt-1">0 ≤ PM2.5 ≤ 500 µg/m³</div>
                <div className="text-[11px] text-gray-400 mt-0.5">Sensor saturation / negative check</div>
              </div>
            </div>

            {/* Violations List */}
            {report.plausibility.violations.length === 0 ? (
              <div className="p-4 rounded-xl bg-emerald-50 border border-emerald-200 text-emerald-800 flex items-center gap-3">
                <CheckCircle2 className="w-5 h-5 text-emerald-600 shrink-0" />
                <div className="text-sm">
                  <strong>All Observations Valid:</strong> 100% of recorded atmospheric and air-quality values comply with physical sanity constraints.
                </div>
              </div>
            ) : (
              <div className="space-y-2">
                <div className="text-xs font-semibold text-gray-700">Flagged Anomalous Records:</div>
                <div className="overflow-x-auto border border-gray-200 rounded-lg">
                  <table className="min-w-full divide-y divide-gray-200 text-xs">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-2.5 text-left font-semibold text-gray-600">Timestamp</th>
                        <th className="px-4 py-2.5 text-left font-semibold text-gray-600">Rule</th>
                        <th className="px-4 py-2.5 text-left font-semibold text-gray-600">Field</th>
                        <th className="px-4 py-2.5 text-left font-semibold text-gray-600">Recorded Value</th>
                        <th className="px-4 py-2.5 text-left font-semibold text-gray-600">Expected</th>
                        <th className="px-4 py-2.5 text-left font-semibold text-gray-600">Severity</th>
                        <th className="px-4 py-2.5 text-left font-semibold text-gray-600">Diagnostic Reason</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-200 bg-white">
                      {report.plausibility.violations.map((v, i) => (
                        <tr key={i} className="hover:bg-gray-50">
                          <td className="px-4 py-2.5 whitespace-nowrap font-mono text-gray-600">
                            {formatVietnamTime(v.timestamp)}
                          </td>
                          <td className="px-4 py-2.5 whitespace-nowrap font-semibold text-gray-900">
                            {v.rule}
                          </td>
                          <td className="px-4 py-2.5 whitespace-nowrap font-mono text-gray-700">
                            {v.field}
                          </td>
                          <td className="px-4 py-2.5 whitespace-nowrap font-mono font-bold text-rose-600">
                            {String(v.value)}
                          </td>
                          <td className="px-4 py-2.5 whitespace-nowrap font-mono text-gray-500">
                            {v.expected}
                          </td>
                          <td className="px-4 py-2.5 whitespace-nowrap">
                            <span className={`px-2 py-0.5 rounded text-[11px] font-semibold uppercase ${
                              v.severity === 'error' ? 'bg-rose-100 text-rose-700' : 'bg-amber-100 text-amber-700'
                            }`}>
                              {v.severity}
                            </span>
                          </td>
                          <td className="px-4 py-2.5 text-gray-600">
                            {v.message}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>

          {/* ── Section 4: Smart Backfill Action Center ── */}
          <div className="bg-gradient-to-br from-blue-50/70 to-indigo-50/50 rounded-xl border border-blue-200 p-6 shadow-sm space-y-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2.5">
                <div className="w-9 h-9 rounded-lg bg-blue-600 text-white flex items-center justify-center shadow-sm">
                  <Database className="w-5 h-5" />
                </div>
                <div>
                  <h3 className="text-base font-bold text-gray-900">
                    Smart Backfill Action Center
                  </h3>
                  <p className="text-xs text-gray-600 mt-0.5">
                    Data-driven recovery recommendations generated directly from detected temporal gaps and missing periods.
                  </p>
                </div>
              </div>
              <span className="text-xs font-semibold px-2.5 py-1 rounded-full bg-blue-100 text-blue-800">
                {report.diagnostics.suggestedBackfills.length} Recommended Backfill(s)
              </span>
            </div>

            {report.diagnostics.suggestedBackfills.length === 0 ? (
              <div className="bg-white p-4 rounded-lg border border-blue-100 text-sm text-gray-700 flex items-center gap-3">
                <CheckCircle2 className="w-5 h-5 text-emerald-500 shrink-0" />
                <span>
                  <strong>No Backfill Required:</strong> All historical windows in this range are complete with no detected missing observation gaps.
                </span>
              </div>
            ) : (
              <div className="space-y-3">
                {report.diagnostics.suggestedBackfills.map((backfill, index) => (
                  <div
                    key={index}
                    className="bg-white p-4 rounded-xl border border-blue-200 shadow-sm flex flex-col sm:flex-row sm:items-center justify-between gap-4"
                  >
                    <div className="space-y-1">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="font-bold text-sm text-gray-900">
                          {backfill.startDate} → {backfill.endDate}
                        </span>
                        <span className="text-xs px-2 py-0.5 rounded bg-amber-100 text-amber-800 font-medium">
                          ~{backfill.missingHours} missing hour(s)
                        </span>
                        <span className="text-xs px-2 py-0.5 rounded bg-gray-100 text-gray-700 capitalize">
                          {backfill.locationKey}
                        </span>
                        {backfill.streams.map((s) => (
                          <span key={s} className="text-[11px] px-1.5 py-0.5 rounded bg-blue-50 text-blue-700 font-mono">
                            {s}
                          </span>
                        ))}
                      </div>
                      <p className="text-xs text-gray-500">
                        Reason: {backfill.reason}
                      </p>
                      <div className="mt-2 flex items-center gap-2">
                        <code className="text-xs bg-gray-100 text-gray-800 px-2.5 py-1.5 rounded font-mono select-all overflow-x-auto max-w-xl">
                          {backfill.suggestedCommand}
                        </code>
                      </div>
                    </div>

                    <div className="flex items-center gap-2 shrink-0">
                      <button
                        onClick={() => copyToClipboard(backfill.suggestedCommand, index)}
                        className="px-3.5 py-2 text-xs font-semibold rounded-lg bg-blue-600 text-white hover:bg-blue-700 transition-colors flex items-center gap-1.5 shadow-sm"
                      >
                        {copiedIndex === index ? (
                          <>
                            <CheckCircle2 className="w-3.5 h-3.5" />
                            Copied!
                          </>
                        ) : (
                          <>
                            <Copy className="w-3.5 h-3.5" />
                            Copy Command
                          </>
                        )}
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}
