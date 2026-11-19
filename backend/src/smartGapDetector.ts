import { MongoClient, type Document } from 'mongodb';
import path from 'path';
import dotenv from 'dotenv';

dotenv.config({ path: process.env.ENV_FILE ?? path.resolve(__dirname, '..', '.env') });

const MONGODB_URI = process.env.MONGODB_URI ?? 'mongodb://localhost:27017';
const DATABASE = process.env.DB_NAME ?? 'climate_db';
const COLLECTION_HISTORICAL = process.env.COLLECTION ?? 'climate_observations';
const COLLECTION_REALTIME = process.env.COLLECTION_REALTIME ?? 'realtime_observations';

const LOCATION_MAP: Record<string, string> = {
  angiang: 'an giang',
  brvt: 'ba ria - vung tau',
  bacgiang: 'bac giang',
  backan: 'bac kan',
  baclieu: 'bac lieu',
  bacninh: 'bac ninh',
  bentre: 'ben tre',
  binhdinh: 'binh dinh',
  binhduong: 'binh duong',
  binhphuoc: 'binh phuoc',
  binhthuan: 'binh thuan',
  camau: 'ca mau',
  cantho: 'can tho',
  caobang: 'cao bang',
  danang: 'da nang',
  daklak: 'dak lak',
  daknong: 'dak nong',
  dienbien: 'dien bien',
  dongnai: 'dong nai',
  dongthap: 'dong thap',
  gialai: 'gia lai',
  hagiang: 'ha giang',
  hanam: 'ha nam',
  hatinh: 'ha tinh',
  haiduong: 'hai duong',
  haiphong: 'hai phong',
  hanoi: 'hanoi',
  haugiang: 'hau giang',
  hcmc: 'hcmc',
  hoabinh: 'hoa binh',
  hue: 'hue',
  hungyen: 'hung yen',
  khanhhoa: 'khanh hoa',
  kiengiang: 'kien giang',
  kontum: 'kon tum',
  laichau: 'lai chau',
  lamdong: 'lam dong',
  langson: 'lang son',
  laocai: 'lao cai',
  longan: 'long an',
  namdinh: 'nam dinh',
  nghean: 'nghe an',
  ninhbinh: 'ninh binh',
  ninhthuan: 'ninh thuan',
  phutho: 'phu tho',
  phuyen: 'phu yen',
  quangbinh: 'quang binh',
  quangnam: 'quang nam',
  quangngai: 'quang ngai',
  quangninh: 'quang ninh',
  quangtri: 'quang tri',
  soctrang: 'soc trang',
  sonla: 'son la',
  tayninh: 'tay ninh',
  thaibinh: 'thai binh',
  thainguyen: 'thai nguyen',
  thanhhoa: 'thanh hoa',
  tiengiang: 'tien giang',
  travinh: 'tra vinh',
  tuyenquang: 'tuyen quang',
  vinhlong: 'vinh long',
  vinhphuc: 'vinh phuc',
  yenbai: 'yen bai',
};

export interface DetectedGap {
  stream: 'weather' | 'air_quality';
  gapStart: string;
  gapEnd: string;
  missingHours: number;
  type: 'internal_gap' | 'leading_gap' | 'trailing_gap';
  description: string;
}

export interface SmartBackfillJob {
  locationKey: string;
  stationId: string;
  allLocations: boolean;
  startDate: string;
  endDate: string;
  missingHours: number;
  streams: Array<'weather' | 'air_quality'>;
  reasons: string[];
  command: string;
}

export interface GapDetectionResult {
  lookbackDays: number;
  scanStartIso: string;
  scanEndIso: string;
  locationsScanned: string[];
  cleanDaysCount: number;
  detectedGapsCount: number;
  jobs: SmartBackfillJob[];
  details: Record<string, { totalObservations: number; gaps: DetectedGap[] }>;
}

function toVietnamIsoDate(date: Date): string {
  const utc7 = new Date(date.getTime() + 7 * 60 * 60 * 1000);
  return utc7.toISOString().slice(0, 10);
}

function mergeDateRanges(
  ranges: Array<{
    startDate: string;
    endDate: string;
    missingHours: number;
    streams: Set<'weather' | 'air_quality'>;
    reasons: string[];
  }>,
) {
  if (ranges.length === 0) return [];
  ranges.sort((a, b) => a.startDate.localeCompare(b.startDate));
  const merged: Array<{
    startDate: string;
    endDate: string;
    missingHours: number;
    streams: Set<'weather' | 'air_quality'>;
    reasons: string[];
  }> = [];

  let cur = {
    ...ranges[0],
    streams: new Set(ranges[0].streams),
    reasons: [...ranges[0].reasons],
  };

  for (let i = 1; i < ranges.length; i++) {
    const next = ranges[i];
    const curEnd = new Date(cur.endDate + 'T00:00:00Z');
    const nextStart = new Date(next.startDate + 'T00:00:00Z');
    const dayDiff = (nextStart.getTime() - curEnd.getTime()) / (24 * 3600 * 1000);

    // Merge if overlapping or adjacent (within 1 day)
    if (dayDiff <= 1) {
      if (next.endDate > cur.endDate) {
        cur.endDate = next.endDate;
      }
      cur.missingHours += next.missingHours;
      for (const s of next.streams) cur.streams.add(s);
      for (const r of next.reasons) cur.reasons.push(r);
    } else {
      merged.push(cur);
      cur = {
        ...next,
        streams: new Set(next.streams),
        reasons: [...next.reasons],
      };
    }
  }
  merged.push(cur);
  return merged;
}

export async function detectGaps(params: {
  locations?: string[];
  allLocations?: boolean;
  lookbackDays?: number;
  mongoUri?: string;
}): Promise<GapDetectionResult> {
  const mongoUri = params.mongoUri ?? MONGODB_URI;
  const lookback = params.lookbackDays ?? 7;

  // Open-Meteo Archive API serves completed UTC calendar days up to yesterday
  const now = new Date();
  const utcYesterday = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1, 23, 59, 59, 999),
  );
  const scanEnd = utcYesterday;
  const scanStart = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - lookback, 0, 0, 0, 0),
  );

  let targetLocationKeys: string[];
  if (params.allLocations) {
    targetLocationKeys = Object.keys(LOCATION_MAP);
  } else if (params.locations && params.locations.length > 0) {
    targetLocationKeys = params.locations.map((l) => l.toLowerCase().replace(/[\s\-_]/g, ''));
  } else {
    targetLocationKeys = ['hanoi'];
  }

  const client = new MongoClient(mongoUri, { serverSelectionTimeoutMS: 5000 });
  await client.connect();

  try {
    const db = client.db(DATABASE);
    const details: Record<string, { totalObservations: number; gaps: DetectedGap[] }> = {};
    let totalCleanDays = 0;
    let totalDetectedGaps = 0;

    // Per-location merged date ranges
    const locationRangesMap = new Map<
      string,
      Array<{
        startDate: string;
        endDate: string;
        missingHours: number;
        streams: Set<'weather' | 'air_quality'>;
        reasons: string[];
      }>
    >();

    for (const locKey of targetLocationKeys) {
      const stationId = LOCATION_MAP[locKey];
      if (!stationId) continue;

      const historicalDocs = await db.collection(COLLECTION_HISTORICAL).find({
        station_id: stationId,
        timestamp: { $gte: scanStart, $lte: scanEnd },
      }).sort({ timestamp: 1 }).toArray();

      const realtimeDocs = await db.collection(COLLECTION_REALTIME).find({
        station_id: stationId,
        timestamp: { $gte: scanStart, $lte: scanEnd },
      }).sort({ timestamp: 1 }).toArray();

      const docMap = new Map<string, Document>();
      for (const d of historicalDocs) {
        docMap.set(String(d._id ?? `${d.station_id}|${d.timestamp}`), d);
      }
      for (const d of realtimeDocs) {
        const id = String(d._id ?? `${d.station_id}|${d.timestamp}`);
        if (!docMap.has(id)) docMap.set(id, d);
      }

      const allDocs = Array.from(docMap.values()).sort(
        (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime(),
      );

      const weatherDocs = allDocs.filter(
        (d) =>
          typeof d.temp_c === 'number' ||
          d.source === 'open-meteo' ||
          d._record_type === 'weather' ||
          d.kafka_topic === 'weather.raw.stream' ||
          d.kafka_topic === 'weather.raw.batch',
      );

      const airDocs = allDocs.filter(
        (d) =>
          typeof d.pm2_5 === 'number' ||
          typeof d.us_aqi === 'number' ||
          d.source === 'aqicn' ||
          d._record_type === 'air_quality' ||
          d.kafka_topic === 'air_quality.raw.stream' ||
          d.kafka_topic === 'air_quality.raw.batch',
      );

      const locationGaps: DetectedGap[] = [];

      function checkStream(streamName: 'weather' | 'air_quality', streamDocs: Document[]) {
        if (streamDocs.length === 0) {
          const totalHours = Math.round((scanEnd.getTime() - scanStart.getTime()) / 3600000);
          locationGaps.push({
            stream: streamName,
            gapStart: scanStart.toISOString(),
            gapEnd: scanEnd.toISOString(),
            missingHours: totalHours,
            type: 'internal_gap',
            description: `Completely missing ${streamName} records across entire ${lookback}-day lookback.`,
          });
          return;
        }

        const timestamps = Array.from(
          new Set(streamDocs.map((d) => new Date(d.timestamp).getTime())),
        ).sort((a, b) => a - b);

        // Leading gap
        const first = timestamps[0];
        if (first - scanStart.getTime() > 2 * 3600 * 1000) {
          const hours = Math.round((first - scanStart.getTime()) / 3600000);
          locationGaps.push({
            stream: streamName,
            gapStart: scanStart.toISOString(),
            gapEnd: new Date(first).toISOString(),
            missingHours: hours,
            type: 'leading_gap',
            description: `Missing initial ${hours}h before first observation.`,
          });
        }

        // Intermittent gaps between consecutive observations (> 105 min)
        for (let i = 0; i < timestamps.length - 1; i++) {
          const t1 = timestamps[i];
          const t2 = timestamps[i + 1];
          const diffMs = t2 - t1;
          if (diffMs > 105 * 60 * 1000) {
            const missing = Math.max(1, Math.round(diffMs / 3600000) - 1);
            locationGaps.push({
              stream: streamName,
              gapStart: new Date(t1).toISOString(),
              gapEnd: new Date(t2).toISOString(),
              missingHours: missing,
              type: 'internal_gap',
              description: `Internal intermittent gap: ${missing} missing consecutive hour(s) between readings.`,
            });
          }
        }

        // Trailing gap (up to scanEnd)
        const last = timestamps[timestamps.length - 1];
        if (scanEnd.getTime() - last > 3 * 3600 * 1000) {
          const missing = Math.round((scanEnd.getTime() - last) / 3600000);
          locationGaps.push({
            stream: streamName,
            gapStart: new Date(last).toISOString(),
            gapEnd: scanEnd.toISOString(),
            missingHours: missing,
            type: 'trailing_gap',
            description: `Trailing gap: ${missing} missing hour(s) up to archive end boundary.`,
          });
        }
      }

      checkStream('weather', weatherDocs);
      checkStream('air_quality', airDocs);

      totalDetectedGaps += locationGaps.length;
      details[locKey] = {
        totalObservations: allDocs.length,
        gaps: locationGaps,
      };

      if (locationGaps.length > 0) {
        // Collect raw date ranges for this location
        const rawRanges: Array<{
          startDate: string;
          endDate: string;
          missingHours: number;
          streams: Set<'weather' | 'air_quality'>;
          reasons: string[];
        }> = [];

        for (const g of locationGaps) {
          const sDate = toVietnamIsoDate(new Date(g.gapStart));
          // Cap eDate at scanEnd's date
          const rawEnd = toVietnamIsoDate(new Date(g.gapEnd));
          const maxEnd = toVietnamIsoDate(scanEnd);
          const eDate = rawEnd > maxEnd ? maxEnd : rawEnd;

          rawRanges.push({
            startDate: sDate,
            endDate: eDate,
            missingHours: g.missingHours,
            streams: new Set([g.stream]),
            reasons: [`${g.stream} ${g.type} (${g.missingHours}h)`],
          });
        }

        // Merge contiguous/overlapping date ranges for this location
        const mergedForLocation = mergeDateRanges(rawRanges);
        locationRangesMap.set(locKey, mergedForLocation);
      } else {
        totalCleanDays += lookback;
      }
    }

    const jobs: SmartBackfillJob[] = [];

    // Check if multi-city consolidation applies
    // If all locations were requested, or if 5+ cities have gaps spanning the full lookback window:
    const earliestStart = toVietnamIsoDate(scanStart);
    const latestEnd = toVietnamIsoDate(scanEnd);

    if (params.allLocations || locationRangesMap.size >= 5) {
      // Consolidate into a single AllLocations job spanning [earliestStart, latestEnd]
      let totalMissingHours = 0;
      const combinedStreams = new Set<'weather' | 'air_quality'>();
      const combinedReasons: string[] = [];

      for (const [_, ranges] of locationRangesMap.entries()) {
        for (const r of ranges) {
          totalMissingHours += r.missingHours;
          for (const s of r.streams) combinedStreams.add(s);
        }
      }

      combinedReasons.push(
        `Consolidated multi-city backfill for ${locationRangesMap.size} locations missing historical observations`,
      );

      jobs.push({
        locationKey: 'all',
        stationId: 'all',
        allLocations: true,
        startDate: earliestStart,
        endDate: latestEnd,
        missingHours: totalMissingHours,
        streams: Array.from(combinedStreams),
        reasons: combinedReasons,
        command: `.\\scripts\\run_weather_backfill.ps1 -StartDate ${earliestStart} -EndDate ${latestEnd} -AllLocations`,
      });
    } else {
      // Per-location consolidated jobs
      for (const [locKey, ranges] of locationRangesMap.entries()) {
        const stationId = LOCATION_MAP[locKey];
        const capitalized = locKey.charAt(0).toUpperCase() + locKey.slice(1);
        for (const span of ranges) {
          jobs.push({
            locationKey: locKey,
            stationId,
            allLocations: false,
            startDate: span.startDate,
            endDate: span.endDate,
            missingHours: span.missingHours,
            streams: Array.from(span.streams),
            reasons: span.reasons,
            command: `.\\scripts\\run_weather_backfill.ps1 -StartDate ${span.startDate} -EndDate ${span.endDate} -Locations ${capitalized}`,
          });
        }
      }
    }

    return {
      lookbackDays: lookback,
      scanStartIso: scanStart.toISOString(),
      scanEndIso: scanEnd.toISOString(),
      locationsScanned: targetLocationKeys,
      cleanDaysCount: totalCleanDays,
      detectedGapsCount: totalDetectedGaps,
      jobs,
      details,
    };
  } finally {
    await client.close();
  }
}

// CLI runner if invoked directly
if (require.main === module) {
  const args = process.argv.slice(2);
  let lookback = 7;
  let allLocs = false;
  const locations: string[] = [];

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--lookback' && args[i + 1]) {
      lookback = Number(args[i + 1]) || 7;
      i++;
    } else if (args[i] === '--all-locations') {
      allLocs = true;
    } else if (args[i] === '--locations' && args[i + 1]) {
      locations.push(...args[i + 1].split(','));
      i++;
    }
  }

  detectGaps({ lookbackDays: lookback, allLocations: allLocs, locations })
    .then((result) => {
      console.log(JSON.stringify(result, null, 2));
      process.exit(0);
    })
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
}
