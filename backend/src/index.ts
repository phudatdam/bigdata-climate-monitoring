import dotenv from 'dotenv';
import express, { Request, Response } from 'express';
import cors from 'cors';
import { MongoClient, Document } from 'mongodb';
import path from 'path';

dotenv.config({ path: process.env.ENV_FILE ?? path.resolve(process.cwd(), '.env') });

// ─── Configuration ────────────────────────────────────────────────────────────
const MONGODB_URI = process.env.MONGODB_URI ?? 'mongodb://localhost:27017';

const DATABASE               = process.env.DB_NAME               ?? "climate_db";
const COLLECTION_HISTORICAL  = process.env.COLLECTION            ?? "climate_observations";
const COLLECTION_REALTIME    = process.env.COLLECTION_REALTIME   ?? "realtime_observations";
const COLLECTION_ALERTS      = process.env.COLLECTION_ALERTS     ?? "realtime_alerts";
const PORT                   = process.env.PORT                   ?? 3001;
const HISTORY_MAX_DAYS = 31;
const HISTORY_DEFAULT_PAGE_SIZE = 100;
const HISTORY_MAX_PAGE_SIZE = 200;

// Pick the right collection based on whether the request is live or historical.
// Live = no date param (last-24h window). Historical = explicit YYYY-MM-DD date.
function collectionFor(dateParam?: string): string {
  return dateParam ? COLLECTION_HISTORICAL : COLLECTION_REALTIME;
}

// Map frontend location IDs → station_id values stored in MongoDB.
// Update these values to match the station_id strings in your collection.
const LOCATION_MAP: Record<string, string> = {
  angiang:      'an giang',
  brvt:         'ba ria - vung tau',
  bacgiang:     'bac giang',
  backan:       'bac kan',
  baclieu:      'bac lieu',
  bacninh:      'bac ninh',
  bentre:       'ben tre',
  binhdinh:     'binh dinh',
  binhduong:    'binh duong',
  binhphuoc:    'binh phuoc',
  binhthuan:    'binh thuan',
  camau:        'ca mau',
  cantho:       'can tho',
  caobang:      'cao bang',
  danang:       'da nang',
  daklak:       'dak lak',
  daknong:      'dak nong',
  dienbien:     'dien bien',
  dongnai:      'dong nai',
  dongthap:     'dong thap',
  gialai:       'gia lai',
  hagiang:      'ha giang',
  hanam:        'ha nam',
  hatinh:       'ha tinh',
  haiduong:     'hai duong',
  haiphong:     'hai phong',
  hanoi:        'hanoi',
  haugiang:     'hau giang',
  hcmc:         'hcmc',
  hoabinh:      'hoa binh',
  hue:          'hue',
  hungyen:      'hung yen',
  khanhhoa:     'khanh hoa',
  kiengiang:    'kien giang',
  kontum:       'kon tum',
  laichau:      'lai chau',
  lamdong:      'lam dong',
  langson:      'lang son',
  laocai:       'lao cai',
  longan:       'long an',
  namdinh:      'nam dinh',
  nghean:       'nghe an',
  ninhbinh:     'ninh binh',
  ninhthuan:    'ninh thuan',
  phutho:       'phu tho',
  phuyen:       'phu yen',
  quangbinh:    'quang binh',
  quangnam:     'quang nam',
  quangngai:    'quang ngai',
  quangninh:    'quang ninh',
  quangtri:     'quang tri',
  soctrang:     'soc trang',
  sonla:        'son la',
  tayninh:      'tay ninh',
  thaibinh:     'thai binh',
  thainguyen:   'thai nguyen',
  thanhhoa:     'thanh hoa',
  tiengiang:    'tien giang',
  travinh:      'tra vinh',
  tuyenquang:   'tuyen quang',
  vinhlong:     'vinh long',
  vinhphuc:     'vinh phuc',
  yenbai:       'yen bai',
};
// ─────────────────────────────────────────────────────────────────────────────

const client = new MongoClient(MONGODB_URI);
let db: ReturnType<typeof client.db>;

async function connectDB() {
  await client.connect();
  db = client.db(DATABASE);
  await Promise.all(
    [COLLECTION_HISTORICAL, COLLECTION_REALTIME, COLLECTION_ALERTS].map((collection) =>
      db.collection(collection).createIndex({ station_id: 1, timestamp: -1 }),
    ),
  );
  await db
    .collection(COLLECTION_REALTIME)
    .createIndex({ timestamp: 1 }, { expireAfterSeconds: 172800 });
  console.log(`Connected to MongoDB — database: "${DATABASE}"`);
}

const app = express();
app.use(cors());
app.use(express.json());

app.get("/api/health", (_req: Request, res: Response) =>
  res.json({ status: "ok" }),
);

// Resolve the 24-hour window for a given date string ("YYYY-MM-DD") or live mode.
// Returns { start, end } as UTC Date objects covering the full calendar day in UTC+7.
function dateWindow(dateParam?: string): { start: Date; end: Date } {
  if (dateParam) {
    // Treat the date as a calendar day in UTC+7 (Vietnam Standard Time)
    const start = new Date(`${dateParam}T00:00:00+07:00`);
    const end = new Date(start.getTime() + 24 * 60 * 60 * 1000);
    return { start, end };
  }
  // Live: last 24 hours
  return { start: new Date(Date.now() - 24 * 60 * 60 * 1000), end: new Date() };
}

function isValidDateParam(value: unknown): value is string {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}

function formatVietnamHour(value: Date | string): string {
  const hour = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Bangkok',
    hour: '2-digit',
    hourCycle: 'h23',
  }).format(value instanceof Date ? value : new Date(value));
  return `${hour}:00`;
}

function nullableNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function nullableString(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function nullableTimestamp(value: unknown): string | null {
  if (!(value instanceof Date) || !Number.isFinite(value.getTime())) return null;
  return value.toISOString();
}

function queryString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined;
}

function historyCursor(value: string): { timestamp: Date; id: string } | null {
  try {
    const parsed: unknown = JSON.parse(Buffer.from(value, 'base64url').toString('utf8'));
    if (
      typeof parsed !== 'object' ||
      parsed === null ||
      !('timestamp' in parsed) ||
      !('id' in parsed) ||
      typeof parsed.timestamp !== 'string' ||
      typeof parsed.id !== 'string'
    ) {
      return null;
    }
    const timestamp = new Date(parsed.timestamp);
    if (!Number.isFinite(timestamp.getTime()) || timestamp.toISOString() !== parsed.timestamp) {
      return null;
    }
    return { timestamp, id: parsed.id };
  } catch {
    return null;
  }
}

function encodeHistoryCursor(doc: Document): string {
  return Buffer.from(
    JSON.stringify({
      timestamp: new Date(doc.timestamp).toISOString(),
      id: String(doc._id),
    }),
  ).toString('base64url');
}

// GET /api/weather/hourly/:locationId?date=YYYY-MM-DD
// Returns all hourly observations within the window, oldest→newest
app.get(
  "/api/weather/hourly/:locationId",
  async (req: Request, res: Response) => {
    const stationId = LOCATION_MAP[req.params.locationId as string];
    if (!stationId) { res.status(400).json({ error: "Unknown location" }); return; }

    const dateParam = req.query.date;
    if (dateParam !== undefined && !isValidDateParam(dateParam)) {
      res.status(400).json({ error: "Invalid date. Expected a real YYYY-MM-DD date." });
      return;
    }
    const { start, end } = dateWindow(dateParam);

    try {
      const docs = (await db
        .collection(collectionFor(dateParam))
        .find({
          station_id: stationId,
          timestamp: { $gte: start, $lt: end },
          temp_c: { $type: 'number' },
        })
        .sort({ timestamp: 1 })
        .toArray()) as Document[];

      res.json(
        docs.map((d) => {
          return {
            time:               formatVietnamHour(d.timestamp),
            timestamp:          new Date(d.timestamp).toISOString(),
            source:             nullableString(d.source),
            schemaVersion:      nullableString(d.schema_version),
            temperature:        nullableNumber(d.temp_c),
            humidity:           nullableNumber(d.humidity),
            windSpeed:          nullableNumber(d.wind_speed),
            windDir:            nullableNumber(d.wind_dir),
            pressure:           nullableNumber(d.pressure),
            precipitation:      nullableNumber(d.precipitation),
            cloudCover:         nullableNumber(d.cloud_cover),
            soilTemperature:    nullableNumber(d.soil_temperature),
            shortwaveRadiation: nullableNumber(d.shortwave_radiation),
            condition:          cloudCoverToCondition(nullableNumber(d.cloud_cover)),
          };
        }),
      );
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: (err as Error).message });
    }
  },
);

// GET /api/history/:locationId?startDate=YYYY-MM-DD&endDate=YYYY-MM-DD&limit=100&cursor=...
app.get("/api/history/:locationId", async (req: Request, res: Response) => {
  const stationId = LOCATION_MAP[req.params.locationId as string];
  if (!stationId) { res.status(400).json({ error: "Unknown location" }); return; }

  const startDate = queryString(req.query.startDate);
  const endDate = queryString(req.query.endDate);
  if (!startDate || !endDate || !isValidDateParam(startDate) || !isValidDateParam(endDate)) {
    res.status(400).json({ error: "startDate and endDate must be real YYYY-MM-DD dates." });
    return;
  }
  if (startDate > endDate) {
    res.status(400).json({ error: "startDate must be on or before endDate." });
    return;
  }

  const { start } = dateWindow(startDate);
  const { end } = dateWindow(endDate);
  const dayCount = (end.getTime() - start.getTime()) / (24 * 60 * 60 * 1000);
  if (dayCount > HISTORY_MAX_DAYS) {
    res.status(400).json({ error: `Date range cannot exceed ${HISTORY_MAX_DAYS} calendar days.` });
    return;
  }

  const limitParam = queryString(req.query.limit);
  const pageSize = limitParam === undefined ? HISTORY_DEFAULT_PAGE_SIZE : Number(limitParam);
  if (
    !Number.isInteger(pageSize) ||
    pageSize < 1 ||
    pageSize > HISTORY_MAX_PAGE_SIZE
  ) {
    res.status(400).json({ error: `limit must be an integer from 1 to ${HISTORY_MAX_PAGE_SIZE}.` });
    return;
  }

  const cursorParam = queryString(req.query.cursor);
  const cursor = cursorParam === undefined ? undefined : historyCursor(cursorParam);
  if (cursorParam !== undefined && !cursor) {
    res.status(400).json({ error: "Invalid history cursor." });
    return;
  }

  const timestampRange: Document = { $gte: start, $lt: end };
  const filters: Document[] = [
    { station_id: stationId },
    { timestamp: timestampRange },
    { $or: [{ temp_c: { $type: 'number' } }, { pm2_5: { $type: 'number' } }, { us_aqi: { $type: 'number' } }] },
  ];
  if (cursor) {
    filters.push({
      $or: [
        { timestamp: { $gt: cursor.timestamp } },
        { timestamp: cursor.timestamp, _id: { $gt: cursor.id } },
      ],
    });
  }

  try {
    const docs = (await db
      .collection(COLLECTION_HISTORICAL)
      .find({ $and: filters })
      .sort({ timestamp: 1, _id: 1 })
      .limit(pageSize + 1)
      .toArray()) as Document[];
    const hasMore = docs.length > pageSize;
    const page = docs.slice(0, pageSize);
    res.json({
      records: page.map((doc) => ({
        id: String(doc._id),
        stationId: nullableString(doc.station_id),
        city: nullableString(doc.city),
        source: nullableString(doc.source),
        schemaVersion: nullableString(doc.schema_version),
        timestamp: new Date(doc.timestamp).toISOString(),
        ingestionTimestamp: nullableTimestamp(doc.ingestion_timestamp),
        kafkaTimestamp: nullableTimestamp(doc.kafka_timestamp),
        eventToIngestionSeconds: nullableNumber(doc.event_to_ingestion_seconds),
        ingestionToKafkaSeconds: nullableNumber(doc.ingestion_to_kafka_seconds),
        eventToKafkaSeconds: nullableNumber(doc.event_to_kafka_seconds),
        recordType: typeof doc.temp_c === 'number' ? 'weather' : 'air_quality',
        temperature: nullableNumber(doc.temp_c),
        humidity: nullableNumber(doc.humidity),
        pressure: nullableNumber(doc.pressure),
        windSpeed: nullableNumber(doc.wind_speed),
        precipitation: nullableNumber(doc.precipitation),
        cloudCover: nullableNumber(doc.cloud_cover),
        aqi: nullableNumber(doc.us_aqi),
        pm25: nullableNumber(doc.pm2_5),
        pm25Mean12h: nullableNumber(doc.pm25_mean_12h),
      })),
      pageSize,
      nextCursor: hasMore ? encodeHistoryCursor(page[page.length - 1]) : null,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: (err as Error).message });
  }
});

// GET /api/air-quality/:locationId?date=YYYY-MM-DD
// Returns all hourly air quality readings within the window, oldest→newest
app.get(
  "/api/air-quality/:locationId",
  async (req: Request, res: Response) => {
    const stationId = LOCATION_MAP[req.params.locationId as string];
    if (!stationId) { res.status(400).json({ error: "Unknown location" }); return; }

    const dateParam = req.query.date;
    if (dateParam !== undefined && !isValidDateParam(dateParam)) {
      res.status(400).json({ error: "Invalid date. Expected a real YYYY-MM-DD date." });
      return;
    }
    const { start, end } = dateWindow(dateParam);

    try {
      const docs = (await db
        .collection(collectionFor(dateParam))
        .find({
          station_id: stationId,
          timestamp: { $gte: start, $lt: end },
          $or: [{ us_aqi: { $type: 'number' } }, { pm2_5: { $type: 'number' } }],
        })
        .sort({ timestamp: 1 })
        .toArray()) as Document[];

      res.json(
        docs.map((d) => {
          return {
            time:          formatVietnamHour(d.timestamp),
            timestamp:     new Date(d.timestamp).toISOString(),
            source:        nullableString(d.source),
            schemaVersion: nullableString(d.schema_version),
            aqi:           nullableNumber(d.us_aqi),
            pm25:          nullableNumber(d.pm2_5),
            pm25Mean12h:   nullableNumber(d.pm25_mean_12h),
            isStagnantAir: typeof d.is_stagnant_air === 'boolean' ? d.is_stagnant_air : null,
          };
        }),
      );
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: (err as Error).message });
    }
  },
);

// GET /api/forecast/:locationId?date=YYYY-MM-DD&hour=0-23
// Live (no date): reads forecast fields from realtime_alerts.
// Historical (with date): reads forecast fields from climate_observations.
app.get(
  "/api/forecast/:locationId",
  async (req: Request, res: Response) => {
    const stationId = LOCATION_MAP[req.params.locationId as string];
    if (!stationId) { res.status(400).json({ error: "Unknown location" }); return; }

    const dateParam = req.query.date;
    if (dateParam !== undefined && !isValidDateParam(dateParam)) {
      res.status(400).json({ error: "Invalid date. Expected a real YYYY-MM-DD date." });
      return;
    }
    const isLive = !dateParam;
    const { start, end } = dateWindow(dateParam);
    const hourParam = req.query.hour !== undefined ? Number(req.query.hour) : undefined;
    if (hourParam !== undefined && (!Number.isInteger(hourParam) || hourParam < 0 || hourParam > 23)) {
      res.status(400).json({ error: "Invalid hour. Expected an integer from 0 to 23." });
      return;
    }

    // Live forecasts come from realtime_alerts; historical from climate_observations.
    const forecastCollection = isLive ? COLLECTION_ALERTS : COLLECTION_HISTORICAL;

    try {
      const forecastFields = Array.from({ length: 6 }, (_, index) => [
        `weather_temp_forecast_h${index + 1}`,
        `weather_cloud_cover_forecast_h${index + 1}`,
        `weather_precipitation_forecast_h${index + 1}`,
        `weather_pressure_forecast_h${index + 1}`,
        `weather_wind_speed_forecast_h${index + 1}`,
        `aq_pm25_forecast_h${index + 1}`,
      ]).flat();
      const candidates = (await db
        .collection(forecastCollection)
        .find({
          station_id: stationId,
          timestamp: { $gte: start, $lt: end },
          $or: forecastFields.map((field) => ({ [field]: { $type: 'number' } })),
        })
        .toArray()) as Document[];

      if (!candidates.length) {
        res.status(404).json({ error: "Forecast data is not available for this location and time." });
        return;
      }

      // Pick the document closest to the requested hour (or most recent if no hour given)
      let doc: Document;
      if (hourParam !== undefined && !isNaN(hourParam)) {
        const targetMs = start.getTime() + hourParam * 60 * 60 * 1000;
        const target = new Date(targetMs);
        doc = candidates.reduce((best, d) => {
          const dDiff = Math.abs(new Date(d.timestamp).getTime() - target.getTime());
          const bDiff = Math.abs(new Date(best.timestamp).getTime() - target.getTime());
          return dDiff < bDiff ? d : best;
        });
      } else {
        doc = candidates.reduce((a, b) =>
          new Date(a.timestamp) > new Date(b.timestamp) ? a : b
        );
      }

      const baseHour = Number(formatVietnamHour(doc.timestamp).slice(0, 2));
      const baseDate = new Date(doc.timestamp);

      const hours = Array.from({ length: 6 }, (_, i) => {
        const h = i + 1;
        const forecastTime = new Date(baseDate.getTime() + h * 60 * 60 * 1000);
        return {
          hour: h,
          timestamp: forecastTime.toISOString(),
          time: formatVietnamHour(forecastTime),
          temperature:  doc[`weather_temp_forecast_h${h}`]           ?? null,
          cloudCover:   doc[`weather_cloud_cover_forecast_h${h}`]    ?? null,
          precipitation:doc[`weather_precipitation_forecast_h${h}`]  ?? null,
          pressure:     doc[`weather_pressure_forecast_h${h}`]       ?? null,
          windSpeed:    doc[`weather_wind_speed_forecast_h${h}`]      ?? null,
          pm25:         doc[`aq_pm25_forecast_h${h}`]                ?? null,
        };
      });

      res.json({
        baseHour,
        baseTime: `${String(baseHour).padStart(2, '0')}:00`,
        baseTimestamp: new Date(doc.timestamp).toISOString(),
        current: {
          temperature:        nullableNumber(doc.temp_c),
          humidity:           nullableNumber(doc.humidity),
          windSpeed:          nullableNumber(doc.wind_speed),
          pressure:           nullableNumber(doc.pressure),
          cloudCover:         nullableNumber(doc.cloud_cover),
          precipitation:      nullableNumber(doc.precipitation),
          pm25:               nullableNumber(doc.pm2_5),
          dewPoint:           doc.dew_point        ?? null,
          pressureDelta3h:    doc.pressure_delta_3h ?? null,
          tempMean6h:         doc.temp_mean_6h      ?? null,
        },
        hours,
      });
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: (err as Error).message });
    }
  },
);

// Derive a weather condition string from cloud_cover percentage
function cloudCoverToCondition(cloudCover: number | null): string {
  if (cloudCover === null) return "Unavailable";
  if (cloudCover <= 10) return "Clear";
  if (cloudCover <= 30) return "Mostly Clear";
  if (cloudCover <= 60) return "Partly Cloudy";
  if (cloudCover <= 85) return "Mostly Cloudy";
  return "Overcast";
}

connectDB()
  .then(() =>
    app.listen(PORT, () =>
      console.log(`Server running on http://localhost:${PORT}`),
    ),
  )
  .catch((err) => {
    console.error("Failed to connect to MongoDB:", err);
    process.exit(1);
  });
