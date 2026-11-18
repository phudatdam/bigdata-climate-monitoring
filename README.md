# Big Data Climate Monitoring

This repository contains a climate-data ingestion pipeline, Spark processing code, a MongoDB-backed API, and a React dashboard. The supported local path includes both live Kafka-to-Spark processing and an explicit historical Kafka-export batch replay into MongoDB. ML forecasting and HDFS/Airflow deployment remain follow-on work.

## Local MVP architecture

```text
Open-Meteo / AQICN
        ↓
Python collectors → Kafka topics → Spark Structured Streaming
                                       ↓
                             MongoDB observations
                                       ↓
                              Express API → React UI
```

Weather and air-quality events are stored independently rather than joined on event time. Their providers can report different observation times and station IDs. Spark canonicalizes both to the configured city and writes live and durable observation collections. API keys are only needed by ingestion; the app and processor connect to local services.

## Requirements

- Docker Desktop running
- Node.js and npm
- Python 3.11 for optional host-side scripts; the Spark processor container supplies Python 3.11, Java 17, and Spark 3.5.1
- API keys in `Ingestion/config/.env` for AQICN; OWM and IQAir keys are optional fallbacks

Do not commit API keys or local `.env` files.

## Run locally (PowerShell)

Run these commands from the repository root.

### 1. Start Kafka and MongoDB

```powershell
docker compose -f Ingestion\docker-compose.yml up -d kafka
docker compose -f database\docker-compose.mongo.yml up -d
```

The local MongoDB Compose service has no authentication and is intended only for development. It uses a persistent Docker volume. Do not expose it to an untrusted network.

### 2. Install ingestion dependencies and prepare Kafka topics

```powershell
python -m pip install -r Ingestion\requirements.txt
python Ingestion\scripts\create_kafka_topics.py
```

### 3. Start Spark processing

In its own terminal, run:

```powershell
docker compose -f processing\docker-compose.yaml up --build processor
```

Keep it running to process Kafka records. On first startup it waits for new messages; on later starts it resumes from the persistent Spark checkpoint. See [processing/README.md](processing/README.md) for replay and output details.

### One-command startup and check

From the repository root, this helper starts Kafka, MongoDB, and Spark only when needed, creates any missing Kafka topics, starts the backend/frontend dev servers if they are not responding, and checks the API data routes. It uses the existing Spark image by default:

```powershell
.\scripts\start_local_mvp.ps1
```

The script does not reset Kafka offsets or remove checkpoints. Add `-RunCollectors` to make one-shot Hanoi readings after the services are ready; unchanged source data may be skipped by the collector checkpoints:

```powershell
.\scripts\start_local_mvp.ps1 -RunCollectors
```

It records command output under `logs\local-mvp\` and uses bounded waits so an unavailable Docker Engine or stalled collector fails with a diagnostic instead of hanging indefinitely. Pass `-BuildProcessor` to explicitly rebuild the Spark image. It does not install dependencies; use the setup steps below if a project has no `node_modules` or Python packages installed.

### Optional: scheduled collectors

The one-shot wrapper runs weather and AQICN sequentially, guards against overlapping runs, and writes a transcript plus JSON summary under `logs\collection\`. The summary distinguishes poll start/finish from the latest provider event time and includes both produced and duplicate-skipped observations. Start with Hanoi at a 15-minute cadence; an empty location list means all configured locations and should only be used after provider quotas/cadence are reviewed:

```powershell
.\scripts\collect_once.ps1 -Locations Hanoi
.\scripts\register_collection_task.ps1 -Locations Hanoi -IntervalMinutes 15
Get-ScheduledTask -TaskName "Climate Monitoring - Collect"
.\scripts\register_collection_task.ps1 -Unregister
```

The task runs only while the current user is logged on. Check its last-run result and the newest collection summary. Skipped unchanged observations are normal; failed counts or a failed task status require investigation.

### 3a. Optional: replay exported Kafka history

The exporter captures Kafka partition offset boundaries and writes a `.manifest.json` beside the JSONL file. The manifest includes per-topic/partition counts, offset ranges, a SHA-256 digest, and whether every captured partition end was reached. A message-limited or timed-out export is marked incomplete and rejected by the manifest-validated batch path. Exporting all retained messages can produce large files:

```powershell
python Ingestion\scripts\export_kafka_to_file.py --topics weather.raw.stream air_quality.raw.stream --from-beginning --max-messages 0 --include-kafka-meta
docker compose -f processing\docker-compose.yaml run --build --rm --no-deps historical-batch --input /workspace/exports/kafka_export_YYYYMMDD_HHMMSS.jsonl --manifest /workspace/exports/kafka_export_YYYYMMDD_HHMMSS.jsonl.manifest.json --dry-run
```

Replace the example filename in both paths with the generated export. After reviewing the dry-run count, rerun without `--dry-run` to upsert and write immutable raw/curated Parquet snapshots partitioned by UTC date and source to `archive\`. The archive records the verified export manifest and is replay-safe for the same file. Deterministic IDs make completed MongoDB reruns idempotent; neither Kafka offsets nor the live Spark checkpoint is reset. The convenience script `.\scripts\run_historical_batch.ps1` runs the full workflow; pass `-DryRun` for a no-write preview. It builds the batch image if missing; pass `-BuildProcessor` after changing its code.

### 3b. Optional: schedule a historical batch

For this single-machine assignment, Windows Task Scheduler is the supported local scheduler; the separate Airflow Compose stack is not part of the MVP and its Spark-submit scaffold is not configured. The script exports and validates a complete retained Kafka snapshot before running the batch, writes a transcript under `logs\`, and returns a failing exit code if any step fails. It does not need provider API keys:

```powershell
.\scripts\run_historical_batch.ps1 -DryRun
```

After the preview succeeds, omit `-DryRun` when scheduling. In Task Scheduler, create a daily task whose action runs `powershell.exe` with arguments `-NoProfile -ExecutionPolicy Bypass -File "C:\path\to\bigdata-climate-monitoring\scripts\run_historical_batch.ps1"`. Inspect the task's last-run result and corresponding transcript in `logs\`; reruns are safe for the same full archive. Avoid scheduling frequent full-topic snapshots unless the retained Kafka volume and available disk/CPU have been checked.

### 3c. Backfill archived weather and air-quality observations

Open-Meteo provides both weather and air-quality archive APIs. The wrapper defaults to Hanoi, limits a run to 31 calendar days, and writes weather to `weather.raw.batch` and air quality to `air_quality.raw.batch`. `-AllLocations` requests the full configured city catalog. It then exports the backfill with retained stream history, validates the manifest, writes Parquet snapshots, and upserts `climate_observations`:

```powershell
.\scripts\run_weather_backfill.ps1 -StartDate 2026-10-12 -EndDate 2026-11-10 -Locations Hanoi
.\scripts\run_weather_backfill.ps1 -StartDate 2026-10-12 -EndDate 2026-11-10 -AllLocations
```

Backfilled events are UTC observations and do not populate the realtime collection. The dedicated batch topics are consumed only by the historical batch, never by the live Spark stream.

For a daily incremental 7-day UTC lookback, register and run the Windows scheduled wrapper:

```powershell
.\scripts\register_backfill_task.ps1 -LookbackDays 7
.\scripts\run_climate_backfill.ps1 -LookbackDays 7
```

### 3d. Browse bounded history

The dashboard's **History** tab shows event-by-event weather and AQI records, including source/schema, the observation timestamp in ICT, nullable measurements, and timing fields. Select a start/end range (maximum 31 calendar days), navigate pages, or download the current page as CSV. The history endpoint returns stable cursor pages and excludes documents without weather or AQI measurements:

```powershell
Invoke-RestMethod "http://localhost:3001/api/history/hanoi?startDate=2026-11-01&endDate=2026-11-09&limit=100"
```

The API uses inclusive Vietnam calendar dates. `nextCursor` is opaque; pass it unchanged as the `cursor` query parameter to retrieve the next page. This is separate from the Overview tab's hour selector, which picks the nearest actual observation and does not create missing hourly readings.

### 3e. Reproducible pipeline benchmarks

Use synthetic input to benchmark without increasing provider API traffic. The command creates a deterministic manifest-validated snapshot, uses a disposable Kafka topic for produce/consume measurement, and runs Spark transform + partitioned Parquet write/read-back. By default MongoDB writes go to the isolated `climate_benchmark.synthetic_observations` collection; live climate collections are not modified. Reports go to `logs\benchmarks\`, while synthetic files and Parquet outputs remain under ignored `exports\` and `archive\`:

```powershell
.\scripts\run_pipeline_benchmark.ps1 -Records 100 -KafkaRecords 100 -SkipMongo -SkipKafka
.\scripts\run_pipeline_benchmark.ps1 -Records 1000 -KafkaRecords 1000
.\scripts\run_pipeline_benchmark.ps1 -Records 10000 -KafkaRecords 10000
```

The runner defaults to 100 Spark records and 1,000 Kafka records. Spark uses `local[1]` by default; set `SPARK_MASTER` and `SPARK_SQL_SHUFFLE_PARTITIONS` only when intentionally tuning a larger run. The API sample uses the current endpoint separately and is not under synthetic load. Use `-SkipKafka` if the broker is unavailable, `-SkipMongo` to omit the isolated Mongo write, and `-ApiUrl` / `-ApiDate` to select the API sample. Run volume steps sequentially; keep the JSON reports with the machine and Spark settings when presenting throughput. Results characterize this workstation only and do not substantiate production-scale claims.

### 4. Collect current readings

In another terminal:

```powershell
python Ingestion\collectors\weather_stream_collector.py --run-once --locations Hanoi
python Ingestion\collectors\aqicn_air_stream_collector.py --run-once --locations Hanoi
```

Use separate terminals or run each command one after the other. The collectors should report a produced record; later polls may skip an unchanged observation. The weather request uses UTC timestamps so downstream processing does not mistake local time for UTC.

### 4a. Verify the live path

For a one-time smoke check, note each collector's `event_time`, `partition`, and `offset` from its JSON log. Read that exact Kafka record (replace the offset with the collector's value):

```powershell
docker exec ingestion-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic weather.raw.stream --partition 1 --offset <WEATHER_OFFSET> --max-messages 1 --timeout-ms 10000
docker exec ingestion-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic air_quality.raw.stream --partition 1 --offset <AQI_OFFSET> --max-messages 1 --timeout-ms 10000
```

Confirm the newest `hanoi` documents in `climate_db.realtime_observations`:

```powershell
docker exec database-mongodb-1 mongosh --quiet --eval "for (const topic of ['weather.raw.stream','air_quality.raw.stream']) { const source = topic === 'weather.raw.stream' ? 'open-meteo' : 'aqicn'; printjson(db.getSiblingDB('climate_db').realtime_observations.find({station_id:'hanoi',source}).sort({timestamp:-1}).limit(1).toArray()); }"
```

The Mongo `timestamp` must match the collector event time in UTC, with the expected weather or AQI fields; `source`, `schema_version`, and nulls should be retained. Compare the same record in `http://localhost:3001/api/weather/hourly/hanoi` or `/api/air-quality/hanoi`, then refresh the dashboard and check the source, ICT observation time, values, and stale label. The integration tests also verify Mongo-to-API timestamp/value equality.

For a normal processor recovery check after ingestion, restart only the processor service and confirm it returns to running and continues consuming. Do not remove `checkpoint\processing` or change offsets:

```powershell
docker compose -f processing\docker-compose.yaml restart processor
docker compose -f processing\docker-compose.yaml logs --tail 50 processor
```

### 5. Start the backend API

In a new terminal:

```powershell
Copy-Item backend\.env.example backend\.env
npm --prefix backend install
npm --prefix backend run dev
```

Check `http://localhost:3001/api/health`; it should return `{"status":"ok"}`. The backend creates its query indexes on startup.

### 6. Start the React dashboard

In another terminal:

```powershell
Copy-Item frontend\.env.example frontend\.env
npm --prefix frontend install
npm --prefix frontend run dev
```

Open the local Vite URL printed in the terminal (normally `http://localhost:5173`) and select Hanoi. The overview loads weather and air-quality observations through the Express API and labels weather readings older than three hours as stale. The forecast tab intentionally displays an unavailable message until ML predictions are connected.

With MongoDB and the backend available, run the API integration suite. It starts an isolated test API/database and removes only that test database when finished:

```powershell
npm --prefix backend run test:integration
```

The dashboard's focused freshness, missing-value, and timestamp-matching tests run with:

```powershell
npm --prefix frontend test
```

## MongoDB collections

- `climate_db.realtime_observations`: recent measurements used by the dashboard; records expire after 48 hours
- `climate_db.climate_observations`: durable measurements used by date-selected historical views
- `climate_db.realtime_alerts`: reserved for future forecast output; not populated by this non-ML MVP

Each Kafka event becomes a document with canonical lowercase `station_id`, UTC BSON `timestamp`, `source`, and `schema_version` (carried from ingestion `data_version`, currently `v1`). Weather and AQI are separate records. Measurements absent from the source remain null rather than being replaced with zero.

| Contract field | Meaning / unit |
|---|---|
| `city`, `station_id` | Canonical configured city name and its lowercase API/database key |
| `source` | Provider identifier such as `open-meteo` or `aqicn`; interpret provider-specific values with this field |
| `timestamp` | Source event time stored as a BSON date in UTC; API timestamps are ISO-8601 UTC strings |
| `ingestion_timestamp` | Collector ingestion time, used to measure source-event-to-collector delay |
| `kafka_timestamp` | Kafka record timestamp, used to measure collector-to-broker delay |
| `event_to_ingestion_seconds`, `ingestion_to_kafka_seconds`, `event_to_kafka_seconds` | Timing deltas in seconds; null when the source envelope lacks the necessary timestamps |
| `temp_c`, `soil_temperature` | Degrees Celsius |
| `humidity`, `cloud_cover` | Percent |
| `pressure` | hPa |
| `wind_speed` | m/s; the UI converts to km/h where shown |
| `wind_dir` | Degrees clockwise from north |
| `precipitation` | mm |
| `shortwave_radiation` | W/m² |
| `pm2_5` | Provider concentration value in µg/m³ when supplied |
| `us_aqi` | Provider's AQI scale (scale may differ by source) |
| `schema_version` | Version of the normalized ingestion contract |

Weather and AQI documents contain different measurement fields; the API filters requests to the matching record type and returns its source, event timestamp, and schema version. The history route caps date ranges at 31 days and pages up to 200 records at a time. See [processing/README.md](processing/README.md).

Historical features include an elapsed-time six-hour temperature mean, an elapsed-time twelve-hour PM2.5 mean, and a three-hour pressure delta matched to a prior observation within a 30-minute tolerance. The PM2.5 field is a mean concentration (`pm25_mean_12h`), not a sum of concentration samples.

## Repository areas

- [Ingestion/](Ingestion): API collectors, Kafka publishers, normalization, validation, checkpoints, and DLQ
- [processing/](processing): Spark stream processor and existing batch/feature-engineering code
- [database/](database): local MongoDB Compose setup and initialization utility
- [backend/](backend): Express API for weather, air quality, and optional forecast reads
- [frontend/](frontend): React dashboard
- [scripts/run_pipeline_benchmark.ps1](scripts/run_pipeline_benchmark.ps1): synthetic Kafka/Spark/Parquet/Mongo/API measurement runner
- [air-quality-ml/](air-quality-ml): separate training, scoring, monitoring, MLflow, and Streamlit code; deferred

The ML package expects a separate `Data/` parquet dataset, which is not included in this checkout. HDFS storage, Airflow scheduling, real forecasts, and production deployment are not required for the local live-dashboard and historical-replay paths. AQI-to-weather advisory matching uses the nearest event timestamp only when it is within 90 minutes; otherwise the dashboard reports no matched AQI reading. Forecast views remain unavailable until forecast fields are actually written.
