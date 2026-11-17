# Climate Monitoring Ingestion

This module is the streaming ingestion boundary of the climate monitoring pipeline. It fetches live data from real external APIs, normalizes records into the shared schema, validates them, and publishes accepted records to Kafka. Failed fetch, validation, and processing events go to DLQ topics.

## Architecture

```text
External APIs
   ->
Streaming collectors
   ->
Normalize + Validate
   ->
Kafka raw stream topics / DLQ topics
   ->
Downstream consumers
```

The production stream entrypoints use these fallback chains:

- weather: `Open-Meteo -> OpenWeatherMap`
- air quality: `AQICN -> IQAir`

Duplicate polling results are skipped and logged. They are not sent to DLQ because an unchanged `current` API response is expected in a short polling window.

Weather timestamps are requested in UTC before normalization. The weather checkpoint filename is versioned so older checkpoints created with local-time-as-UTC timestamps do not suppress new, correctly timed observations.

## Delivered scope

- Kafka local stack via Docker Compose
- topic creation for stream and DLQ topics
- streaming collectors for Open-Meteo, OpenWeatherMap, AQICN, and IQAir
- source fallback in the production stream collectors
- shared retry, logging, metadata, checkpoint, and serialization helpers
- DLQ routing for fetch, validation, and processing failures
- GE-aligned input quality rules for downstream validation
- Kafka JSONL export with offset-range and integrity manifests for historical replay

## Source of truth

- production stream entrypoints:
  - `Ingestion/collectors/weather_stream_collector.py`
  - `Ingestion/collectors/aqicn_air_stream_collector.py`
- shared configuration and path helpers: `Ingestion/utils/runtime_config.py`
- shared location catalog: `Ingestion/utils/locations.py`
- normalized schema: `Ingestion/validators/normalized_schema.py`
- validation rules: `Ingestion/validators/`

## Folder layout

```text
Ingestion/
|-- collectors/     # Streaming ingestion entrypoints and source clients
|-- config/         # Kafka and local environment configuration
|-- great_expectations/
|-- producers/      # Kafka and DLQ producer helpers
|-- scripts/        # Topic setup, auditable export, and validation helpers
|-- utils/          # Shared runtime helpers and source catalogs
|-- validators/     # Normalization and validation logic
|-- docker-compose.yml
`-- requirements.txt
```

The root `docker-compose.airflow.yml` and the old Spark-submit DAG are an unsupported scaffold. Use the root [`scripts\run_historical_batch.ps1`](../scripts/run_historical_batch.ps1) with Windows Task Scheduler for local batch orchestration; live collection remains in the collector processes.

## Kafka topics

- `weather.raw.stream`
- `weather.raw.batch` (historical Open-Meteo archive records; processed by historical batch, not live Spark)
- `weather.raw.dlq`
- `air_quality.raw.stream`
- `air_quality.raw.dlq`

## Main commands

Install dependencies:

```powershell
python -m pip install -r Ingestion\requirements.txt
```

Start Kafka:

```powershell
docker compose -f Ingestion\docker-compose.yml up -d
```

Create topics:

```powershell
python Ingestion\scripts\create_kafka_topics.py
```

Run one weather stream cycle:

```powershell
python Ingestion\collectors\weather_stream_collector.py --run-once --locations Hanoi
```

Run one air-quality stream cycle:

```powershell
python Ingestion\collectors\aqicn_air_stream_collector.py --run-once --locations Hanoi
```

Export real Kafka data to a local JSONL file:

```powershell
python Ingestion\scripts\export_kafka_to_file.py --topics weather.raw.stream air_quality.raw.stream --from-beginning --max-messages 500 --include-kafka-meta
```

The exporter writes a sibling manifest containing per-partition offsets, counts, completeness, file size, and SHA-256. A capped export is marked incomplete if it stops before reaching the captured Kafka partition ends; use `--max-messages 0` for an uncapped snapshot.

Run a bounded historical weather backfill from the repository root. The default is Hanoi; use `-AllLocations` for the configured Open-Meteo city catalog. Backfills use UTC event times and a separate Kafka topic, then the wrapper validates/exports records and applies them through the historical Spark batch:

```powershell
.\scripts\run_weather_backfill.ps1 -StartDate 2026-10-12 -EndDate 2026-11-10 -Locations Hanoi
```

## Required API keys

- `OWM_API_KEY` enables OpenWeatherMap weather fallback.
- `AQICN_API_KEY` is required for the production air-quality stream.
- `IQAIR_API_KEY` enables IQAir air-quality fallback.

Put local keys in `Ingestion/config/.env` or pass supported `--api-key` options on source-specific collectors.

## Validation model

- ingestion validators perform blocking checks before Kafka publish
- Great Expectations-compatible rules are prepared as a downstream quality layer
- shared thresholds live in `Ingestion/utils/quality_rules.py`

## Handover notes

- Historical replay is handled by the separate Spark batch job under `processing\batch\historical_flow.py`.
- No fake data, sample payloads, or smoke-test producers are included.
- Keep real API keys only in `Ingestion/config/.env`.
- Local batch scheduling is documented in the root README; Airflow is not configured for this local MVP.
