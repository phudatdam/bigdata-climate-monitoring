# Local Kafka-to-MongoDB processing

The local MVP consumes normalized weather and air-quality events from Kafka, calculates weather features with Spark, and writes observations to MongoDB. It does not load ML models or require HDFS.

## Data flow

```text
weather.raw.stream ─┐
                    ├─ Spark Structured Streaming ─ MongoDB
air_quality.raw.stream ┘                         ├─ realtime_observations
                                                  └─ climate_observations
```

Each event is stored independently. Weather and air-quality APIs can report different event times and station identifiers, so the processor canonicalizes the station from the city/coordinates rather than requiring an event-time join. MongoDB IDs are deterministic to make Spark replays idempotent. The stored contract includes canonical lowercase `station_id`, UTC BSON `timestamp`, provider `source`, `schema_version` from ingestion's `data_version`, `ingestion_timestamp`, and Kafka's record timestamp; unavailable measurements stay null.

The shared Spark transformation calculates coordinate projections, Vietnam-local cyclic time features, air density, wind components, dew point, stagnation, and cooling degree days. The historical batch path additionally calculates provider-isolated weather and AQI windows; the two measurement streams and different providers are never blended. The 6-hour temperature mean and 12-hour PM2.5 mean use observations whose event timestamps fall within those elapsed-time windows. The 3-hour pressure delta compares against the nearest earlier same-source weather pressure within 30 minutes of the 3-hour target; it is null when no such observation exists. Its bounded event-time window avoids a station-wide range self-join. Wind shear is a deterministic difference from the preceding same-source weather observation, ordered by timestamp and record ID.

Historical window features are recomputed from all records in the supplied export. Re-exporting the complete retained Kafka history and rerunning the batch lets late events revise affected deterministic Mongo records. A partial export may not contain the window context and must not be treated as a full-history recomputation. There is no streaming watermark or weather/AQI join in the live path.

## Start the processor

From the repository root, start Kafka first so Docker Compose creates the shared network:

```powershell
docker compose -f Ingestion\docker-compose.yml up -d kafka
docker compose -f database\docker-compose.mongo.yml up -d
```

Create ingestion topics if they have not already been created:

```powershell
python Ingestion\scripts\create_kafka_topics.py
```

Build and run Spark in the foreground to see its logs:

```powershell
docker compose -f processing\docker-compose.yaml up --build processor
```

The container uses Python 3.11, Java 17, Spark 3.5.1, and the Spark Kafka/MongoDB connectors. It connects to Kafka over the shared Docker network and writes to `climate_db.realtime_observations` and `climate_db.climate_observations`. Spark's checkpoint is persisted at `checkpoint\processing`.

The processor defaults to `latest` offsets on its first start, then resumes from its checkpoint. Start it before running collectors so it receives their next messages. To replay existing Kafka data, set `KAFKA_STARTING_OFFSETS=earliest` and use a new checkpoint directory; do not remove an existing checkpoint unless you intend to replay the topics.

## Processor tests

The focused Spark transformation tests run inside the processor image:

```powershell
docker compose -f processing\docker-compose.yaml run --build --rm --no-deps processor python -m unittest discover -s processing/tests -v
```

They verify city canonicalization, independent source records, timestamp parsing, null preservation, malformed-record rejection, stable document IDs, latency calculations, elapsed-time windows, source isolation, duplicate/out-of-order inputs, late-event recomputation, and Parquet replay. MongoDB upserts use deterministic IDs, so replay replaces the existing observation instead of creating another document.

## Historical Kafka-export batch

The historical batch processor reuses the same parser and stateless features as the live processor. It accepts JSONL files produced by the Kafka exporter with `--include-kafka-meta`, calculates historical windows over the supplied files, and idempotently upserts results into `climate_db.climate_observations`. Pass all export files covering the desired history in one run: processing a partial file later recomputes windows only from that file for its records. Window feature durations are based on event timestamps, not record counts.

The orchestration script also writes immutable raw Kafka-envelope and curated-observation Parquet snapshots under `archive\raw_kafka\` and `archive\curated_observations\`. Each snapshot is partitioned by UTC event date and source (`year`, `month`, `day`, `source`), keyed by the validated export SHA-256, and accompanied by an archive manifest recording source offsets, topic counts, and row count. Reprocessing the identical export reuses its snapshot; it does not overwrite existing Parquet. The raw JSONL remains the auditable source input, and MongoDB remains the serving store.

Open-Meteo historical weather and air quality are collected to separate `weather.raw.batch` and `air_quality.raw.batch` topics. Spark's live query intentionally does not subscribe to batch topics; export them with retained stream topics and run the historical batch to update `climate_observations`. The bounded PowerShell entry point defaults to Hanoi and accepts at most 31 dates:

```powershell
.\scripts\run_weather_backfill.ps1 -StartDate 2026-10-12 -EndDate 2026-11-10 -Locations Hanoi
.\scripts\run_weather_backfill.ps1 -StartDate 2026-10-12 -EndDate 2026-11-10 -AllLocations
```

The collectors request UTC timestamps so naive archive API hour labels are interpreted correctly by the common schema. The wrapper exports both batch topics together with retained weather/AQI stream topics, validates the export manifest, writes raw and curated Parquet, and idempotently upserts historical observations. It never resets live offsets/checkpoints or writes backfill records to realtime collections. All-location runs make one archive request per configured Open-Meteo city for each weather and air-quality API. Weather and AQI use separate deterministic IDs even when the provider timestamp is identical.

Export the Kafka topics to a file from the repository root:

```powershell
python Ingestion\scripts\export_kafka_to_file.py --topics weather.raw.stream air_quality.raw.stream --from-beginning --max-messages 0 --include-kafka-meta
```

The exporter writes a JSONL file and a sibling `.manifest.json` with the snapshot's per-partition low/high offsets, counts, completion status, file size, and SHA-256 digest. `--max-messages 0` means no record cap; a positive cap is useful for sampling but does not produce a complete archive unless all captured partition ends were reached. The historical processor validates completeness and file integrity when `--manifest` is supplied. Kafka-metadata envelopes include the broker timestamp in milliseconds when available.

The processor retains event, collector-ingestion, and Kafka timestamps and derives `event_to_ingestion_seconds`, `ingestion_to_kafka_seconds`, and `event_to_kafka_seconds`; missing legacy values remain null. These are clock-derived deltas: a small negative collector-to-Kafka value indicates clock skew/precision between the Windows collector and Docker Kafka clock, not negative processing time. Synchronize clocks before interpreting latency. Live processor logs emit p50/p95 upstream delays and the total Spark-to-Mongo callback duration per micro-batch. Historical runs report transform/Mongo and Parquet duration/throughput. These are measurements, not an alert latency SLO.

Run a no-write preview first, replacing the example basename with the JSONL file and matching manifest in `exports\`:

```powershell
docker compose -f processing\docker-compose.yaml run --build --rm --no-deps historical-batch --input /workspace/exports/kafka_export_YYYYMMDD_HHMMSS.jsonl --manifest /workspace/exports/kafka_export_YYYYMMDD_HHMMSS.jsonl.manifest.json --dry-run
```

If the preview reports the expected valid unique record count, run the same command without `--dry-run` to write the Parquet snapshots and upsert the batch. To rebuild the batch image after changing processing code, pass `-BuildProcessor` to `scripts\run_historical_batch.ps1`; otherwise the script uses the existing image. Exports without a manifest remain available for legacy/manual investigation but log that completeness is unverified; Parquet archiving specifically requires a validated complete manifest. No Kafka offsets or streaming Spark checkpoint are reset, and no existing MongoDB documents are deleted. Batch writes only to the durable historical collection.

The batch service defaults to 8 Spark shuffle partitions for the local dataset. Set `SPARK_SQL_SHUFFLE_PARTITIONS` in the Compose environment to tune this for larger exports or available hardware. The PowerShell orchestration entry point is `scripts\run_historical_batch.ps1`; it runs export → manifest validation → Spark batch and writes a transcript in `logs\`. It is suitable for Windows Task Scheduler and does not receive ingestion API keys.

## Synthetic scale benchmark

From the repository root, run:

```powershell
.\scripts\run_pipeline_benchmark.ps1 -Records 100 -KafkaRecords 100 -SkipMongo -SkipKafka
.\scripts\run_pipeline_benchmark.ps1 -Records 1000 -KafkaRecords 1000
.\scripts\run_pipeline_benchmark.ps1 -Records 10000 -KafkaRecords 10000
```

Start with the 100-record no-write smoke run, then increase sizes sequentially only while Docker remains healthy. The runner defaults to 100 Spark records and 1,000 Kafka records. Spark uses `local[1]` by default; set `SPARK_MASTER` and `SPARK_SQL_SHUFFLE_PARTITIONS` in the environment only when intentionally tuning a larger run. The generator writes deterministic synthetic weather/AQI Kafka-shaped envelopes and a completeness manifest; no provider is contacted and no synthetic records are written to the live climate Kafka topics. A disposable uniquely named Kafka topic measures broker produce/consume throughput and is deleted afterward. Spark reports transform/count, partitioned Parquet write/read-back, file counts and bytes. By default it also writes to the isolated `climate_benchmark.synthetic_observations` database/collection; pass `-SkipMongo` to omit that stage. An API sample runs against the current history endpoint separately and is not under synthetic load. Reports are saved in ignored `logs\benchmarks\`; snapshots live in ignored `exports\benchmark\` and `archive\benchmark_runs\`. Record the computer, container limits, and Spark settings alongside results. Do not extrapolate a workstation result to production capacity.

## Expected MongoDB documents

`station_id` is the canonical lowercase city name used by the API (`hanoi`, `da nang`, etc.). Weather and air-quality messages are separate documents; fields not present in the source event remain null. The backend filters for records containing the requested measurements.

Weather records also include derived `dew_point`, `wind_U`, `wind_V`, `is_stagnant_air`, and `cooling_degree_days` values. Historical records are retained in `climate_observations`; `realtime_observations` is the live serving collection.

## Local Python run (optional)

The Docker processor is the supported local path because it pins Python 3.11, Java 17, and Spark 3.5.1. To run it directly, use Python 3.11, install `processing\requirements.txt`, and set:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:MONGODB_URI = "mongodb://localhost:27017"
python -m processing.streaming.streaming_flow
```

The Spark Kafka and MongoDB connector artifacts are downloaded when the Spark session starts, so Maven Central must be reachable.
