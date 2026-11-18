# Big Data Climate Monitoring: proposal-aligned roadmap

This roadmap uses the proposal slide as a statement of intent, not as a fixed implementation specification. It distinguishes demonstrated behavior from configured or aspirational behavior so that project claims remain auditable.

## North star

Deliver a reproducible Lambda-style climate-data pipeline that demonstrates:

- **Volume:** scalable partitioned storage and measured batch/stream behavior, without claiming production-scale capacity from a small local sample.
- **Velocity:** continuously polled sources, Kafka buffering, Spark Structured Streaming, observable freshness, and a measured processing-latency target.
- **Variety:** weather and air-quality providers with explicit units, source identity, schema version, and nullable fields.
- **Veracity:** validation, retries/fallbacks, deduplication, dead-letter records, replay, and data-quality reporting.

The local implementation should remain simple enough to run on one Windows workstation. Kafka and Spark provide the distributed-processing concepts; MongoDB is serving storage; Parquet is the durable analytical format. HDFS and Airflow are optional deployment choices unless explicitly required by the course rubric. Do not represent observed measurements as forecasts.

## Proposal-to-repository assessment

| Proposal claim | Current evidence | Gap / interpretation |
|---|---|---|
| 63-city weather and six-city air-quality collection | Ingestion configuration lists 63 Open-Meteo locations, six AQICN locations, and six IQAir fallback locations. | Configuration is not proof of sustained collection or uniform provider coverage. The local live demonstration has been Hanoi-focused. |
| Scheduled ingestion | A non-overlapping 15-minute Task Scheduler job is registered for Hanoi under the logged-in user; both a manual and scheduled invocation produced successful structured summaries. | This verifies scheduling mechanics, not continuous multi-city coverage or five-minute observation availability. Polling does not make providers publish new observations at the same cadence. |
| Validation, retries, provider fallback, checkpoints, and DLQ | These mechanisms and the Kafka stream/DLQ topics exist in ingestion. | Need sustained-run summaries, source freshness/lag, DLQ review, and reliable operational failure status. |
| Spark streaming and sub-minute alerting | Spark Structured Streaming parses Kafka and writes derived observations to MongoDB. | There is no verified event-time stream-stream join, model inference, or measured sub-minute alert SLO. Independent raw weather and AQI observations are intentional because provider event times differ. |
| Historical batch, HDFS, and Parquet | A complete 30-day Open-Meteo archive backfill produced 45,360 weather and 45,360 air-quality observations for 63 cities. Spark preserved the complete Kafka snapshot in raw Parquet, wrote separate weather/AQI curated records, and upserted 90,940 observations in the replay. Weather and AQI now have distinct deterministic IDs. | HDFS utilities remain legacy/unwired. Overlap across snapshots remains visible in per-snapshot manifests and is not automatically compacted into one lake table. MongoDB remains serving storage. |
| Archive API and date-range dashboard | The API provides a 31-day-bounded history range with stable cursor pagination; the History tab displays source timestamps and nullable fields and exports the current page as CSV. | CSV export is page-scoped, not an unbounded whole-range download. API pagination and UI behavior are integration/component tested. |
| Forecasts and six environmental indicators | A separate ML package has training and scoring code; forecast UI correctly stays unavailable until real predictions exist. | The documented `Data/` parquet input is absent. The ML README describes PM2.5 horizons, not a verified six-indicator forecast. |
| Tens of millions of rows and <1-minute end-to-end alerts | The local database currently contains only a small number of Hanoi observations. | Scale and latency claims have not been benchmarked and must not be presented as demonstrated results. |

## Current architecture and design decisions

```text
Provider APIs
  -> Python collectors / validation / retry / fallback / checkpoints
  -> Kafka stream topics and DLQs
  -> Spark Structured Streaming
  -> MongoDB realtime and historical serving collections
  -> Express API
  -> React dashboard

Kafka snapshot + completeness manifest
  -> Spark historical processing
  -> immutable, partitioned raw/curated Parquet archive
  -> MongoDB historical serving collection
```

- Keep weather and AQI as separate facts. Any future joined advisory must define its event-time tolerance, watermark/late-arrival behavior, missing-source behavior, and threshold source.
- Preserve the canonical record contract: city/station, provider source, UTC event timestamp, schema version, units, and nulls.
- Keep credentials in the ingestion environment only. The batch/archive workflow must not need provider credentials.
- Use deterministic IDs for serving upserts and manifest validation for Kafka snapshot replay.
- Prefer a local Task Scheduler workflow over enabling the existing Airflow/Spark-submit scaffold unless Airflow is a grading requirement.

## Phase 1 and Phase 2 implementation status

- **Phase 1:** `scripts\collect_once.ps1` runs collectors sequentially, prevents overlap, and writes structured summaries. The Task Scheduler job `Climate Monitoring - Collect` is registered for Hanoi every 15 minutes; the manual run and first scheduled run both completed successfully. A fresh event was traced through Kafka, MongoDB, and the weather/AQI API. The schedule remains limited to Hanoi until provider quotas and sustained-run behavior are reviewed.
- **Phase 2:** complete export manifests are enforced before batch work. Dry-run was verified not to write. A 30-day all-city weather backfill produced 45,360 weather records; a separate Open-Meteo Air Quality API backfill produced 45,360 AQI records. The replay retained 94,575 Kafka envelopes across batch/live topics and produced 90,940 curated observations, with weather and AQI kept separate by record type in deterministic IDs. Immutable raw/curated Parquet snapshots and MongoDB history were written. The full Hanoi 30-day range is visible in the History tab; a Hanoi Vietnam-local day returns 24 weather and 24 AQI observations from both API endpoints. An initial mistaken shared Open-Meteo weather/AQI ID collision was found and fixed with a replay regression test.
- The raw JSONL plus its exporter manifest remain the auditable input under ignored `exports\`; archive snapshot manifests retain each input's SHA-256, topic counts, and partition offsets. The export timestamp is in the manifest, not repeated in each JSONL envelope, so identical retained Kafka records produce the same content hash.
- **Phase 3:** a range API and dashboard History tab have been added. Queries use inclusive Vietnam calendar dates, a 31-day maximum, a bounded page size, stable `(timestamp, _id)` cursors, nullable measurement fields, and page-level CSV download. Backend integration and frontend component tests pass; the running dashboard was also checked against MongoDB-backed Hanoi history.
- **Phase 4:** historical windows partition by station and provider source. The 3-hour pressure delta uses an event-time window limited to 2.5–3.5 hours before the current observation, selecting the closest prior pressure and returning null when no candidate qualifies. This avoids a range self-join and prevents unrelated provider records from contributing. Kafka export envelopes retain broker timestamps; streaming and batch paths calculate event→collector, collector→Kafka, and event→Kafka latency. The complete processor/batch suite passes **10/10 tests in 158.7 seconds**, including elapsed-time boundaries, late arrivals, duplicates, out-of-order input, provider isolation, and Parquet replay. Live metrics can show small negative collector→Kafka deltas when Windows and Docker clocks differ; this is documented as clock skew, not negative processing duration.
- **Phase 5:** the deterministic synthetic pipeline benchmark ran at 100, 1,000, 10,000, and 50,000 records. At 50,000, raw and curated Parquet read-back counts both matched the manifest and curated count; the isolated MongoDB benchmark collection contained 50,000 rows, separate from the 70 live historical observations at final verification. With Spark `local[1]`, 8 shuffle partitions, 12 reported host CPUs, and a 3.7-GiB Docker memory limit, the 10,000-record run took 92.0 seconds total (20.1 transform/count, 26.2 Parquet write, 9.2 read-back, 21.0 Mongo upsert); the 50,000 run took 198.8 seconds total (54.6, 75.9, 32.3, and 23.9 seconds respectively). A separate disposable three-partition Kafka test produced and consumed 10,000 records at 23,571 and 121,988 records/second, then deleted its topic. These are local synthetic measurements, not production capacity or real-time end-to-end SLA claims. Synthetic records were not sent to live topics or collections.
- **Still open:** quantify sustained real-source collection/freshness, verify task failure recovery, and compact/merge overlapping snapshot archives if a unified lake table becomes necessary. Benchmark sizes should be increased only after reviewing machine/container limits. HDFS/Airflow and ML remain out of scope unless the rubric requires them.

## Phase 1 — Reliable, observable collection

### Work

1. Provide a single, non-overlapping invocation for weather and AQI collectors. It should use the existing collector checkpoints and ingestion-only credentials, summarize produced/skipped/failed records, and return a nonzero exit status on actual failure.
2. Register a conservative Windows Task Scheduler task for the requested city set (15-minute default), running at the user's logon, starting missed runs when possible, and ignoring a new invocation while the previous run is active. Change the cadence or use all configured locations only after checking provider terms and quotas. Keep an explicit uninstall/update path.
3. Begin with the configured provider catalogs, while allowing a smaller city subset for demonstrations and respecting provider cadence/rate limits.
4. Report **poll time**, **provider event time**, and **last successful event** separately. Skipped unchanged observations are expected and should not be labeled as collector failures.
5. Add a sustained-run check for records per provider/city, failures, duplicate skips, and ingestion lag. Do not claim five-minute measurement cadence unless the provider actually supplies it.

### Acceptance

- The scheduled task action and cadence are inspectable; a manual run and a scheduled run both produce a structured summary.
- Two overlapping triggers do not run two copies of the collector workflow.
- Checkpoints are preserved, failures are visible, and API keys do not appear in task arguments/logs.
- A sustained run demonstrates incoming Kafka records and reports per-source freshness.

## Phase 2 — Reproducible, durable history

### Work

1. Create a complete Kafka snapshot with topic/partition offset ranges, per-topic counts, export time, completeness, byte size, and SHA-256. Reject incomplete or modified snapshots before Spark starts.
2. Preserve each verified raw snapshot and manifest as an auditable artifact.
3. From the same input, create raw/curated Parquet outputs partitioned by event date and source. Avoid `coalesce(1)` and avoid silently appending duplicate data on rerun.
4. Make snapshot identity and rerun behavior explicit. Reprocessing the same snapshot must be deterministic and idempotent; snapshots with overlapping offset ranges must be identifiable rather than mistaken for disjoint history.
5. Keep MongoDB historical upserts as a serving/query materialization, not as the only archive. Verify Parquet independently by reading it back and comparing counts, offsets, schema, and selected values.
6. **Implemented for weather and AQI:** Open-Meteo weather Archive records use `weather.raw.batch`; Open-Meteo Air Quality Archive records use `air_quality.raw.batch`. Both use UTC timestamps and the shared historical transformation without adding batch topics to the live streaming subscription. The daily incremental task has a 7-day UTC lookback and 31-day manual range limit.

### Acceptance

- A complete snapshot has a manifest and can be verified before processing.
- A dry run performs no archive or Mongo writes.
- A real run writes partitioned Parquet, records its source manifest, and upserts Mongo history.
- Repeating the same snapshot creates no duplicate logical observations and reproduces the same values.
- A read-back validation reports per-source/date counts and confirms important fields/nulls.

## Phase 3 — Historical exploration

1. **Implemented:** `GET /api/history/:locationId` requires start/end dates, validates real calendar dates and a maximum 31-calendar-day range, caps page size at 200, and uses an opaque cursor over stable `(timestamp, _id)` ordering.
2. **Implemented:** the History tab supports date range, previous/next cursor navigation, source/schema and UTC-to-ICT timestamps, explicit nulls, stage latency fields, and current-page CSV export.
5. **Implemented:** Overview 24-hour weather/AQI charts use elapsed-time x-axes (`timestampMs`, Recharts time scale), so sparse observations occupy their real distance instead of being compressed into evenly spaced category slots. Live data may still contain gaps; unavailable timestamps are not invented or interpolated.
3. Keep the existing hour selector as an observation-selection aid; it remains separate from the event-by-event history table.
4. Acceptance evidence: API integration tests cover date bounds, equal-timestamp pagination, cursor validation, page limits, nulls, and excluded non-observations; frontend tests cover page navigation, nullable fields, and CSV escaping.

## Phase 4 — Event-time processing and alert semantics

1. **Implemented:** raw weather/AQI records remain independent. Historical rolling windows now partition by station and source, preventing different providers from being averaged together.
2. **Implemented:** a full retained snapshot recomputes windows for all included records and deterministic Mongo IDs replace prior feature values, so late records revise affected observations when that complete history is rerun. A partial export is not sufficient for correct historical windows; no streaming watermark or cross-source join is claimed.
3. **Implemented:** stable timestamp/ID ordering makes wind-shear lag deterministic. Pressure matching retains its ±30-minute tolerance around the three-hour target and uses a bounded bucketed candidate join. Tests include gaps, source isolation, out-of-order and duplicate records, and a late record changing a prior window without changing its observation ID.
4. **Implemented (instrumentation):** export envelopes carry Kafka broker timestamps; feature documents retain event/collector/Kafka times and their derived delays. Live batches log p50/p95 upstream delay and Spark-to-Mongo callback duration; historical runs log transform/Mongo and Parquet durations/throughput. API history exposes stored timing fields.
5. Advisory thresholds remain out of scope until checked against authoritative guidance; no alert SLO is claimed. Spark timing tests and repeated benchmarks still need to be run and recorded on the target host.
6. Acceptance: the test definitions and measurements now exist; do not make a latency/SLO claim until benchmark reports from actual runs support it.

## Phase 5 — Big-data benchmark and recovery

1. **Implemented:** deterministic climate-shaped snapshots with complete manifests; synthetic rows never go to live topics.
2. **Implemented:** `scripts\run_pipeline_benchmark.ps1` measures local Kafka produce/consume in a temporary topic, Spark transform/count, partitioned Parquet write/read-back, optional writes to `climate_benchmark.synthetic_observations`, and a separately labeled API range-query sample. Reports record dataset hash/count/bytes, Spark configuration, stage timings, throughput, and Parquet file/byte counts.
3. **Implemented:** snapshot archive replay and processor checkpoint recovery have been tested. Full benchmark sizes should be run sequentially because Spark competes for local CPU/memory; Kafka benchmark topic is deleted after consumption.
4. Acceptance: no tens-of-millions or production-capacity claim. Run increasing synthetic sizes, retain the generated `logs\benchmarks\spark_benchmark_*.json` report, compare records/sec and bytes, and disclose that the API sample is not under synthetic load.

## Phase 6 — ML integration (deferred)

1. Resolve the absent `Data/` training input and reconcile it with the ML package documentation.
2. Build model features from the finalized historical contract and verify chronological train/validation/test splits.
3. Start with one target/horizon and compare against a simple baseline.
4. Register only evaluated models. Persist model version, prediction time, horizon, and feature/source timestamp.
5. Connect forecast API/UI only when real, identifiable predictions exist.

## Recommended execution order

1. Phase 1: non-overlapping scheduled collection and observable freshness.
2. Phase 2: complete snapshots, durable Parquet archive, deterministic replay, and Mongo serving upserts.
3. Phase 3: browse/export the resulting history.
4. Phase 4 and 5: prove event-time correctness and big-data performance.
5. Phase 6 only when a valid dataset is available.

This order prioritizes the assignment's big-data story—streaming, batch, durable partitioned storage, veracity, and measurable scale—without building a cluster or claiming a forecast before the data justifies it.
