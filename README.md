# Airflow Batch and Streaming Demo

This repository demonstrates two complementary data processing strategies built
with Apache Airflow:

* **Batch Weatherstack pipeline** – polls the Weatherstack API on a slower
  cadence, snapshots multiple cities at once, derives aggregated metrics, and
  writes timestamped JSON outputs.
* **Streaming-inspired pipeline** – polls the same Weatherstack API at a much
  higher frequency, processes only readings that were not previously seen, and
  stores incremental insights.

Both DAGs are self-contained Python files that can be dropped into an Airflow
`dags/` directory for experimentation or demos.

## Repository layout

```
├── dags/
│   ├── batch_pipeline.py         # Scheduled batch DAG
│   └── streaming_pipeline.py     # Minute-level streaming DAG
├── data/
│   ├── batch/
│   │   ├── raw/                  # Sample Weatherstack snapshots captured in batch runs
│   │   ├── processed/            # Sample + runtime batch outputs
│   │   └── state.json            # Tracks the latest processed observation per city
│   └── stream/
│       ├── raw/                  # JSONL weather log appended by the DAG
│       ├── processed/            # Sample + runtime streaming outputs
│       └── state.json            # DAG checkpoint storing latest observation per city
├── scripts/
│   └── stream_source.py          # Optional synthetic high-velocity event generator
└── README.md
```

## Batch pipeline (`batch_weather_pipeline`)

* **Schedule**: every two hours (`0 */2 * * *`).
* **Source data**: Weatherstack current-conditions API snapshots for the
  configured cities (`New York`, `San Francisco`, `London`, `Tokyo`). The DAG
  uses the a personal key provided by the weatherstack API. Each run pulls the latest payload
  per city, skips observations whose `localtime` matches the stored checkpoint,
  and appends any new JSON objects to `data/batch/raw/weather_history.jsonl` for
  lineage.
* **Tasks**:
  1. `extract_weather` invokes the Weatherstack API for each city and filters
     out readings the pipeline has already processed (tracked in
     `data/batch/state.json`).
  2. `transform_observations` computes per-city running counts plus average
     temperature and humidity for the new batch.
  3. `load_results` updates checkpoints, appends the raw payloads to the JSONL
     log, and writes timestamped batch summaries under
     `data/batch/processed/`.

The repository includes `data/batch/processed/weather_batch_sample.json` to show
the output structure alongside a seeded `data/batch/state.json` and
`data/batch/raw/weather_history.jsonl` representing a prior successful run.

### Real-world fit

Batch pipelines excel when upstream systems deliver sizable extracts on a
predictable cadence. Example use cases include:

* Nightly fleet telemetry rollups derived from an external monitoring API.
* Scheduled compliance checks that must snapshot external KPIs at precise
  intervals for auditing.
* Periodic enrichment jobs that join third-party reference data (like weather)
  onto internal fact tables on a slower cadence.

## Streaming-inspired pipeline (`streaming_pipeline`)

* **Schedule**: every minute (`*/1 * * * *`)
* **Source data**: Live current-conditions responses from
  [Weatherstack](https://weatherstack.com/) for the same list of cities. The DAG
  also falls back to the bundled demo key if `WEATHERSTACK_API_KEY` is not set.
  For offline review, the repository includes sample payloads in
  `data/stream/raw/weather_observations.jsonl` plus a processed example at
  `data/stream/processed/weather_sample_run.json`. These files show exactly what
  a DAG execution will write even if you cannot reach the external API.
* **Tasks**:
  1. `extract_weather` calls Weatherstack for each city, filtering out readings
     whose `localtime` matches the last stored value in `data/stream/state.json`
     so repeated runs only capture new observations.
  2. `transform_weather` computes per-city rolling averages and whole-fleet
     statistics (temperature and humidity) for the fresh readings.
  3. `load_weather` updates the per-city checkpoints, appends the raw API
     payloads to `data/stream/raw/weather_observations.jsonl`, and writes
     timestamped JSON summaries under `data/stream/processed/`. The committed
     `data/stream/state.json` mirrors the post-run checkpoint for the sample
     artifacts so you can inspect the full lifecycle.

### Real-world fit

Streaming-style orchestration shines when latency requirements are measured in
seconds or minutes. Potential applications include:

* Monitoring IoT sensors where each minute of readings must be validated and
  aggregated for alerting.
* Tracking user behavior events for rapid experimentation dashboards.
* Capturing operational logs from microservices so anomalies can be surfaced in
  near-real time.

## Simulating the streaming source

The `scripts/stream_source.py` utility continuously emits synthetic events to
`data/stream/raw/events.jsonl`. Example usage:

```bash
python scripts/stream_source.py --interval 2 --batch-size 10 --iterations 30
```

The script maintains its own checkpoint (`data/stream/source_state.json`) so
restarts continue with monotonically increasing `event_id` values.

## Running the DAGs locally

1. Copy the `dags/` directory into your Airflow `dags_folder`.
2. Provide the `WEATHERSTACK_API_KEY` in the environment or secrets backend for
   the Airflow workers/scheduler executing the streaming DAG.
3. Ensure the working directory is mounted or accessible so the DAGs can read
   and write within `data/`.
4. Trigger each DAG from the Airflow UI or CLI:

```bash
airflow dags trigger batch_weather_pipeline
airflow dags trigger streaming_pipeline
```

The processed outputs appear in the corresponding `data/*/processed/`
subdirectories.
