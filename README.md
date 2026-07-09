# Volumetric Anomaly Detection — Data Pipeline Final Project

An end-to-end data engineering pipeline for detecting "smart money" volume
spikes in equity markets: streaming trade ticks (Kafka), late-arriving
analyst sentiment (up to 48h delay, watermark-safe), and batch historical
baselines (Yahoo Finance), landed through a bronze/silver/gold medallion
architecture on Apache Iceberg + MinIO, scheduled end-to-end by Airflow.

Full write-up of the business question, data sources, and data model is in
[docs/architecture.md](docs/architecture.md) (includes a diagram) and
[docs/data_model.md](docs/data_model.md).

## What's in this repo

| Directory         | Contents                                                        | Docs |
|--------------------|-------------------------------------------------------------------|------|
| `streaming/`       | Kafka (KRaft) + two producers (market ticks, analyst sentiment)   | [docs/streaming_interface.md](docs/streaming_interface.md) |
| `processing/`      | MinIO + Spark + Iceberg REST catalog, all ETL jobs                | [docs/processing_interface.md](docs/processing_interface.md) |
| `orchestration/`   | Airflow, schedules the batch chain + supervises the streaming job | this file |
| `docs/`            | Architecture, data model, component interfaces                    | — |

## Prerequisites

- Docker & Docker Compose v2
- Git

No host Python setup is required to see the pipeline run end-to-end — every
job and producer runs inside its own container, and `STREAM_MODE=replay`
(the default) works without any API keys. A host `.venv` is only useful if
you want to run a producer standalone outside Docker for debugging.

## Local startup

Run these four steps in order. Each stack must be up before the next one
that depends on it (orchestration triggers jobs inside the processing
container via `docker exec`, so processing must already be running).

### 1. Create shared network (once)

```bash
docker network create data_pipeline_network 2>/dev/null || true
```

### 2. Start processing stack (MinIO + Spark + Iceberg)

```bash
cd processing
docker compose up -d --build
```

| Service           | URL                          |
|-------------------|------------------------------|
| MinIO API         | http://localhost:9000        |
| MinIO Console     | http://localhost:9001        |
| Spark UI          | http://localhost:8080        |
| Jupyter Notebook  | http://localhost:8888        |
| Iceberg REST      | http://localhost:8181        |

MinIO credentials: `admin` / `supersecret` — bucket `warehouse` is created automatically.

The batch jobs (download → ingest → silver → gold) run automatically once
Airflow is up (step 4 below). To run them by hand instead — everything here
runs entirely inside the container, no host Python needed (see
[docs/processing_interface.md](docs/processing_interface.md) for the full job
dependency order and final table names):

```bash
docker exec spark-iceberg python3 /home/iceberg/jobs/download_yahoo.py
docker exec spark-iceberg spark-submit /home/iceberg/jobs/ingest_yahoo_to_bronze.py
docker exec spark-iceberg spark-submit /home/iceberg/jobs/bronze_to_silver.py
docker exec spark-iceberg spark-submit /home/iceberg/jobs/silver_to_gold.py

# interactive
docker exec -it spark-iceberg pyspark
docker exec -it spark-iceberg spark-sql
```

Check the results at any time:

```bash
docker exec spark-iceberg spark-sql -e "SELECT * FROM demo.gold.fact_volumetric_anomalies LIMIT 10;"
```

### 3. Start the streaming stack (Kafka + producers)

```bash
cp .env.example .env   # optional: fill in Alpaca/Alpha Vantage keys for live mode
cd ../streaming
docker compose up -d --build
```

| Service   | URL                                                  |
|-----------|-------------------------------------------------------|
| Kafka     | localhost:9092 (host) / kafka:29092 (docker network)   |
| Kafka UI  | http://localhost:8090                                  |

This starts Kafka plus both producers (`market-events-producer`,
`analyst-ratings-producer`), continuously publishing in `replay` mode by
default — see [docs/streaming_interface.md](docs/streaming_interface.md) for
the topic/schema contract. Watch them work:

```bash
docker logs -f market-events-producer
docker logs -f analyst-ratings-producer
```

Set `STREAM_MODE=live` in `.env` (and fill in the API keys) for real
Alpaca/Alpha Vantage feeds, then `docker compose up -d --build` again to
pick up the change.

### 4. Start orchestration (Airflow)

```bash
cd ../orchestration
docker compose up -d --build
```

| Service     | URL                            |
|-------------|----------------------------------|
| Airflow UI  | http://localhost:8082 (admin / admin) |

The `volumetric_pipeline` DAG runs every 15 minutes automatically (no manual
unpause needed) and chains: `download_yahoo_data` + `ensure_streaming_running`
(starts the Kafka → bronze streaming job if it isn't already running) →
`ingest_yahoo_batch` → `bronze_to_silver` → `silver_to_gold`. Every task has
retries and an on-failure alert callback. Trigger a run manually from the
UI, or:

```bash
docker exec airflow airflow dags trigger volumetric_pipeline
```

### 5. Stop

```bash
cd processing && docker compose down
cd ../streaming && docker compose down
cd ../orchestration && docker compose down
```

## Data sources

| Source        | Kind              | How it's fetched                                                        |
|-----------------|---------------------|-----------------------------------------------------------------------------|
| Alpaca          | Streaming (real-time) | `market_events_producer.py` — IEX trade feed via `alpaca-py`               |
| Alpha Vantage   | Streaming, deliberately late (1-48h) | `analyst_ratings_producer.py` — `NEWS_SENTIMENT` endpoint (no dedicated analyst-ratings endpoint on the free tier) |
| Yahoo Finance   | Batch (5y daily history, 30 tickers) | `download_yahoo.py` — `yfinance`                                          |

## Data quality

Every batch job in `processing/jobs/` prints a `[DQ]` summary (row counts,
null-key counts) on each run — see the job source for the exact checks. Bad
market ticks are flagged via an `is_valid` column in `silver_market_prices`
rather than silently dropped.

## Demo & presentation

Recordings live in the shared Drive folder:
https://drive.google.com/drive/folders/1FfadtXh8PLl0mxetc5rG-RXbG60SKB1r?usp=sharing

## Git workflow

- `main` — protected, no direct commits
- `develop` — integration branch
- `feature/*` — feature branches
