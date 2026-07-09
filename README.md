# Data Pipeline — Final Project

Multi-service data pipeline: Kafka streaming, Spark + Iceberg processing, Airflow orchestration.

## Prerequisites

- Docker & Docker Compose v2
- Git

No host Python setup is required to see the pipeline run end-to-end — every
job and producer runs inside its own container, and `STREAM_MODE=replay`
(the default) works without any API keys. A host `.venv` is only useful if
you want to run a producer standalone outside Docker for debugging.

## Local startup

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
unpause needed) and chains: `ensure_streaming_running` (starts the Kafka →
bronze streaming job if it isn't already running) + `ingest_yahoo_batch` in
parallel → `bronze_to_silver` → `silver_to_gold`. Trigger a run manually from
the UI, or:

```bash
docker exec airflow airflow dags trigger volumetric_pipeline
```

Tasks run via `docker exec` into `spark-iceberg` (the Airflow container has
the Docker CLI and the host's Docker socket mounted), so the `processing`
stack must already be up before triggering a run.

### 5. Stop

```bash
cd processing && docker compose down
cd ../streaming && docker compose down
cd ../orchestration && docker compose down
```

## Git workflow

- `main` — protected, no direct commits
- `develop` — integration branch
- `feature/*` — feature branches
