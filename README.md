# Data Pipeline — Final Project

Multi-service data pipeline: Kafka streaming, Spark + Iceberg processing, Airflow orchestration.

## Prerequisites

- Docker & Docker Compose v2
- Git

## Local startup

### 1. Create shared network (once)

```bash
docker network create data_pipeline_network 2>/dev/null || true
```

### 2. Start processing stack (MinIO + Spark + Iceberg)

```bash
cd processing
docker compose up -d
```

| Service           | URL                          |
|-------------------|------------------------------|
| MinIO API         | http://localhost:9000        |
| MinIO Console     | http://localhost:9001        |
| Spark UI          | http://localhost:8080        |
| Jupyter Notebook  | http://localhost:8888        |
| Iceberg REST      | http://localhost:8181        |

MinIO credentials: `admin` / `supersecret` — bucket `warehouse` is created automatically.

Run Spark jobs inside the processing container (see
[docs/processing_interface.md](docs/processing_interface.md) for the full job
dependency order, exact spark-submit commands, and the final table names):

```bash
docker exec spark-iceberg spark-submit /home/iceberg/jobs/ingest_yahoo_to_bronze.py
docker exec spark-iceberg spark-submit /home/iceberg/jobs/bronze_to_silver.py
docker exec spark-iceberg spark-submit /home/iceberg/jobs/silver_to_gold.py

# interactive
docker exec -it spark-iceberg pyspark
docker exec -it spark-iceberg spark-sql
```

### 3. Start the streaming stack (Kafka)

```bash
cd ../streaming
docker compose up -d
```

| Service   | URL                                                  |
|-----------|-------------------------------------------------------|
| Kafka     | localhost:9092 (host) / kafka:29092 (docker network)   |
| Kafka UI  | http://localhost:8090                                  |

Copy `.env.example` to `.env` and fill in Alpaca/Alpha Vantage keys, then run the
producers from the repo root (see [docs/streaming_interface.md](docs/streaming_interface.md)
for the full topic/schema contract):

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python streaming/producers/market_events_producer.py
python streaming/producers/analyst_ratings_producer.py
```

Set `STREAM_MODE=replay` in `.env` to run both producers without live market hours
or API keys.

### 4. Start orchestration (when ready)

```bash
cd ../orchestration && docker compose up -d
```

### 5. Stop

```bash
cd processing && docker compose down
cd ../streaming && docker compose down
```

## Git workflow

- `main` — protected, no direct commits
- `develop` — integration branch
- `feature/*` — feature branches
