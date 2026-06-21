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

Run Spark jobs inside the processing container:

```bash
docker exec -it spark-iceberg pyspark
docker exec -it spark-iceberg spark-sql
```

### 3. Start other components (when ready)

```bash
cd ../streaming && docker compose up -d
cd ../orchestration && docker compose up -d
```

### 4. Stop

```bash
cd processing && docker compose down
```

## Git workflow

- `main` — protected, no direct commits
- `develop` — integration branch
- `feature/*` — feature branches
