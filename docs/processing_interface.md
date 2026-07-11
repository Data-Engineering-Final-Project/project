# Processing Interface

Contract between the processing layer (`/processing`) and orchestration
(`/orchestration`): job dependency order, spark-submit commands, and final table names.

All jobs run inside the `spark-iceberg` container against the `demo` Iceberg
REST catalog (backed by MinIO). Namespaces: `demo.bronze`, `demo.silver`, `demo.gold`.

## Job dependency order

```
download_yahoo.py          (batch, run once — pulls raw data from Yahoo Finance)
        │
        ▼
ingest_yahoo_to_bronze.py  (batch, run once / whenever new history is needed)
bronze_ingest_streams.py   (streaming, long-running — Kafka -> bronze)
        │
        ▼
bronze_to_silver.py        (batch, schedule periodically, e.g. every 5-15 min)
        │
        ├──────────────┐
        ▼              ▼
silver_to_gold.py  maintain_tables.py  (batch, same schedule — compacts +
                                         expires snapshots on the two
                                         streaming bronze tables)
```

`bronze_ingest_streams.py` is the one genuinely continuous job — everything else
is a batch step meant to be triggered on a schedule (Airflow `SparkSubmitOperator`
or equivalent), consistent with the project spec's split between "Stream
Processing" (Kafka → bronze) and "Batch Processing" (bronze → silver → gold ETL).

`maintain_tables.py` runs alongside `silver_to_gold.py` (both depend only on
`bronze_to_silver.py` having read the bronze tables, not on each other) — see
"Notable infra fixes" below for why it exists.

## spark-submit commands

```bash
# One-time: download raw Yahoo Finance data (30 tickers, 5y daily) into data/bronze/yahoo/
docker exec spark-iceberg python3 /home/iceberg/jobs/download_yahoo.py

# One-time / periodic batch ingestion of Yahoo historical data into Iceberg
docker exec spark-iceberg spark-submit /home/iceberg/jobs/ingest_yahoo_to_bronze.py

# Long-running: Kafka -> Iceberg bronze (needs the Kafka connector, not bundled)
docker exec spark-iceberg spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  /home/iceberg/jobs/bronze_ingest_streams.py

# Scheduled batch: bronze -> silver
docker exec spark-iceberg spark-submit /home/iceberg/jobs/bronze_to_silver.py

# Scheduled batch: silver -> gold (must run after bronze_to_silver)
docker exec spark-iceberg spark-submit /home/iceberg/jobs/silver_to_gold.py
```

For Airflow's `SparkSubmitOperator`, point at the same `spark-iceberg` container
(or run spark-submit directly if Airflow shares the `data_pipeline_network`) with
the same `--packages` flag on the streaming job only.

## Final table names

| Layer  | Table                          | Written by                    |
|--------|----------------------------------|--------------------------------|
| bronze | `bronze_historical_prices`      | `ingest_yahoo_to_bronze.py`    |
| bronze | `bronze_market_events`          | `bronze_ingest_streams.py`     |
| bronze | `bronze_analyst_ratings`        | `bronze_ingest_streams.py`     |
| silver | `silver_historical_stats`       | `bronze_to_silver.py`          |
| silver | `silver_market_prices`          | `bronze_to_silver.py`          |
| silver | `silver_analyst_ratings`        | `bronze_to_silver.py`          |
| gold   | `dim_stocks`                    | `seed_dim_stocks.py` (one-time)|
| gold   | `dim_analyst_coverage` (SCD2)   | `silver_to_gold.py`            |
| gold   | `fact_volumetric_anomalies`     | `silver_to_gold.py`            |

## Dashboard

`processing/dashboard/` — FastAPI + a persistent SparkSession, backing the
four presentation panels plus a live ticker strip, served at
**http://localhost:8000**. It's a service in `processing/docker-compose.yml`
(`dashboard-api`), starts automatically with the rest of the processing stack.

```bash
# one-time: seed the sector reference table and train the model
docker exec spark-iceberg python3 /home/iceberg/jobs/seed_dim_stocks.py
docker exec spark-iceberg python3 /home/iceberg/jobs/train_model.py
```

Endpoints: `/api/sector-heatmap`, `/api/top-spikes`, `/api/predict/{ticker}`,
`/api/late-arrivals`, `/api/live-feed`.

Query any of them via `spark-sql`, e.g.:

```bash
docker exec spark-iceberg spark-sql -e "SELECT * FROM demo.gold.fact_volumetric_anomalies LIMIT 10;"
```

## Notable infra fixes (if you're debugging locally)

- `processing/docker-compose.yml`'s network must be `external: true` — it shares
  `data_pipeline_network` with the `streaming` stack, created once via
  `docker network create data_pipeline_network`.
- MinIO needs `s3.path-style-access=true` on the Iceberg catalog client side
  (see `processing/conf/spark-defaults.conf`) — without it, the S3 client
  addresses buckets as `warehouse.minio:9000` (virtual-hosted style), which
  MinIO can't resolve, and every write fails with `NoSuchBucketException`
  even though the bucket exists.
- `download_yahoo.py` needs `yfinance>=1.5.1` — 0.2.51 fails outright against
  Yahoo's current API (session/crumb bootstrap issue). `processing/Dockerfile`
  installs it into the `spark-iceberg` image so this runs without a host
  Python setup. Its output path must be absolute (`/home/iceberg/data/...`),
  not relative — relative paths resolve against the container's default
  working directory, not the mounted `../data` volume.
- `bronze_to_silver.py` and `silver_to_gold.py` are batch jobs, not Structured
  Streaming readers off the bronze/silver Iceberg tables — that path hits a
  real incompatibility (Iceberg's streaming source offset tracking needs the
  table's S3 FileIO, Spark's generic checkpoint commit log needs a
  Hadoop-recognized filesystem for the same path, and this image doesn't ship
  `hadoop-aws` to reconcile the two). MERGE INTO by natural key gets the same
  idempotent, late-data-safe result without that dependency.
- **Memory — the big one.** The base image's `entrypoint.sh` unconditionally
  starts a full standalone Spark master/worker/history-server/thrift-server
  before running anything else. Nothing in this project uses them —
  `spark-defaults.conf` never sets `spark.master` to
  `spark://spark-iceberg:7077`, every job runs in local mode via
  `docker exec ... spark-submit`. That unused baseline was costing ~1.1GB in
  `spark-iceberg` and ~1.4GB in `dashboard-api`, continuously, for nothing.
  Under Docker Desktop's default ~7.75GB, running the full stack (Kafka, 2
  producers, Airflow x2, MinIO, both Spark containers) for a long stretch
  pushed total usage past 85%, and the OOM killer started picking off
  containers — including Kafka itself once, and the streaming consumer
  repeatedly (it runs inside `spark-iceberg`, so it was the first casualty
  of that container's own bloat, not a bug in the consumer or in how it's
  detached). Both services now override `entrypoint` to skip
  `entrypoint.sh` — `spark-iceberg` goes straight to `/bin/notebook` (keeps
  Jupyter, which is genuinely used, skips the rest), `dashboard-api` skips
  straight to the API process (needs neither). Verified: `spark-iceberg`
  dropped from 1.148GB to 78MB, `dashboard-api` from ~1.4GB to ~300MB, total
  stack from ~6.75GB to ~4GB, and the streaming consumer has stayed alive
  continuously since with batch offsets climbing normally.
- Overriding `command` alone isn't enough to bypass `entrypoint.sh` — that
  script only does `eval "$1"`, so a list-form `command: ["python3", "path"]`
  silently drops everything past the first element (runs a bare REPL that
  exits immediately on EOF, clean exit 0, no error). Must be either one
  string (`command: ["python3 path"]`) or, better, override `entrypoint`
  directly to skip the script's own startup logic entirely.
