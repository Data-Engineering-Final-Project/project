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
        ▼
silver_to_gold.py          (batch, schedule after bronze_to_silver completes)
```

`bronze_ingest_streams.py` is the one genuinely continuous job — everything else
is a batch step meant to be triggered on a schedule (Airflow `SparkSubmitOperator`
or equivalent), consistent with the project spec's split between "Stream
Processing" (Kafka → bronze) and "Batch Processing" (bronze → silver → gold ETL).

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
| gold   | `dim_analyst_coverage` (SCD2)   | `silver_to_gold.py`            |
| gold   | `fact_volumetric_anomalies`     | `silver_to_gold.py`            |

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
