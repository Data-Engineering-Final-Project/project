# Architecture

Medallion architecture (bronze/silver/gold) on Iceberg + MinIO, fed by one
streaming source (Kafka), one deliberately-late streaming source (Kafka), and
one batch source (Yahoo Finance). Airflow schedules the batch ETL chain and
keeps the streaming ingestion job alive.

```mermaid
flowchart TB
    subgraph Sources
        Alpaca[Alpaca API<br/>live trades]
        AlphaVantage[Alpha Vantage API<br/>news sentiment, 1-48h late]
        Yahoo[Yahoo Finance<br/>yfinance, batch]
    end

    subgraph Streaming["streaming/ (Student 1)"]
        Kafka[(Kafka<br/>KRaft mode)]
        MEP[market_events_producer.py]
        ARP[analyst_ratings_producer.py]
    end

    subgraph Processing["processing/ (Student 2)"]
        direction TB
        Bronze[(Bronze<br/>bronze_market_events<br/>bronze_analyst_ratings<br/>bronze_historical_prices)]
        Silver[(Silver<br/>silver_market_prices<br/>silver_analyst_ratings<br/>silver_historical_stats)]
        Gold[(Gold<br/>dim_analyst_coverage SCD2<br/>fact_volumetric_anomalies)]
        MinIO[(MinIO S3<br/>Iceberg warehouse)]
    end

    subgraph Orchestration["orchestration/ (Student 3)"]
        Airflow[Airflow DAG<br/>volumetric_pipeline]
    end

    Alpaca --> MEP --> Kafka
    AlphaVantage --> ARP --> Kafka
    Yahoo --> Yahoo_job[ingest_yahoo_to_bronze.py]

    Kafka -->|bronze_ingest_streams.py| Bronze
    Yahoo_job --> Bronze
    Bronze -->|bronze_to_silver.py<br/>dedup, outlier flag, sentiment map| Silver
    Silver -->|silver_to_gold.py<br/>MERGE INTO SCD2, anomaly labeling| Gold

    Bronze -.-> MinIO
    Silver -.-> MinIO
    Gold -.-> MinIO

    Airflow -->|docker exec spark-submit| Yahoo_job
    Airflow -->|docker exec spark-submit| Bronze
    Airflow -->|docker exec spark-submit| Silver
    Airflow -->|ensures running| Kafka
```

## Layer responsibilities

- **Bronze**: raw, as-landed data with `ingestion_timestamp` appended. No
  cleaning, no joins — the source of truth in its original shape.
- **Silver**: deduped, outlier-flagged, sentiment mapped to a numeric score,
  and the 10-day rolling stats (volume ratio, volatility, RSI) computed once
  here so gold doesn't recompute them per query.
- **Gold**: `dim_analyst_coverage` is a proper SCD Type 2 dimension (history
  of sentiment changes per ticker, not just the latest value). `fact_volumetric_anomalies`
  answers the project's business question directly — every row is a day where
  volume exceeded 2x the 10-day average, joined to the analyst sentiment that
  was actually active on that date, labeled with whether price moved ±5% over
  the following 5 trading days.

## Why batch, not continuous streaming, for bronze→silver→gold

Only `bronze_ingest_streams.py` (Kafka → bronze) is a genuinely continuous
Structured Streaming job. `bronze_to_silver.py` and `silver_to_gold.py` are
batch jobs scheduled by Airflow every 15 minutes. See the design note at the
top of [`processing/jobs/bronze_to_silver.py`](../processing/jobs/bronze_to_silver.py)
for why: reading an Iceberg table as a streaming *source* ties the
checkpoint to the table's S3 FileIO in a way this image's dependencies can't
reconcile with Spark's generic checkpoint mechanism. MERGE INTO by natural
key gets the same idempotent, late-data-safe result without that dependency,
and matches the project spec's own split between "Stream Processing" (Kafka
ingestion) and "Batch Processing" (the ETL chain).

## Late-arrival handling

`analyst_ratings_producer.py` holds each rating for a genuine 1-48h delay
before publishing (not just a stamped field — see
[docs/streaming_interface.md](streaming_interface.md)). Because
`bronze_to_silver.py` and `silver_to_gold.py` are idempotent (MERGE INTO by
key), a rating that only becomes visible in bronze on a later scheduled run
is simply picked up then — no data is lost or double-counted regardless of
how late it arrives.
