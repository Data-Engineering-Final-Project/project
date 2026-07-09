"""
Bronze -> Silver: dedup, outlier flagging, sentiment mapping, and the
10-day rolling stats used downstream for volume_ratio/RSI/volatility.

Run inside the spark-iceberg container:

    docker exec spark-iceberg spark-submit /home/iceberg/jobs/bronze_to_silver.py

Design note on "batch", not continuous streaming: the project spec splits
work into Stream Processing (Kafka -> bronze, see bronze_ingest_streams.py)
and Batch Processing (bronze -> silver -> gold ETL, scheduled by Airflow).
This job is meant to be run periodically (e.g. every few minutes via a DAG),
not left running. Each run reads whatever bronze rows are currently visible
-- including analyst ratings that arrived late, up to 48h after their
event_time -- and MERGEs them into silver by natural key. That MERGE is
what makes reprocessing idempotent and correctly handles out-of-order
arrival: a late rating that shows up in bronze on a later run is simply
picked up and inserted then, no different from an on-time one. (An
alternative would be Spark's structured-streaming watermark API, but that
requires reading the bronze Iceberg tables as a streaming *source*, whose
internal offset tracking needs the table's own S3 FileIO for its
checkpoint -- incompatible with this image, which doesn't ship the
hadoop-aws connector needed for Spark's generic checkpoint to also use
s3://. Batch + MERGE sidesteps that entirely and is simpler to reason
about for a periodic ETL job anyway.)
"""

from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SENTIMENT_MAP = {
    "Bullish": 2,
    "Somewhat-Bullish": 1,
    "Neutral": 0,
    "Somewhat-Bearish": -1,
    "Bearish": -2,
}


def dq_report(df, name, key_cols):
    """Basic data quality checks: null keys and row counts, printed for the run log."""
    total = df.count()
    null_keys = df.filter(reduce(lambda a, b: a | b, [F.col(c).isNull() for c in key_cols])).count()
    print(f"[DQ] {name}: {total} rows, {null_keys} with null key columns {key_cols}")


def merge_into_silver(spark, df, table, key_col):
    df.createOrReplaceTempView("_staged")
    if spark.catalog.tableExists(table):
        cols = df.columns
        set_clause = ", ".join(f"t.{c} = s.{c}" for c in cols if c != key_col)
        insert_cols = ", ".join(cols)
        insert_vals = ", ".join(f"s.{c}" for c in cols)
        spark.sql(f"""
            MERGE INTO {table} t
            USING _staged s
            ON t.{key_col} = s.{key_col}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)
    else:
        df.writeTo(table).create()


def run_historical_stats_batch(spark):
    df = spark.table("demo.bronze.bronze_historical_prices")

    ticker_window = Window.partitionBy("ticker").orderBy("event_date")
    rolling_10d = ticker_window.rowsBetween(-9, 0)

    with_returns = (
        df.withColumn("prev_close", F.lag("close").over(ticker_window))
        .withColumn("daily_change", F.col("close") - F.col("prev_close"))
        .withColumn("gain", F.when(F.col("daily_change") > 0, F.col("daily_change")).otherwise(0.0))
        .withColumn("loss", F.when(F.col("daily_change") < 0, -F.col("daily_change")).otherwise(0.0))
    )

    rsi_window = ticker_window.rowsBetween(-13, 0)  # 14-period RSI
    stats = (
        with_returns
        .withColumn("avg_10d_volume", F.avg("volume").over(rolling_10d))
        .withColumn("volume_ratio", F.col("volume") / F.col("avg_10d_volume"))
        .withColumn("volatility_index", F.stddev("close").over(rolling_10d))
        .withColumn("avg_gain_14d", F.avg("gain").over(rsi_window))
        .withColumn("avg_loss_14d", F.avg("loss").over(rsi_window))
        .withColumn(
            "rsi_value",
            F.when(F.col("avg_loss_14d") == 0, 100.0)
            .otherwise(100.0 - (100.0 / (1.0 + (F.col("avg_gain_14d") / F.col("avg_loss_14d")))))
        )
        .select("event_date", "ticker", "close", "volume", "avg_10d_volume", "volume_ratio", "volatility_index", "rsi_value")
    )

    dq_report(stats, "silver_historical_stats", ["ticker", "event_date"])
    stats.writeTo("demo.silver.silver_historical_stats").createOrReplace()
    print("Wrote demo.silver.silver_historical_stats")


def run_market_prices_batch(spark):
    events = spark.table("demo.bronze.bronze_market_events")

    silver = (
        events
        .dropDuplicates(["event_id"])
        .withColumn("is_valid", (F.col("price") > 0) & (F.col("price") < 100000) & (F.col("volume") >= 0))
        .select("event_id", "ticker", "event_time", "price", "volume", "is_valid")
    )

    dq_report(silver, "silver_market_prices", ["event_id", "ticker"])
    merge_into_silver(spark, silver, "demo.silver.silver_market_prices", "event_id")
    print("Merged into demo.silver.silver_market_prices")


def run_analyst_ratings_batch(spark):
    ratings = spark.table("demo.bronze.bronze_analyst_ratings")

    sentiment_expr = F.create_map(*[F.lit(x) for pair in SENTIMENT_MAP.items() for x in pair])

    silver = (
        ratings
        .dropDuplicates(["rating_id"])
        .withColumn("sentiment_score", sentiment_expr[F.col("rating_text")])
        .select("rating_id", "ticker", "event_time", "arrival_time", "rating_text", "sentiment_score")
    )

    dq_report(silver, "silver_analyst_ratings", ["rating_id", "ticker"])
    merge_into_silver(spark, silver, "demo.silver.silver_analyst_ratings", "rating_id")
    print("Merged into demo.silver.silver_analyst_ratings")


def main():
    spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.silver")

    run_historical_stats_batch(spark)
    run_market_prices_batch(spark)
    run_analyst_ratings_batch(spark)

    spark.stop()


if __name__ == "__main__":
    main()
