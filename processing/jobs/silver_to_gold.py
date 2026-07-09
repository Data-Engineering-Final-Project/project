"""
Silver -> Gold: SCD Type 2 analyst coverage dimension, and the anomaly fact
table used for the dashboard and the ML feature store.

Run inside the spark-iceberg container:

    docker exec spark-iceberg spark-submit /home/iceberg/jobs/silver_to_gold.py

dim_analyst_coverage (SCD2): for each ticker, "coverage" is the latest
sentiment_score from silver_analyst_ratings. When a ticker's sentiment
changes, the previously-current row is closed out (effective_to set,
is_current=false) and a new current row is inserted -- the standard
two-step Iceberg MERGE INTO pattern for SCD2, so history is preserved
and re-running is idempotent (unchanged tickers are simply skipped).

fact_volumetric_anomalies: an "anomaly" is a day where volume_ratio > 2x
the 10-day average (the project's own business question threshold).
target_label answers that business question directly: did the price move
+-5% over the following 5 trading days? That's only knowable from data
we already have (silver_historical_stats), so the fact table is built
from there, not from live ticks -- a fresh intraday spike has no
5-day-forward outcome yet by definition. Each anomaly is joined against
dim_analyst_coverage as-of its event_date (a temporal join on the SCD2
effective_from/effective_to range) to attach "the exact active analyst
sentiment" at that point in time.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

VOLUME_RATIO_ANOMALY_THRESHOLD = 2.0
FORWARD_RETURN_LABEL_THRESHOLD = 0.05  # +-5%
FORWARD_TRADING_DAYS = 5


def run_scd2_dim_analyst_coverage(spark):
    table = "demo.gold.dim_analyst_coverage"

    latest_ratings = (
        spark.table("demo.silver.silver_analyst_ratings")
        .withColumn("rn", F.row_number().over(Window.partitionBy("ticker").orderBy(F.col("event_time").desc())))
        .filter("rn = 1")
        .select("ticker", "sentiment_score", F.col("event_time").alias("change_time"))
    )

    if not spark.catalog.tableExists(table):
        init = (
            latest_ratings
            .withColumn("analyst_key", F.concat_ws("-", F.col("ticker"), F.col("change_time").cast("string")))
            .withColumn("effective_from", F.col("change_time"))
            .withColumn("effective_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .select("analyst_key", "ticker", "sentiment_score", "effective_from", "effective_to", "is_current")
        )
        init.writeTo(table).create()
        print(f"[SCD2] Bootstrapped {table} with {init.count()} current rows")
        return

    latest_ratings.createOrReplaceTempView("_latest_ratings")

    # Step 1: close out current rows whose sentiment actually changed
    spark.sql(f"""
        MERGE INTO {table} t
        USING _latest_ratings s
        ON t.ticker = s.ticker AND t.is_current = true
        WHEN MATCHED AND t.sentiment_score != s.sentiment_score THEN
          UPDATE SET t.effective_to = s.change_time, t.is_current = false
    """)

    # Step 2: insert a new current row for every ticker that no longer has one
    # (i.e. it was just closed out above, or it's a ticker we've never seen)
    spark.sql(f"""
        MERGE INTO {table} t
        USING _latest_ratings s
        ON t.ticker = s.ticker AND t.is_current = true
        WHEN NOT MATCHED THEN
          INSERT (analyst_key, ticker, sentiment_score, effective_from, effective_to, is_current)
          VALUES (concat_ws('-', s.ticker, cast(s.change_time as string)), s.ticker, s.sentiment_score, s.change_time, NULL, true)
    """)
    print(f"[SCD2] Merged latest sentiment into {table}")


def run_fact_volumetric_anomalies(spark):
    stats = spark.table("demo.silver.silver_historical_stats")
    ticker_window = Window.partitionBy("ticker").orderBy("event_date")

    labeled = (
        stats
        .withColumn("future_close", F.lead("close", FORWARD_TRADING_DAYS).over(ticker_window))
        .withColumn("forward_return", (F.col("future_close") - F.col("close")) / F.col("close"))
        .withColumn("target_label", F.abs(F.col("forward_return")) >= FORWARD_RETURN_LABEL_THRESHOLD)
        .filter(F.col("volume_ratio") > VOLUME_RATIO_ANOMALY_THRESHOLD)
        .filter(F.col("future_close").isNotNull())  # drop the trailing rows with no 5-day-forward outcome yet
    )

    coverage = spark.table("demo.gold.dim_analyst_coverage") if spark.catalog.tableExists("demo.gold.dim_analyst_coverage") else None

    if coverage is not None:
        joined = labeled.alias("a").join(
            coverage.alias("c"),
            (F.col("a.ticker") == F.col("c.ticker"))
            & (F.col("c.effective_from") <= F.col("a.event_date"))
            & (F.col("c.effective_to").isNull() | (F.col("a.event_date") < F.col("c.effective_to"))),
            "left",
        ).select(
            "a.ticker", "a.event_date", "a.volume_ratio", "a.rsi_value", "a.volatility_index",
            F.col("c.sentiment_score").alias("sentiment_score"), "a.target_label",
        )
    else:
        joined = labeled.select(
            "ticker", "event_date", "volume_ratio", "rsi_value", "volatility_index",
            F.lit(None).cast("int").alias("sentiment_score"), "target_label",
        )

    fact = (
        joined
        .withColumn("anomaly_id", F.concat_ws("-", F.col("ticker"), F.col("event_date").cast("string")))
        .withColumn("event_time", F.col("event_date").cast("timestamp"))
        .select("anomaly_id", "ticker", "event_time", "volume_ratio", "rsi_value", "volatility_index", "sentiment_score", "target_label")
    )

    total = fact.count()
    positive = fact.filter("target_label = true").count()
    print(f"[DQ] fact_volumetric_anomalies: {total} anomalies, {positive} labeled positive (>= 5% move within {FORWARD_TRADING_DAYS} days)")

    fact.writeTo("demo.gold.fact_volumetric_anomalies").createOrReplace()
    print("Wrote demo.gold.fact_volumetric_anomalies")


def main():
    spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.gold")

    run_scd2_dim_analyst_coverage(spark)
    run_fact_volumetric_anomalies(spark)

    spark.stop()


if __name__ == "__main__":
    main()
