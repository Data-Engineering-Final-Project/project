"""
Structured Streaming: Kafka topics market-events / analyst-ratings -> Iceberg bronze tables.

Run inside the spark-iceberg container (needs the Kafka connector, not bundled
in the base image):

    docker exec spark-iceberg spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
      /home/iceberg/jobs/bronze_ingest_streams.py

Each topic gets its own append-only streaming write, no watermarking here —
bronze is the raw, as-is landing zone. Watermarking and lateness handling
happen in bronze_to_silver.py.

Uses a 30s processing-time trigger, not the default "as fast as possible"
trigger. Without an explicit trigger, this commits (and creates a new
Iceberg snapshot + manifest + often a brand new tiny data file) on every
micro-batch, which for continuously-producing Kafka producers means a new
commit every few hundred milliseconds. Left running for the better part of
a day, that produced 20,000+ metadata files and 8,000+ tiny data files on
this table alone (measured directly against the warehouse's MinIO storage),
which is what was making every query slow, bloating iceberg-rest's memory
until it got OOM-killed, and generally making the whole stack flaky. A 30s
batching window still updates the dashboard's "live" feed well within its
own 5s poll / 10min freshness window, just without generating a commit
per tick.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"  # internal listener, same docker network
TRIGGER_INTERVAL = "30 seconds"

MARKET_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("ticker", StringType()),
    StructField("event_time", TimestampType()),
    StructField("arrival_time", TimestampType()),
    StructField("price", DoubleType()),
    StructField("volume", LongType()),
])

ANALYST_RATINGS_SCHEMA = StructType([
    StructField("rating_id", StringType()),
    StructField("ticker", StringType()),
    StructField("event_time", TimestampType()),
    StructField("arrival_time", TimestampType()),
    StructField("rating_text", StringType()),
    StructField("source", StringType()),
])


def read_topic(spark, topic, schema):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        # Without this, any offset mismatch between the checkpoint and what
        # Kafka currently has (broker restart losing unflushed segments,
        # topic auto-recreated after the broker was down long enough that
        # producers' first successful publish recreated it fresh, retention
        # rolling past the checkpointed offset) is a fatal
        # IllegalStateException that kills the whole query -- and since
        # Structured Streaming resumes from the same checkpoint on every
        # restart, it hits the exact same error again immediately, forever.
        # Observed this directly: a real Kafka outage (see
        # streaming/docker-compose.yml's restart policy fix) left this job
        # permanently crash-looping even after Kafka itself came back
        # healthy, because bronze_analyst_ratings' checkpoint still expected
        # an offset (181191) the broker no longer had (its topic had reset
        # to offset 49). For a bronze landing zone that's meant to be
        # resilient to exactly this kind of infra hiccup, skipping past the
        # gap and continuing is the right tradeoff -- silver/gold recompute
        # from whatever bronze actually has, same as any other late/missing
        # data.
        .option("failOnDataLoss", "false")
        .load()
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("ingestion_timestamp", current_timestamp())
    )


def main():
    spark = SparkSession.builder.appName("bronze_ingest_streams").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.bronze")

    market_events = read_topic(spark, "market-events", MARKET_EVENTS_SCHEMA)
    analyst_ratings = read_topic(spark, "analyst-ratings", ANALYST_RATINGS_SCHEMA)

    market_query = (
        market_events.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", "/home/iceberg/warehouse/_checkpoints/bronze_market_events")
        .toTable("demo.bronze.bronze_market_events")
    )

    ratings_query = (
        analyst_ratings.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", "/home/iceberg/warehouse/_checkpoints/bronze_analyst_ratings")
        .toTable("demo.bronze.bronze_analyst_ratings")
    )

    print("Streaming bronze_market_events and bronze_analyst_ratings. Ctrl-C to stop.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
