from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_date, col

spark = SparkSession.builder.appName("ingest_yahoo_to_bronze").getOrCreate()

input_path = "/home/iceberg/data/bronze/yahoo/historical_market_data.parquet"

df = spark.read.parquet(input_path)

bronze_df = (
    df
    .withColumn("event_date", to_date(col("Date")))
    .withColumn("ingestion_timestamp", current_timestamp())
    .select(
        "event_date",
        "Ticker",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "ingestion_timestamp"
    )
)

bronze_df = (
    bronze_df
    .withColumnRenamed("Ticker", "ticker")
    .withColumnRenamed("Open", "open")
    .withColumnRenamed("High", "high")
    .withColumnRenamed("Low", "low")
    .withColumnRenamed("Close", "close")
    .withColumnRenamed("Volume", "volume")
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.bronze")

bronze_df.writeTo("demo.bronze.bronze_historical_prices").createOrReplace()

print("Successfully created Iceberg table: demo.bronze.bronze_historical_prices")
print(f"Rows written: {bronze_df.count()}")

spark.stop()