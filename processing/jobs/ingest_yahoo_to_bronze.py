from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_date, col

spark = (
    SparkSession.builder
    .appName("ingest_yahoo_to_bronze")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

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

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")

bronze_df.writeTo("local.bronze.bronze_historical_prices").createOrReplace()

print("Successfully created Iceberg table: local.bronze.bronze_historical_prices")
print(f"Rows written: {bronze_df.count()}")

spark.stop()