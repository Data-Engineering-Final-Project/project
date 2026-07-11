"""
Iceberg table maintenance: compact small files and expire old snapshots on
the two tables that get continuous streaming writes.

Why this needs to exist at all: without it, an append-only streaming write
creates a new snapshot (and typically a new small data file) on every
micro-batch commit. Run for the better part of a day with no trigger
throttling, that produced 20,000+ metadata files and 8,000+ tiny data files
on bronze_market_events alone -- measured directly against the warehouse's
MinIO storage while debugging why the whole stack kept getting slower and
crashing (iceberg-rest itself was getting OOM-killed trying to track all
that metadata). bronze_ingest_streams.py now throttles to a 30s trigger,
which reduces the rate this accumulates at, but doesn't stop it -- this job
is what actually keeps it bounded over time.

Run inside the spark-iceberg container, ideally on a schedule (see the
maintain_tables task in orchestration/dags/volumetric_pipeline_dag.py):
    docker exec spark-iceberg spark-submit /home/iceberg/jobs/maintain_tables.py
"""

from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

STREAMING_TABLES = ["bronze.bronze_market_events", "bronze.bronze_analyst_ratings"]
RETAIN_LAST_SNAPSHOTS = 10
COMPACT_TARGET_FILE_SIZE = 134217728  # 128MB
EXPIRE_OLDER_THAN_HOURS = 1


def main():
    spark = SparkSession.builder.appName("maintain_tables").getOrCreate()

    for table in STREAMING_TABLES:
        print(f"[maintain] Compacting {table}")
        result = spark.sql(f"""
            CALL demo.system.rewrite_data_files(
                table => '{table}',
                options => map('target-file-size-bytes', '{COMPACT_TARGET_FILE_SIZE}')
            )
        """).collect()
        print(f"[maintain] {table} compaction result: {result}")

        cutoff = (datetime.now(timezone.utc) - timedelta(hours=EXPIRE_OLDER_THAN_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[maintain] Expiring snapshots on {table} older than {cutoff} (keeping last {RETAIN_LAST_SNAPSHOTS})")
        result = spark.sql(f"""
            CALL demo.system.expire_snapshots(
                table => '{table}',
                older_than => TIMESTAMP '{cutoff}',
                retain_last => {RETAIN_LAST_SNAPSHOTS}
            )
        """).collect()
        print(f"[maintain] {table} expire_snapshots result: {result}")

    spark.stop()


if __name__ == "__main__":
    main()
