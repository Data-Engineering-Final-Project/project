"""
Schedules the batch ETL chain (bronze -> silver -> gold) and makes sure the
Kafka -> bronze streaming job is alive. Talks to the processing container
via `docker exec` (docker-outside-of-docker, matching the quickstart guide
referenced in the project spec) rather than a Spark client connection --
simpler to reason about and doesn't need Spark libs inside the Airflow image.

See docs/processing_interface.md for the job dependency order and exact
spark-submit commands this DAG wraps.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_CONTAINER = "spark-iceberg"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"


def alert_on_failure(context):
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    exec_date = context["execution_date"]
    logging.error(
        "[ALERT] Task '%s' in DAG '%s' failed at %s. Check Airflow logs for the "
        "full stack trace. (Swap this callback for an EmailOperator/Slack "
        "webhook call to get real notifications.)",
        task_id, dag_id, exec_date,
    )


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="volumetric_pipeline",
    description="Bronze -> Silver -> Gold ETL for the volumetric anomaly detection pipeline",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # runs on its own schedule right away, no manual unpause needed
    tags=["volumetric-anomaly-detection"],
) as dag:

    ensure_streaming_running = BashOperator(
        task_id="ensure_streaming_running",
        bash_command=(
            # `docker exec -d` (run from this Airflow container, which has the
            # docker CLI) truly detaches the process. `nohup cmd &` inside a
            # single `docker exec spark-iceberg bash -c "..."` looks like it
            # works but the process still dies the moment that exec session
            # ends -- verified this the hard way: streaming silently stopped
            # after the first restart even though this task kept reporting
            # success, because it only checked "did the start command run",
            # not "is the process still alive 15 minutes later".
            f"docker exec {SPARK_CONTAINER} pgrep -f bronze_ingest_streams.py > /dev/null || "
            f"docker exec -d {SPARK_CONTAINER} bash -c "
            f"'spark-submit --packages {KAFKA_PACKAGE} "
            f"/home/iceberg/jobs/bronze_ingest_streams.py "
            f"> /home/iceberg/warehouse/bronze_ingest_streams.log 2>&1'"
        ),
    )

    download_yahoo_data = BashOperator(
        task_id="download_yahoo_data",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} bash -c "
            f"'test -f /home/iceberg/data/bronze/yahoo/historical_market_data.parquet || "
            f"python3 /home/iceberg/jobs/download_yahoo.py'"
        ),
    )

    ingest_yahoo_batch = BashOperator(
        task_id="ingest_yahoo_batch",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} "
            f"spark-submit /home/iceberg/jobs/ingest_yahoo_to_bronze.py"
        ),
    )

    download_yahoo_data >> ingest_yahoo_batch

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} "
            f"spark-submit /home/iceberg/jobs/bronze_to_silver.py"
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} "
            f"spark-submit /home/iceberg/jobs/silver_to_gold.py"
        ),
    )

    # Compacts small files and expires old snapshots on the two streaming
    # bronze tables. Runs every cycle (every 15 minutes) rather than on a
    # separate, longer schedule -- the job itself is cheap (a few seconds on
    # this data volume) and running it often is what keeps the table's file
    # and snapshot count bounded instead of letting it grow between runs.
    # See processing/jobs/maintain_tables.py for why this exists: without it,
    # unthrottled streaming commits accumulated 20,000+ metadata files in
    # under a day and OOM-killed the Iceberg REST catalog.
    maintain_tables = BashOperator(
        task_id="maintain_tables",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} "
            f"spark-submit /home/iceberg/jobs/maintain_tables.py"
        ),
    )

    [ensure_streaming_running, ingest_yahoo_batch] >> bronze_to_silver >> [silver_to_gold, maintain_tables]
