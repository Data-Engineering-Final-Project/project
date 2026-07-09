# Adds the docker CLI to the official Airflow image so DAG tasks can
# `docker exec` into the spark-iceberg container (docker-outside-of-docker,
# same pattern as the quickstart guide linked in the project spec).
FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
