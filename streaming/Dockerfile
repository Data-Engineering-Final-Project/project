# Build context is the repo root (see streaming/docker-compose.yml) so this
# can reach both requirements.txt and streaming/producers/.
FROM python:3.11-slim

# Unbuffered stdout so `docker logs -f` shows progress in real time instead
# of waiting for Python's block-buffering to flush.
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streaming/producers/ streaming/producers/
