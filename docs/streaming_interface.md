# Streaming Interface (Student 1 → Student 2 handoff)

## Running it

```bash
docker network create data_pipeline_network 2>/dev/null || true
cp .env.example .env   # optional: real API keys for live mode
cd streaming
docker compose up -d --build
```

| Service                    | URL / notes                                          |
|------------------------------|-------------------------------------------------------|
| Kafka                       | localhost:9092 (host), kafka:29092 (docker network)    |
| Kafka UI                    | http://localhost:8090                                  |
| market-events-producer      | `docker logs -f market-events-producer`                |
| analyst-ratings-producer    | `docker logs -f analyst-ratings-producer`               |

Both producers run as their own containers (`streaming/Dockerfile`, built from
the repo root so it can reach `requirements.txt`), continuously in `replay`
mode by default — no API keys needed, no host Python setup. Set
`STREAM_MODE=live` in `.env` and fill in the Alpaca/Alpha Vantage keys for
real feeds, then `docker compose up -d --build` again.

To run a producer standalone on the host instead (e.g. for debugging):

```bash
python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python streaming/producers/market_events_producer.py
```

## Topics

### `market-events` → `bronze_market_events`

```json
{
  "event_id": "uuid",
  "ticker": "AAPL",
  "event_time": "2026-07-09T20:48:58.000730+00:00",
  "arrival_time": "2026-07-09T20:48:58.000730+00:00",
  "price": 141.4463,
  "volume": 99890800
}
```

`arrival_time == event_time` for this source — no simulated delay.

### `analyst-ratings` → `bronze_analyst_ratings`

```json
{
  "rating_id": "uuid",
  "ticker": "MSFT",
  "event_time": "2026-07-09T14:59:23+00:00",
  "arrival_time": "2026-07-10T18:48:04.834740+00:00",
  "rating_text": "Bullish",
  "source": "Alpha Vantage"
}
```

**`arrival_time` is genuinely 1-48h after `event_time`** (the producer holds the
message in memory and only publishes once the delay elapses) — this is the
late-arriving data the processing layer needs to watermark on `event_time`,
not on Kafka ingestion time. `rating_text` is one of: `Bullish`,
`Somewhat-Bullish`, `Neutral`, `Somewhat-Bearish`, `Bearish` (Alpha Vantage's
`overall_sentiment_label`, the closest free-tier proxy to an analyst rating —
map to a numeric `sentiment_score` in the silver layer).

## Notes for Student 2

- Consume both topics with `spark.readStream.format("kafka")`, `subscribe` per topic,
  `kafka.bootstrap.servers=kafka:29092` (internal listener, reachable from the
  `processing` compose stack once both are on `data_pipeline_network`).
- Watermark on `event_time`, not on Kafka's own timestamp — `arrival_time` in the
  payload is informational, the actual out-of-order-ness Spark needs to handle is
  the gap between `event_time` and when the record is actually consumable.
- `bronze_historical_prices` is not a streaming topic — it's produced by
  `processing/jobs/ingest_yahoo_to_bronze.py` directly from batch Yahoo data.
