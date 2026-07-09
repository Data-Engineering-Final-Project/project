"""
Publishes live (or replayed) trade ticks to the Kafka topic "market-events".

Message schema matches bronze_market_events (see docs/data_model.md):
    event_id, ticker, event_time, arrival_time, price, volume

For this source arrival_time == event_time (no simulated delay) — the
late-arrival scenario in this project comes from analyst_ratings_producer.py.

Modes (set STREAM_MODE in .env):
    live    - subscribes to Alpaca's IEX trade feed (needs market hours + API keys)
    replay  - replays data/bronze/yahoo/historical_market_data.csv as synthetic
              ticks at accelerated speed, so the demo works any time of day
"""

import csv
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "market-events"
STREAM_MODE = os.environ.get("STREAM_MODE", "live")

TICKERS = [
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN",
    "META", "GOOG", "AMD", "NFLX", "JPM",
    "BAC", "XOM", "UNH", "JNJ", "COST",
    "WMT", "DIS", "PEP", "KO", "INTC",
    "CRM", "ORCL", "CSCO", "V", "MA",
    "PFE", "MRK", "NKE", "ADBE", "AVGO",
]

HISTORICAL_CSV = Path(__file__).resolve().parents[2] / "data" / "bronze" / "yahoo" / "historical_market_data.csv"


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for {msg.key()}: {err}")


def publish_event(producer: Producer, ticker: str, price: float, volume: int, event_time: datetime) -> None:
    event = {
        "event_id": str(uuid.uuid4()),
        "ticker": ticker,
        "event_time": event_time.isoformat(),
        "arrival_time": event_time.isoformat(),
        "price": round(float(price), 4),
        "volume": int(volume),
    }
    producer.produce(TOPIC, key=ticker, value=json.dumps(event), callback=delivery_report)
    producer.poll(0)


def run_replay(producer: Producer) -> None:
    if not HISTORICAL_CSV.exists():
        raise FileNotFoundError(
            f"{HISTORICAL_CSV} not found. Run processing/jobs/download_yahoo.py first, "
            "or switch STREAM_MODE=live once Alpaca keys are set."
        )

    print(f"Replay mode: streaming synthetic ticks from {HISTORICAL_CSV}")
    with open(HISTORICAL_CSV, newline="") as f:
        rows = list(csv.DictReader(f))
    random.shuffle(rows)

    for row in rows:
        ticker = row.get("Ticker")
        if ticker not in TICKERS:
            continue
        try:
            close = float(row["Close"])
            day_volume = int(float(row["Volume"]))
        except (KeyError, ValueError):
            continue

        # Simulate a handful of intraday ticks around the day's close/volume
        # instead of replaying one event per day.
        for _ in range(random.randint(1, 3)):
            jitter = random.uniform(-0.01, 0.01)
            tick_price = close * (1 + jitter)
            tick_volume = max(1, int(day_volume * random.uniform(0.001, 0.01)))
            publish_event(producer, ticker, tick_price, tick_volume, datetime.now(timezone.utc))
            time.sleep(0.2)

    producer.flush()
    print("Replay complete.")


def run_live(producer: Producer) -> None:
    from alpaca.data.enums import DataFeed
    from alpaca.data.live import StockDataStream

    api_key = os.environ["ALPACA_API_KEY"]
    secret_key = os.environ["ALPACA_SECRET_KEY"]
    feed = DataFeed(os.environ.get("ALPACA_FEED", "iex"))

    stream = StockDataStream(api_key, secret_key, feed=feed)

    async def on_trade(trade):
        publish_event(producer, trade.symbol, trade.price, trade.size, trade.timestamp)

    stream.subscribe_trades(on_trade, *TICKERS)
    print(f"Live mode: subscribed to Alpaca {feed.value} trades for {len(TICKERS)} tickers")
    stream.run()


if __name__ == "__main__":
    kafka_producer = make_producer()
    if STREAM_MODE == "replay":
        run_replay(kafka_producer)
    else:
        run_live(kafka_producer)
