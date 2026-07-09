"""
Publishes analyst/news sentiment items to the Kafka topic "analyst-ratings",
deliberately arriving late relative to their event_time so the downstream
Spark watermarking logic (up to 48h lateness) has something to handle.

Message schema matches bronze_analyst_ratings (see docs/data_model.md):
    rating_id, ticker, event_time, arrival_time, rating_text, source

Alpha Vantage has no dedicated "analyst ratings" endpoint on the free tier,
so NEWS_SENTIMENT is used as the closest proxy: each news item's
overall_sentiment_label becomes rating_text.

Modes (set STREAM_MODE in .env):
    live    - fetches real news via Alpha Vantage (25 req/day free cap,
              so responses are cached to data/bronze/alpha_vantage/) and
              holds each item for its full randomized 1-48h delay before
              publishing — realistic, but slow to demo.
    replay  - same delay logic, compressed by DELAY_COMPRESSION_FACTOR
              (default 720x, so 48h -> ~4 minutes) using cached or
              synthetic sample data — this is what the demo video should use.
"""

import json
import os
import random
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "analyst-ratings"
STREAM_MODE = os.environ.get("STREAM_MODE", "live")
DELAY_COMPRESSION_FACTOR = int(os.environ.get("DELAY_COMPRESSION_FACTOR", "720"))
SOURCE = "Alpha Vantage"

TICKERS = [
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN",
    "META", "GOOG", "AMD", "NFLX", "JPM",
]  # smaller subset than market data — Alpha Vantage free tier is 25 req/day

CACHE_DIR = Path(__file__).resolve().parents[2] / "data" / "bronze" / "alpha_vantage"


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for {msg.key()}: {err}")


def publish_rating(producer: Producer, ticker: str, rating_text: str, event_time: datetime, arrival_time: datetime) -> None:
    rating = {
        "rating_id": str(uuid.uuid4()),
        "ticker": ticker,
        "event_time": event_time.isoformat(),
        "arrival_time": arrival_time.isoformat(),
        "rating_text": rating_text,
        "source": SOURCE,
    }
    producer.produce(TOPIC, key=ticker, value=json.dumps(rating), callback=delivery_report)
    producer.poll(0)


def schedule_delayed_publish(producer: Producer, ticker: str, rating_text: str, event_time: datetime, compress: bool) -> None:
    """Sleeps for the (optionally compressed) delay in a background thread, then publishes."""
    real_delay_seconds = random.uniform(3600, 48 * 3600)  # 1h - 48h
    sleep_seconds = real_delay_seconds / DELAY_COMPRESSION_FACTOR if compress else real_delay_seconds
    arrival_time = event_time + timedelta(seconds=real_delay_seconds)

    def _worker():
        time.sleep(sleep_seconds)
        publish_rating(producer, ticker, rating_text, event_time, arrival_time)
        producer.flush()

    threading.Thread(target=_worker, daemon=True).start()
    print(f"  scheduled {ticker} rating: event_time={event_time.isoformat()} "
          f"-> arrives in {sleep_seconds:.1f}s (simulates {real_delay_seconds/3600:.1f}h)")


def fetch_alpha_vantage_news(ticker: str, api_key: str) -> list[dict]:
    resp = requests.get(
        "https://www.alphavantage.co/query",
        params={"function": "NEWS_SENTIMENT", "tickers": ticker, "apikey": api_key},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("feed", [])


def get_items_for_ticker(ticker: str, api_key: str | None) -> list[dict]:
    """Returns cached news items if present, else fetches (live mode with a key)."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"{ticker}.json"

    if cache_file.exists():
        return json.loads(cache_file.read_text())

    if not api_key:
        return synthetic_items(ticker)

    try:
        feed = fetch_alpha_vantage_news(ticker, api_key)
    except requests.RequestException as e:
        print(f"  Alpha Vantage request failed for {ticker}: {e}, falling back to synthetic data")
        return synthetic_items(ticker)

    if not feed:
        return synthetic_items(ticker)

    cache_file.write_text(json.dumps(feed))
    return feed


def synthetic_items(ticker: str) -> list[dict]:
    labels = ["Bullish", "Somewhat-Bullish", "Neutral", "Somewhat-Bearish", "Bearish"]
    now = datetime.now(timezone.utc)
    return [
        {
            "time_published": (now - timedelta(hours=random.uniform(0, 12))).strftime("%Y%m%dT%H%M%S"),
            "overall_sentiment_label": random.choice(labels),
        }
        for _ in range(random.randint(1, 3))
    ]


def parse_event_time(raw: str) -> datetime:
    try:
        return datetime.strptime(raw, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc)


def run(producer: Producer) -> None:
    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY") if STREAM_MODE == "live" else None
    compress = STREAM_MODE != "live"

    for ticker in TICKERS:
        items = get_items_for_ticker(ticker, api_key)
        for item in items:
            event_time = parse_event_time(item.get("time_published", ""))
            rating_text = item.get("overall_sentiment_label", "Neutral")
            schedule_delayed_publish(producer, ticker, rating_text, event_time, compress)

    print("All late-arriving ratings scheduled. Waiting for delivery...")
    # Keep the process alive long enough for the background threads to publish.
    max_wait = (48 * 3600 / DELAY_COMPRESSION_FACTOR) + 30 if compress else 48 * 3600 + 30
    time.sleep(max_wait)


if __name__ == "__main__":
    kafka_producer = make_producer()
    run(kafka_producer)
