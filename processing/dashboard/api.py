"""
Dashboard backend: one persistent SparkSession serving the four panels from
the presentation, plus the static frontend. A persistent session matters --
spark-submit per request has multi-second JVM startup overhead, unusable for
a UI that polls every few seconds.

Run inside the spark-iceberg container (see docker-compose.yml's
dashboard-api service):
    python3 /home/iceberg/dashboard/api.py
"""

import os
import pickle
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pyspark.sql import SparkSession

MODEL_PATH = "/home/iceberg/warehouse/models/anomaly_predictor.pkl"
FRONTEND_DIR = Path(__file__).resolve().parent / "frontend"

# Found this process sitting at 2.23GB after ~2 weeks of uninterrupted
# uptime (started around 300MB right after the entrypoint/memory fix). A
# long-lived local-mode Spark driver polled every 5s for weeks accumulates
# query execution history, cached plans, etc. that never gets fully
# reclaimed -- normal for a process that's never meant to run indefinitely
# without a restart. That growth was a real contributor to a stack-wide OOM
# event that killed the Kafka container outright (see streaming/docker-compose.yml).
# Recycling once a day is cheap (a few seconds of downtime, `restart:
# unless-stopped` brings a fresh SparkSession right back) and keeps memory
# bounded instead of trusting it never grows enough to matter.
MAX_UPTIME_SECONDS = 24 * 60 * 60
_start_time = time.monotonic()

spark = (
    SparkSession.builder.appName("dashboard_api")
    # Iceberg's REST catalog client wraps tables in a CachingCatalog with no
    # default expiration (cache-enabled defaults to true, with no TTL set).
    # For a one-shot batch job that's irrelevant, but this process holds one
    # SparkSession for its entire lifetime -- caught this directly: after
    # restarting dashboard-api at a moment when bronze_market_events had no
    # recent rows (the streaming consumer was mid-recovery from a separate
    # Kafka outage), it kept returning empty/stale results from
    # /api/live-feed for the rest of its life, even minutes later once the
    # consumer was healthy and thousands of fresh rows had landed -- because
    # the very first table lookup got cached and nothing ever told Spark to
    # look again. A brand-new process picked the same data up immediately,
    # confirming it's this cache, not the query or the underlying data.
    # Disabling it costs a bit of REST-catalog metadata traffic per query
    # (irrelevant at this poll volume); correctness for a page whose whole
    # point is showing current data matters more.
    .config("spark.sql.catalog.demo.cache-enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

_model_cache = None


def run_query(sql_text: str):
    """spark.sql(...).collect(), with two deliberate exit conditions that
    both resolve the same way -- fail fast and let Docker's `restart:
    unless-stopped` bring up a fresh process with a fresh SparkSession,
    rather than trying to patch up state in-process:

    1. The JVM backing this long-lived SparkSession can die outright
       (observed in practice under memory pressure), so every subsequent
       query fails with a Py4J connection error even though nothing is
       wrong with the query itself. PySpark sessions aren't designed to be
       rebuilt cleanly inside the same interpreter (the driver's internal
       state stays half-wired to the dead JVM).
    2. The process has been up longer than MAX_UPTIME_SECONDS. Nothing has
       necessarily failed yet, but unbounded memory growth over many days
       of uptime is a real, observed risk (see the comment above) -- this
       recycles proactively instead of waiting for it to become another
       OOM incident.
    """
    if time.monotonic() - _start_time > MAX_UPTIME_SECONDS:
        print(f"[INFO] Uptime exceeded {MAX_UPTIME_SECONDS}s; exiting for a scheduled clean restart", flush=True)
        os._exit(0)
    try:
        return spark.sql(sql_text).collect()
    except Exception as e:
        if "Connection refused" in str(e) or "Py4J" in type(e).__name__:
            print(f"[FATAL] Spark JVM connection lost ({e!r}); exiting for a clean restart", flush=True)
            os._exit(1)
        raise


def load_model():
    """Read the pickled model off disk into _model_cache.

    Deliberately called once at startup rather than lazily on the first
    /api/predict request. The lazy version meant a failure here only
    surfaced when someone actually clicked the ML panel -- and it surfaced
    as a raw 500 with a stack trace, with the other four panels working
    fine, which reads like "the ML panel is broken" rather than "the model
    file couldn't be read". Loading eagerly turns that into one obvious
    line in `docker logs dashboard-api` at boot.
    """
    global _model_cache
    if not Path(MODEL_PATH).exists():
        print(f"[WARN] No model at {MODEL_PATH}; /api/predict will return 503 "
              f"until train_model.py runs", flush=True)
        return
    try:
        with open(MODEL_PATH, "rb") as f:
            _model_cache = pickle.load(f)
        print(f"[INFO] Loaded model from {MODEL_PATH}", flush=True)
    except OSError as e:
        # Seen for real: OSError(errno 35, 'Resource deadlock avoided')
        # reading a file that exists and has a valid size. Cause was the
        # host's cloud-sync layer (OneDrive Files On-Demand) having evicted
        # the file's contents to the cloud after ~2 weeks of nobody touching
        # it -- the model is written once by train_model.py and then only
        # read, so it's exactly the kind of file that gets dehydrated. A
        # read from the host transparently re-downloads it; a read from
        # inside a container through the bind mount cannot trigger that, it
        # just fails. MODEL_DIR is a named Docker volume now specifically so
        # this path never touches cloud-synced storage again, but keep this
        # handler: it turns an unexplained 500 into a named cause.
        print(f"[WARN] Could not read {MODEL_PATH} ({e}); /api/predict will "
              f"return 503. If the warehouse is on cloud-synced storage "
              f"(OneDrive/iCloud/Dropbox), the file may be a placeholder -- "
              f"re-run train_model.py to regenerate it locally.", flush=True)


def get_model():
    if _model_cache is None:
        raise HTTPException(
            status_code=503,
            detail="Model unavailable. Run: docker exec spark-iceberg python3 /home/iceberg/jobs/train_model.py",
        )
    return _model_cache


app = FastAPI(title="Volumetric Anomaly Detection Dashboard")

load_model()


def rows_to_dicts(rows):
    return [row.asDict() for row in rows]


@app.get("/api/sector-heatmap")
def sector_heatmap():
    rows = run_query("""
        SELECT s.sector,
               avg(f.volume_ratio) AS avg_volume_ratio,
               count(*) AS anomaly_count
        FROM demo.gold.fact_volumetric_anomalies f
        JOIN demo.gold.dim_stocks s ON f.ticker = s.ticker
        GROUP BY s.sector
        ORDER BY avg_volume_ratio DESC
    """)
    return rows_to_dicts(rows)


@app.get("/api/top-spikes")
def top_spikes(limit: int = 10):
    rows = run_query(f"""
        SELECT f.ticker, s.company_name, s.sector, f.event_time,
               f.volume_ratio, f.rsi_value, f.target_label
        FROM demo.gold.fact_volumetric_anomalies f
        JOIN demo.gold.dim_stocks s ON f.ticker = s.ticker
        ORDER BY f.volume_ratio DESC
        LIMIT {limit}
    """)
    return rows_to_dicts(rows)


@app.get("/api/predict/{ticker}")
def predict(ticker: str):
    cached = get_model()
    model, features = cached["model"], cached["features"]

    rows = run_query(f"""
        SELECT {", ".join(features)}
        FROM demo.gold.fact_volumetric_anomalies
        WHERE ticker = '{ticker.upper()}'
        ORDER BY event_time DESC
        LIMIT 1
    """)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No anomaly data for {ticker}")

    row = rows[0].asDict()
    x = [[row[f] if row[f] is not None else 0 for f in features]]
    probability = model.predict_proba(x)[0][1]
    predicted = bool(model.predict(x)[0])

    return {
        "ticker": ticker.upper(),
        "features": row,
        "predicted_label": predicted,
        "probability": round(float(probability), 4),
    }


@app.get("/api/late-arrivals")
def late_arrivals(limit: int = 20):
    rows = run_query(f"""
        SELECT ticker, event_time, arrival_time,
               (unix_timestamp(arrival_time) - unix_timestamp(event_time)) / 3600.0 AS delay_hours,
               rating_text, sentiment_score
        FROM demo.silver.silver_analyst_ratings
        ORDER BY arrival_time DESC
        LIMIT {limit}
    """)
    return rows_to_dicts(rows)


@app.get("/api/live-feed")
def live_feed(limit: int = 30):
    # Reads bronze, not silver -- bronze_ingest_streams.py appends every few
    # seconds as ticks arrive on Kafka, while silver_market_prices only
    # refreshes on Airflow's ~15min batch cycle. This is the one panel that
    # should visibly move second-to-second; the others legitimately reflect
    # the last batch run.
    #
    # The `event_time > now() - 10 minutes` filter isn't just semantically
    # correct for a "live feed" (hour-old ticks aren't live) -- it's load
    # bearing. Continuous append-only streaming writes accumulate many small
    # Iceberg files over hours of uptime (thousands after a long-running demo
    # session), and an unfiltered ORDER BY had to plan a scan across all of
    # them, taking long enough to feel broken under memory pressure. Filtering
    # on event_time lets Iceberg prune files by their min/max stats instead of
    # touching the whole table.
    rows = run_query(f"""
        SELECT ticker, event_time, price, volume
        FROM demo.bronze.bronze_market_events
        WHERE price > 0 AND event_time > current_timestamp() - INTERVAL 10 MINUTES
        ORDER BY event_time DESC
        LIMIT {limit}
    """)
    return rows_to_dicts(rows)


@app.get("/api/last-updated")
def last_updated():
    """How current the data behind the dashboard actually is, split by layer
    -- the live ticker strip reflects the continuous streaming write into
    bronze, while the heatmap/spikes/predictor panels only move when
    Airflow's ~15min batch chain refreshes the gold tables. Blending these
    into a single number would misrepresent how fresh those panels are, so
    both are reported separately.

    Same event_time window filter as /api/live-feed on the bronze query --
    load-bearing for Iceberg file pruning, not just semantics (see that
    endpoint's comment).

    analytics_as_of will always lag "today" by roughly a trading week even
    right after a same-day Yahoo download -- not staleness, but inherent to
    how fact_volumetric_anomalies is labeled: silver_to_gold.py uses
    lead(close, 5) to check whether price moved +-5% over the *next* 5
    trading days, so a day can't be labeled (and therefore can't appear in
    this table) until 5 trading days after it actually happened.
    """
    live = run_query("""
        SELECT max(event_time) AS latest
        FROM demo.bronze.bronze_market_events
        WHERE event_time > current_timestamp() - INTERVAL 10 MINUTES
    """)
    gold = run_query("""
        SELECT max(event_time) AS latest
        FROM demo.gold.fact_volumetric_anomalies
    """)
    return {
        "live_feed_as_of": live[0]["latest"] if live else None,
        "analytics_as_of": gold[0]["latest"] if gold else None,
    }


app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
