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
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pyspark.sql import SparkSession

MODEL_PATH = "/home/iceberg/warehouse/models/anomaly_predictor.pkl"
FRONTEND_DIR = Path(__file__).resolve().parent / "frontend"

spark = SparkSession.builder.appName("dashboard_api").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

_model_cache = None


def run_query(sql_text: str):
    """spark.sql(...).collect(), with one specific failure mode handled
    deliberately: the JVM backing this long-lived SparkSession can die
    (observed in practice under memory pressure) while this Python process
    keeps running, so every subsequent query fails with a Py4J connection
    error even though nothing is wrong with the query itself.

    PySpark sessions aren't designed to be rebuilt cleanly inside the same
    interpreter (the driver's internal state stays half-wired to the dead
    JVM), so patching this up in-process is the fragile path. Restarting
    the whole process and letting Docker's `restart: unless-stopped` bring
    up a fresh container with a fresh SparkSession is the same fix real
    infra uses for "a stateful dependency wedged" -- fail fast, come back
    clean, rather than accumulate patched-up state.
    """
    try:
        return spark.sql(sql_text).collect()
    except Exception as e:
        if "Connection refused" in str(e) or "Py4J" in type(e).__name__:
            print(f"[FATAL] Spark JVM connection lost ({e!r}); exiting for a clean restart", flush=True)
            os._exit(1)
        raise


def get_model():
    global _model_cache
    if _model_cache is None:
        if not Path(MODEL_PATH).exists():
            raise HTTPException(
                status_code=503,
                detail="Model not trained yet. Run: docker exec spark-iceberg python3 /home/iceberg/jobs/train_model.py",
            )
        with open(MODEL_PATH, "rb") as f:
            _model_cache = pickle.load(f)
    return _model_cache


app = FastAPI(title="Volumetric Anomaly Detection Dashboard")


def rows_to_dicts(rows):
    return [row.asDict() for row in rows]


def date_range_clause(column: str, start_date: Optional[str], end_date: Optional[str]) -> str:
    """Builds a `WHERE`-ready SQL fragment from optional YYYY-MM-DD strings.

    No filter (default) means "show everything currently in the table" --
    same as before this feature existed. Rejects anything that doesn't parse
    as a plain date rather than interpolating user input directly into SQL.
    """
    clauses = []
    try:
        if start_date:
            datetime.strptime(start_date, "%Y-%m-%d")  # raises ValueError if malformed
            clauses.append(f"{column} >= TIMESTAMP '{start_date} 00:00:00'")
        if end_date:
            datetime.strptime(end_date, "%Y-%m-%d")
            clauses.append(f"{column} <= TIMESTAMP '{end_date} 23:59:59'")
    except ValueError:
        raise HTTPException(status_code=400, detail="Dates must be YYYY-MM-DD")
    return (" AND " + " AND ".join(clauses)) if clauses else ""


@app.get("/api/sector-heatmap")
def sector_heatmap(start_date: Optional[str] = None, end_date: Optional[str] = None):
    where = date_range_clause("f.event_time", start_date, end_date)
    rows = run_query(f"""
        SELECT s.sector,
               avg(f.volume_ratio) AS avg_volume_ratio,
               count(*) AS anomaly_count
        FROM demo.gold.fact_volumetric_anomalies f
        JOIN demo.gold.dim_stocks s ON f.ticker = s.ticker
        WHERE 1=1 {where}
        GROUP BY s.sector
        ORDER BY avg_volume_ratio DESC
    """)
    return rows_to_dicts(rows)


@app.get("/api/top-spikes")
def top_spikes(limit: int = 10, start_date: Optional[str] = None, end_date: Optional[str] = None):
    where = date_range_clause("f.event_time", start_date, end_date)
    rows = run_query(f"""
        SELECT f.ticker, s.company_name, s.sector, f.event_time,
               f.volume_ratio, f.rsi_value, f.target_label
        FROM demo.gold.fact_volumetric_anomalies f
        JOIN demo.gold.dim_stocks s ON f.ticker = s.ticker
        WHERE 1=1 {where}
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
def late_arrivals(limit: int = 20, start_date: Optional[str] = None, end_date: Optional[str] = None):
    where = date_range_clause("event_time", start_date, end_date)
    rows = run_query(f"""
        SELECT ticker, event_time, arrival_time,
               (unix_timestamp(arrival_time) - unix_timestamp(event_time)) / 3600.0 AS delay_hours,
               rating_text, sentiment_score
        FROM demo.silver.silver_analyst_ratings
        WHERE 1=1 {where}
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


app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
