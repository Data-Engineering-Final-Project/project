# Data Model

```mermaid
erDiagram
    bronze_historical_prices {
        date event_date
        string ticker
        double open
        double high
        double low
        double close
        long volume
        timestamp ingestion_timestamp
    }

    bronze_market_events {
        string event_id
        string ticker
        timestamp event_time
        timestamp arrival_time
        double price
        long volume
        timestamp ingestion_timestamp
    }

    bronze_analyst_ratings {
        string rating_id
        string ticker
        timestamp event_time
        timestamp arrival_time
        string rating_text
        string source
        timestamp ingestion_timestamp
    }

    silver_historical_stats {
        date event_date
        string ticker
        double close
        long volume
        double avg_10d_volume
        double volume_ratio
        double volatility_index
        double rsi_value
    }

    silver_market_prices {
        string event_id
        string ticker
        timestamp event_time
        double price
        long volume
        boolean is_valid
    }

    silver_analyst_ratings {
        string rating_id
        string ticker
        timestamp event_time
        timestamp arrival_time
        string rating_text
        int sentiment_score
    }

    dim_analyst_coverage {
        string analyst_key
        string ticker
        int sentiment_score
        timestamp effective_from
        timestamp effective_to
        boolean is_current
    }

    fact_volumetric_anomalies {
        string anomaly_id
        string ticker
        timestamp event_time
        double volume_ratio
        double rsi_value
        double volatility_index
        int sentiment_score
        boolean target_label
    }

    dim_stocks {
        string ticker
        string company_name
        string sector
        string industry
    }

    fact_volumetric_anomalies }o--|| dim_stocks : "ticker"
    fact_volumetric_anomalies }o--o| dim_analyst_coverage : "ticker, as of event_time"
```

## Gold layer notes

`fact_volumetric_anomalies` is the star schema's fact table. One row per day
where a ticker's volume exceeded 2x its 10-day average, labeled with whether
the price then moved +-5% within 5 trading days.

`dim_analyst_coverage` is a **Type 2 slowly changing dimension**. A ticker
appears once per sentiment change, each version bounded by
`effective_from`/`effective_to`, with exactly one row per ticker carrying
`is_current = true`. The fact table joins to it *temporally* — matching each
anomaly against the version that was in effect on that date, not the current
one — which is what makes the history worth keeping. The join is a left join,
so `sentiment_score` is null for anomalies predating any analyst coverage.

`dim_stocks` is a static reference dimension (30 tickers, 7 sectors), loaded
once by `seed_dim_stocks.py`. It was not in the mid-semester model: the
sector heatmap panel needs a sector per ticker and nothing else in the
pipeline provides one, so it was added to support that.
