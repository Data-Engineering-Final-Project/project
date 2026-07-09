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