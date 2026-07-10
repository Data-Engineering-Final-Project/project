"""
Trains the anomaly outcome predictor the dashboard's ML panel serves.

Features/label are exactly what fact_volumetric_anomalies already provides
(see docs/data_model.md) -- no extra feature engineering needed, this is a
direct read of the gold feature table already built for this purpose.

Run inside the spark-iceberg container (after silver_to_gold.py has run at
least once):
    docker exec spark-iceberg python3 /home/iceberg/jobs/train_model.py
"""

import pickle

from pyspark.sql import SparkSession
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report

FEATURES = ["volume_ratio", "rsi_value", "volatility_index", "sentiment_score"]
LABEL = "target_label"
MODEL_PATH = "/home/iceberg/warehouse/models/anomaly_predictor.pkl"


def main():
    spark = SparkSession.builder.appName("train_model").getOrCreate()

    df = spark.table("demo.gold.fact_volumetric_anomalies").toPandas()
    # sentiment_score is null wherever no analyst coverage was active yet for
    # that date -- treat as neutral (0) rather than dropping the row.
    df["sentiment_score"] = df["sentiment_score"].fillna(0)
    df = df.dropna(subset=["volume_ratio", "rsi_value", "volatility_index"])

    X = df[FEATURES]
    y = df[LABEL].astype(int)

    print(f"Training on {len(df)} anomalies, {y.sum()} positive ({y.mean():.1%})")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = RandomForestClassifier(
        n_estimators=200, max_depth=6, class_weight="balanced", random_state=42
    )
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    print(f"Test accuracy: {accuracy_score(y_test, preds):.3f}")
    print(classification_report(y_test, preds))
    print("Feature importances:", dict(zip(FEATURES, model.feature_importances_.round(3))))

    import os
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump({"model": model, "features": FEATURES}, f)
    print(f"Saved model to {MODEL_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
