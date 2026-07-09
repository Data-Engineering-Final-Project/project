import os
import pandas as pd
import yfinance as yf

OUTPUT_DIR = "data/bronze/yahoo"

tickers = [
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN",
    "META", "GOOG", "AMD", "NFLX", "JPM",
    "BAC", "XOM", "UNH", "JNJ", "COST",
    "WMT", "DIS", "PEP", "KO", "INTC",
    "CRM", "ORCL", "CSCO", "V", "MA",
    "PFE", "MRK", "NKE", "ADBE", "AVGO"
]

os.makedirs(OUTPUT_DIR, exist_ok=True)

all_data = []

for ticker in tickers:
    print(f"Downloading {ticker}...")

    df = yf.download(
        ticker,
        period="5y",
        interval="1d",
        progress=False
    )

    if df.empty:
        print(f"No data for {ticker}")
        continue

    df = df.reset_index()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df["Ticker"] = ticker

    df = df[["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]]

    all_data.append(df)

historical_data = pd.concat(all_data, ignore_index=True)

csv_path = f"{OUTPUT_DIR}/historical_market_data.csv"
parquet_path = f"{OUTPUT_DIR}/historical_market_data.parquet"

historical_data.to_csv(csv_path, index=False)
historical_data.to_parquet(parquet_path, index=False)

print("Done!")
print(f"Rows: {len(historical_data)}")
print(f"CSV saved to: {csv_path}")
print(f"Parquet saved to: {parquet_path}")
print(historical_data.head())