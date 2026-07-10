"""
One-time static reference table for the dashboard's sector heatmap.

Not part of the approved data_model.md (the mid-semester proposal mentioned
a Company_Dim with sector/industry, but it never made it into the final
schema) -- added specifically to support the sector heatmap panel, which
needs a sector attribute per ticker that nothing else in the pipeline provides.

Run inside the spark-iceberg container:
    docker exec spark-iceberg python3 /home/iceberg/jobs/seed_dim_stocks.py
"""

from pyspark.sql import SparkSession

# (ticker, company_name, sector, industry) -- well-known public classifications
# for the 30 tickers tracked across the pipeline (see download_yahoo.py).
STOCKS = [
    ("AAPL", "Apple Inc.", "Technology", "Consumer Electronics"),
    ("MSFT", "Microsoft Corporation", "Technology", "Software"),
    ("NVDA", "NVIDIA Corporation", "Technology", "Semiconductors"),
    ("TSLA", "Tesla, Inc.", "Consumer Discretionary", "Automobiles"),
    ("AMZN", "Amazon.com, Inc.", "Consumer Discretionary", "Internet Retail"),
    ("META", "Meta Platforms, Inc.", "Communication Services", "Internet Content"),
    ("GOOG", "Alphabet Inc.", "Communication Services", "Internet Content"),
    ("AMD", "Advanced Micro Devices, Inc.", "Technology", "Semiconductors"),
    ("NFLX", "Netflix, Inc.", "Communication Services", "Entertainment"),
    ("JPM", "JPMorgan Chase & Co.", "Financials", "Banks"),
    ("BAC", "Bank of America Corporation", "Financials", "Banks"),
    ("XOM", "Exxon Mobil Corporation", "Energy", "Oil & Gas"),
    ("UNH", "UnitedHealth Group Incorporated", "Healthcare", "Managed Care"),
    ("JNJ", "Johnson & Johnson", "Healthcare", "Pharmaceuticals"),
    ("COST", "Costco Wholesale Corporation", "Consumer Staples", "Retail"),
    ("WMT", "Walmart Inc.", "Consumer Staples", "Retail"),
    ("DIS", "The Walt Disney Company", "Communication Services", "Entertainment"),
    ("PEP", "PepsiCo, Inc.", "Consumer Staples", "Beverages"),
    ("KO", "The Coca-Cola Company", "Consumer Staples", "Beverages"),
    ("INTC", "Intel Corporation", "Technology", "Semiconductors"),
    ("CRM", "Salesforce, Inc.", "Technology", "Software"),
    ("ORCL", "Oracle Corporation", "Technology", "Software"),
    ("CSCO", "Cisco Systems, Inc.", "Technology", "Networking"),
    ("V", "Visa Inc.", "Financials", "Payment Services"),
    ("MA", "Mastercard Incorporated", "Financials", "Payment Services"),
    ("PFE", "Pfizer Inc.", "Healthcare", "Pharmaceuticals"),
    ("MRK", "Merck & Co., Inc.", "Healthcare", "Pharmaceuticals"),
    ("NKE", "Nike, Inc.", "Consumer Discretionary", "Apparel"),
    ("ADBE", "Adobe Inc.", "Technology", "Software"),
    ("AVGO", "Broadcom Inc.", "Technology", "Semiconductors"),
]


def main():
    spark = SparkSession.builder.appName("seed_dim_stocks").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.gold")

    df = spark.createDataFrame(STOCKS, ["ticker", "company_name", "sector", "industry"])
    df.writeTo("demo.gold.dim_stocks").createOrReplace()

    print(f"Wrote demo.gold.dim_stocks: {df.count()} tickers across {df.select('sector').distinct().count()} sectors")
    spark.stop()


if __name__ == "__main__":
    main()
