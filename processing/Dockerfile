# Adds yfinance (not bundled in the base image) so download_yahoo.py can run
# entirely inside the container -- no host Python setup needed to get batch
# data flowing, matching the "runs with just docker" requirement.
FROM tabulario/spark-iceberg

RUN pip install --no-cache-dir yfinance==1.5.1
