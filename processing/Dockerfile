# Adds yfinance (not bundled in the base image) so download_yahoo.py can run
# entirely inside the container -- no host Python setup needed to get batch
# data flowing, matching the "runs with just docker" requirement. Also adds
# the dashboard's dependencies: scikit-learn for train_model.py, FastAPI +
# uvicorn for the dashboard API service (same image, different command --
# see docker-compose.yml's dashboard-api service).
FROM tabulario/spark-iceberg

RUN pip install --no-cache-dir \
    yfinance==1.5.1 \
    scikit-learn==1.5.2 \
    "fastapi==0.115.5" \
    "uvicorn[standard]==0.32.1"
