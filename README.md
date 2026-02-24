# CryptoStream: Predictive Lambda Architecture

A production-grade, end-to-end data pipeline demonstrating the Lambda Architecture pattern for algorithmic trading research. This system simultaneously processes real-time cryptocurrency market data (Speed Layer) and aggregates historical trends with machine learning forecasts (Batch Layer).

## Architecture Overview

* **Speed Layer (Real-Time):** A Python-based Kafka Producer/Consumer streams live price data, ensuring low-latency updates.
* **Batch Layer (Historical):** Apache Airflow orchestrates daily data ingestion from the CoinGecko API. 
* **Transformation (Data Warehouse):** Raw Parquet files are modeled and transformed using **dbt** and materialized into a **DuckDB** warehouse for analytical querying.
* **Serving Layer (UI & ML):** A Streamlit dashboard visualizes the real-time order flow alongside an ARIMA statistical model, projecting 7-day price forecasts with 95% confidence intervals.

## Tech Stack

* **Language:** Python 3.9
* **Streaming Engine:** Apache Kafka, Zookeeper
* **Orchestration:** Apache Airflow
* **Data Transformation:** dbt (data build tool)
* **Data Warehouse:** DuckDB (Storage: Parquet)
* **Visualization:** Streamlit, Plotly
* **Machine Learning:** statsmodels (ARIMA)
* **Infrastructure:** Docker & Docker Compose

## 🚀 How to Run Locally

### 1. Boot the Infrastructure
Ensure Docker Desktop is running, then spin up the core services (Kafka, Airflow, Postgres):
```bash
docker-compose up -d
```

### Start the Real-Time Stream
Activate virtual environment and launch the Kafka producer:
```bash
python src/ingestion/stream_bitcoin.py
```

### Build the Data Warehouse
Transform the raw historical data into the DuckDB analytical tables:
```bash
cd transform
dbt run --profiles-dir .
cd ..
```

### Launch the Dashboard
Start the Streamlit application to view the live market and ML forecasts:
```bash
streamlit run src/dashboard/app.py
```
## Machine Learning Engine
The batch layer utilizes an Auto-Regressive Integrated Moving Average ARIMA(5,1,0) model to project future asset prices based on the 7-day moving average. The dashboard renders these projections inside a 95% statistical confidence interval to simulate professional quantitative research environments.
