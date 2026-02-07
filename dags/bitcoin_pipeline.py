from airflow.decorators import dag, task
from datetime import datetime
import requests
import pandas as pd
import os

OUTPUT_PATH = "/opt/airflow/data/raw/bitcoin_data_lake"

@dag(
    start_date=datetime(2024, 1, 1), # When did this workflow "start"?
    schedule='@daily',               
    catchup=False,                   # Don't run for all the past days we missed
    tags=['crypto', 'ingestion']    
)
def bitcoin_pipeline():

    @task
    def extract_bitcoin_data():
        print("🚀 Starting extraction from CoinGecko...")
        
        # Fetch Data
        url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
        params = {'vs_currency': 'usd', 'days': '1', 'interval': 'daily'} 
        
        try:
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            # Transform
            df = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')
            
            # Ensure folder exists
            os.makedirs(OUTPUT_PATH, exist_ok=True)
            
            # Save using the date partition
            save_path = os.path.join(OUTPUT_PATH)
            df.to_parquet(save_path, partition_cols=['date'])
            
            print(f"Data saved to {save_path}")
            return "Success"

        except Exception as e:
            print(f"Error: {e}")
            raise e

    extract_bitcoin_data()

dag_instance = bitcoin_pipeline()