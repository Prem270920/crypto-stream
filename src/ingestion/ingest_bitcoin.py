import requests
import pandas as pd
from datetime import datetime
import os

# This points to crypto-stream/data/raw/bitcoin_data_lake
output_dir = os.path.join("data", "raw", "bitcoin_data_lake")
os.makedirs(output_dir, exist_ok=True)

# Define the API endpoint and parameters
url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
params = {'vs_currency': 'usd', 'days': '30', 'interval': 'daily'}

print("Fetching data from CoinGecko...")
response = requests.get(url, params=params)
data = response.json()

prices = data['prices']
df = pd.DataFrame(prices, columns= ['timestamp', 'price'])
# Convert timestamp from milliseconds to datetime
df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms')
# Format date as 'YYYY-MM-DD'
df["date"] = df["timestamp"].dt.strftime('%Y-%m-%d')

print(f"Data fetched! Rows: {len(df)}")
print(df.head())

# Save to the new structured path
print(f"Saving to {output_dir}...")
df.to_parquet(output_dir, partition_cols=['date'])
print("Data saved successfully.")