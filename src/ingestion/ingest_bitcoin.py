import requests
import pandas as pd
from datetime import datetime

# Define the API endpoint and parameters
url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
params = {
    'vs_currency': 'usd',
    'days': '30',
    'interval': 'daily'
}

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