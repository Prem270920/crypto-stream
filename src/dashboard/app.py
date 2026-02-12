import streamlit as st
import pandas as pd
import glob
import time
import duckdb
import plotly.express as px
import uuid

st.set_page_config(
    page_title="CryptoStream: Lambda Architecture",
    page_icon="⚡",
    layout="wide"
)

st.title("⚡ CryptoStream: Real-Time & Historical")

# Data Sources
STREAM_DIR = "data/processed/stream_history/*.parquet"
WAREHOUSE_DB = "data/crypto_warehouse.duckdb"

# The Hot Path
def load_live_data():
    files = glob.glob(STREAM_DIR)
    if not files:
        return pd.DataFrame()
    
    dfs = [pd.read_parquet(f) for f in files]
    full_df = pd.concat(dfs, ignore_index=True)
    full_df['timestamp'] = pd.to_datetime(full_df['timestamp'], unit='ms')
    return full_df.sort_values("timestamp")

# The Cold Path
def load_historical_trend():
    # connect to the DuckDB file dbt created
    con = duckdb.connect(WAREHOUSE_DB, read_only=True)
    
    query = """
    SELECT metric_date, close_price, moving_avg_7d 
    FROM bitcoin_weekly_trend 
    ORDER BY metric_date DESC
    LIMIT 30
    """
    df = con.sql(query).df()
    con.close()
    return df

col1, col2 = st.columns(2)

# We use a placeholder to allow for dynamic updates without re-rendering the entire page every time. 
# This way we can fetch live data and historical data in a loop and update the relevant
placeholder = st.empty()

while True:
    # Fetch Live Data
    live_df = load_live_data()
    
    # Fetch Warehouse Data 
    try:
        hist_df = load_historical_trend()
        warehouse_status = "Online"
    except Exception as e:
        hist_df = pd.DataFrame()
        warehouse_status = f"Offline ({str(e)})"

    with placeholder.container():
        # THE TRADER VIEW (Real-Time)
        with col1:
            st.header(f"Live Market (Speed Layer)")
            if not live_df.empty:
                latest = live_df.iloc[-1]
                st.metric("Live Price", f"${latest['price']:,.2f}")
                
                fig = px.line(live_df, x='timestamp', y='price', title='Last Hour Volatility')
                fig.update_layout(height=400)
                # We use a unique key for the live chart to ensure it updates correctly without caching issues.
                st.plotly_chart(fig, use_container_width=True, key=f"live_chart_{uuid.uuid4()}")
            else:
                st.warning("Waiting for live data...")

        # THE ANALYST VIEW (Historical)
        with col2:
            st.header(f"📊 Market Trend (Batch Layer)")
            st.caption(f"Warehouse Status: {warehouse_status}")
            
            if not hist_df.empty:
                latest_trend = hist_df.iloc[0]
                st.metric("7-Day Moving Avg", f"${latest_trend['moving_avg_7d']:,.2f}")
                
                # Price vs Moving Average
                fig2 = px.line(hist_df, x='metric_date', y=['close_price', 'moving_avg_7d'], 
                               title='30-Day Trend Analysis', color_discrete_map={"moving_avg_7d": "orange", "close_price": "blue"})
                fig2.update_layout(height=400)
                # We use a unique key for the historical chart to ensure it updates correctly without caching issues.
                st.plotly_chart(fig2, use_container_width=True, key=f"history_chart_{uuid.uuid4()}")
            else:
                st.info("Run 'dbt run' to populate the warehouse!")

    time.sleep(2)