import streamlit as st
import pandas as pd
import glob
import time

st.set_page_config(
    page_title="CryptoStream Real-Time",
    page_icon="📈",
    layout="wide"
)

st.title("Bitcoin Real-Time Lakehouse")

DATA_DIR = "data/processed/stream_history/*.parquet"

# This function reads ALL the mini-batches and glues them together.
def load_data():
    # Find all parquet files in the folder
    files = glob.glob(DATA_DIR)
    
    if not files:
        return pd.DataFrame()
    
    # Read and combine them
    dfs = [pd.read_parquet(f) for f in files]
    full_df = pd.concat(dfs, ignore_index=True)

    # Convert milliseconds to readable datetime
    full_df['timestamp'] = pd.to_datetime(full_df['timestamp'], unit='ms')
    
    # Sort by time so the chart looks right
    full_df = full_df.sort_values("timestamp")
    return full_df


# The Loop (Auto-Refresh)
placeholder = st.empty()

while True:
    with placeholder.container():
        df = load_data()
        
        if not df.empty:
            # Calculate KPIs
            latest_price = df.iloc[-1]['price']
            start_price = df.iloc[0]['price']
            price_change = latest_price - start_price

            # Show Big Numbers (Metrics)
            kpi1, kpi2, kpi3 = st.columns(3)
            kpi1.metric(
                label="Current Price (USD)", 
                value=f"${latest_price:,.2f}",
                delta=f"{price_change:,.2f}"
            )
            kpi2.metric(
                label="Total Data Points", 
                value=len(df)
            )
            
            st.markdown("### Price Trend (Last 1 Hour)")
            # set 'timestamp' as the index
            st.line_chart(df.set_index("timestamp")['price'])
            
            # Show Raw Data
            with st.expander("View Raw Data"):
                st.dataframe(df.tail(10))
        
        else:
            st.warning("Waiting for data... (Is your Producer/Consumer running?)")

    time.sleep(2)