import streamlit as st
import pandas as pd
import glob
import time
import duckdb
import plotly.express as px
import uuid
from datetime import timedelta
import plotly.graph_objects as go
from statsmodels.tsa.arima.model import ARIMA


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

@st.cache_data(ttl=3600)

def generate_forecast(df):
    if len(df) < 14:
        return pd.DataFrame() 
    
    train_df = df.sort_values("metric_date").reset_index(drop=True)
    prices = train_df["close_price"].values

    try:
        # Train ARIMA Model (Auto-Regressive Integrated Moving Average)

        model = ARIMA(prices, order=(5, 1, 0))
        model_fit = model.fit()

        #Predict next 7 days
        forecast = model_fit.forecast(steps=7)

        #Create Future Dates
        last_date = train_df['metric_date'].iloc[-1]
        future_dates = [last_date + timedelta(days=i) for i in range(1, 8)]

        forecast_df = pd.DataFrame({
            'metric_date': pd.to_datetime(future_dates),
            'forecast_price': forecast
        })

        return forecast_df
    
    except Exception as e:
        print(f"ML Error: {e}")
        return pd.DataFrame()

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
        # Generate ML predictions
        forecast_df = generate_forecast(hist_df)
    except Exception as e:
        hist_df = pd.DataFrame()
        forecast_df = pd.DataFrame()
        warehouse_status = f"Offline ({str(e)})"

    with placeholder.container():

        col1, col2 = st.columns(2)
        # THE TRADER VIEW (Real-Time)
        with col1:
            st.header(f"Live Market (Speed Layer)")
            if not live_df.empty:
                latest = live_df.iloc[-1]
                st.metric("Live Price", f"${latest['price']:,.2f}")
                
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=live_df['timestamp'], y=live_df['price'],
                                         mode='lines', name='Live Price', line=dict(color='#00b4d8')))
                fig.update_layout(title='Last Hour Volatility', height=400, template="plotly_dark")
                
                st.plotly_chart(fig, use_container_width=True, key=f"live_chart_{uuid.uuid4()}")
            else:
                st.warning("Waiting for live data...")

        # THE RESEARCH LAYER (ML Forecast)
        with col2:
            st.header(f"🧠 ML Forecast (Batch Layer)")
            st.caption(f"Warehouse Status: {warehouse_status} | Model: ARIMA(5,1,0)")
            
            if not hist_df.empty:
                latest_trend = hist_df.iloc[0]
                
                fig2 = go.Figure()
                
                #Plot Actual Historical Price
                fig2.add_trace(go.Scatter(x=hist_df['metric_date'], y=hist_df['close_price'],
                                          mode='lines', name='Actual Price', line=dict(color='#4361ee')))
                
                #Plot Moving Average
                fig2.add_trace(go.Scatter(x=hist_df['metric_date'], y=hist_df['moving_avg_7d'],
                                          mode='lines', name='7-Day MA', line=dict(color='#fca311')))
                #Plot ML Forecast
                if not forecast_df.empty:
                    # Connect the last actual point to the first forecast point so the line doesn't break
                    connect_x = [hist_df['metric_date'].iloc[0], forecast_df['metric_date'].iloc[0]]
                    connect_y = [hist_df['close_price'].iloc[0], forecast_df['forecast_price'].iloc[0]]
                    fig2.add_trace(go.Scatter(x=connect_x, y=connect_y, mode='lines', showlegend=False, line=dict(color='#ef233c', dash='dash')))
                    
                    # Plot the rest of the forecast
                    fig2.add_trace(go.Scatter(x=forecast_df['metric_date'], y=forecast_df['forecast_price'],
                                              mode='lines', name='7-Day Prediction', line=dict(color='#ef233c', dash='dash')))
                    
                    next_day_pred = forecast_df.iloc[0]['forecast_price']

                    st.metric(label="Predicted Price (Tomorrow)", value=f"${next_day_pred:,.2f}", 
                              delta=f"${next_day_pred - latest_trend['close_price']:,.2f} from today")

                fig2.update_layout(title='30-Day Trend & Machine Learning Projection', height=400, template="plotly_dark")
                st.plotly_chart(fig2, use_container_width=True, key=f"history_chart_{uuid.uuid4()}")

            else:
                st.info("Run 'dbt run' to populate the warehouse!")

    time.sleep(2)