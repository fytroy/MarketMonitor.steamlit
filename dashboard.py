import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
import plotly.express as px
import plotly.graph_objects as go
import time

# ==========================================
# 1. SETUP & CONNECTION
# ==========================================
st.set_page_config(page_title="Market Terminal", layout="wide", page_icon="💹")

# Use the same server name that worked for you
SERVER_NAME = r'localhost\fyt' 
DATABASE_NAME = 'YahooFinanceDB'
DRIVER = 'ODBC Driver 17 for SQL Server'

@st.cache_resource
def get_connection():
    try:
        params = urllib.parse.quote_plus(
            f"DRIVER={{{DRIVER}}};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};Trusted_Connection=yes;"
        )
        connection_string = f"mssql+pyodbc:///?odbc_connect={params}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        st.error(f"SQL Connection Failed: {e}")
        return None

engine = get_connection()

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def load_data(asset_type):
    if not engine: return pd.DataFrame()
    
    query = f"""
    SELECT * FROM MarketData 
    WHERE Asset_Type = '{asset_type}' 
    ORDER BY Date ASC
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Query Error: {e}")
        return pd.DataFrame()

# ==========================================
# 3. SIDEBAR CONTROLS
# ==========================================
st.sidebar.header("🕹️ Control Panel")

# Asset Selection
if engine:
    try:
        asset_types = pd.read_sql("SELECT DISTINCT Asset_Type FROM MarketData", engine)
        asset_list = asset_types['Asset_Type'].tolist()
    except:
        asset_list = ["Stocks", "Crypto", "Forex"]
else:
    asset_list = []

selected_asset = st.sidebar.selectbox("Asset Class", asset_list, index=0)
df = load_data(selected_asset)

if not df.empty:
    all_tickers = df['Ticker'].unique().tolist()
    default_tickers = all_tickers[:3] if len(all_tickers) >= 3 else all_tickers
    
    selected_tickers = st.sidebar.multiselect("Select Tickers", all_tickers, default=default_tickers)
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("⚙️ Chart Settings")
    chart_type = st.sidebar.radio("Chart Type", ["Line", "Candlestick"], horizontal=True)
    
    normalize = False
    show_sma = False
    
    if chart_type == "Line":
        normalize = st.sidebar.checkbox("Normalize (%)", value=True, help="Compare performance starting at 0%")
        if not normalize:
            show_sma = st.sidebar.checkbox("Show SMA (20)", value=True, help="Show 20-period Simple Moving Average")

    st.sidebar.markdown("---")
    auto_refresh = st.sidebar.checkbox("🔴 Live Auto-Refresh (15s)")

# ==========================================
# 4. DASHBOARD UI
# ==========================================
st.title(f"💹 {selected_asset} Live Terminal")

if not df.empty and selected_tickers:
    # Filter Data
    filtered_df = df[df['Ticker'].isin(selected_tickers)].copy()
    
    # --- TOP KPI ROW ---
    cols = st.columns(min(len(selected_tickers), 4)) # Max 4 columns
    
    for i, ticker in enumerate(selected_tickers[:4]):
        ticker_df = filtered_df[filtered_df['Ticker'] == ticker]
        if not ticker_df.empty:
            latest = ticker_df.iloc[-1]
            start = ticker_df.iloc[0]
            
            price = latest['Close']
            change = ((price - start['Open']) / start['Open']) * 100
            
            with cols[i]:
                st.metric(
                    label=ticker,
                    value=f"${price:,.2f}",
                    delta=f"{change:.2f}% (1d)"
                )

    # --- MAIN CHART AREA ---
    st.markdown("### Price Action")
    
    if chart_type == "Line":
        # LINE CHART LOGIC
        plot_df = filtered_df.copy()
        
        # Calculate SMA-20 for Technical Analysis
        plot_df['SMA_20'] = plot_df.groupby('Ticker')['Close'].transform(lambda x: x.rolling(window=20).mean())

        if normalize:
            # Normalize Logic
            normalized_data = []
            for ticker in selected_tickers:
                t_df = plot_df[plot_df['Ticker'] == ticker].copy()
                start_price = t_df.iloc[0]['Open']
                t_df['Rel_Performance'] = ((t_df['Close'] - start_price) / start_price) * 100
                normalized_data.append(t_df)
            plot_df = pd.concat(normalized_data)
            y_axis = 'Rel_Performance'
            y_title = "Performance (%)"
            
            fig = px.line(plot_df, x='Date', y=y_axis, color='Ticker', height=500)
            
        else:
            # Standard Price Logic with Optional SMA
            y_axis = 'Close'
            y_title = "Price ($)"
            
            fig = px.line(plot_df, x='Date', y=y_axis, color='Ticker', height=500)
            
            if show_sma:
                # Add SMA lines as dashed lines
                # We iterate tickers to add an SMA trace for each
                colors = px.colors.qualitative.Plotly
                for i, ticker in enumerate(selected_tickers):
                    t_data = plot_df[plot_df['Ticker'] == ticker]
                    fig.add_scatter(
                        x=t_data['Date'], 
                        y=t_data['SMA_20'], 
                        mode='lines',
                        name=f"{ticker} SMA",
                        line=dict(width=1, dash='dot')
                    )

        fig.update_yaxes(title=y_title)
        fig.update_layout(hovermode="x unified", template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)

    else:
        # CANDLESTICK LOGIC
        if len(selected_tickers) > 0:
            primary_ticker = selected_tickers[0]
            if len(selected_tickers) > 1:
                st.info(f"Showing Candlestick for: **{primary_ticker}** (Select only one ticker for best view)")
            
            candle_data = filtered_df[filtered_df['Ticker'] == primary_ticker]
            
            fig = go.Figure(data=[go.Candlestick(
                x=candle_data['Date'],
                open=candle_data['Open'],
                high=candle_data['High'],
                low=candle_data['Low'],
                close=candle_data['Close']
            )])
            fig.update_layout(
                title=f"{primary_ticker} Price Action", 
                height=500, 
                template="plotly_dark",
                xaxis_rangeslider_visible=False
            )
            st.plotly_chart(fig, use_container_width=True)

    # --- RAW DATA EXPANDER ---
    with st.expander("📂 View Underlying Data Grid"):
        st.dataframe(
            filtered_df.sort_values(by=['Date', 'Ticker'], ascending=[False, True]),
            use_container_width=True
        )

else:
    st.warning("Waiting for data... Ensure your ETL script is running.")

# ==========================================
# 5. AUTO REFRESH LOGIC
# ==========================================
if auto_refresh:
    time.sleep(15) # Wait 15 seconds
    st.rerun()     # Force reload