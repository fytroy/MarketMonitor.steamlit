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
def load_market_data(asset_type):
    if not engine: return pd.DataFrame()
    query = f"SELECT * FROM MarketData WHERE Asset_Type = '{asset_type}' ORDER BY Date ASC"
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Query Error: {e}")
        return pd.DataFrame()

def load_options_data(ticker):
    if not engine: return pd.DataFrame()
    # Fetch all options for this ticker, sort by time
    query = f"SELECT * FROM Options_Data WHERE Underlying_Ticker = '{ticker}' ORDER BY Last_Updated ASC"
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        # Keep only the latest snapshot for each contract symbol
        if not df.empty:
            df = df.sort_values('Last_Updated').drop_duplicates(subset=['Contract_Symbol'], keep='last')
        return df
    except Exception as e:
        st.error(f"Options Query Error: {e}")
        return pd.DataFrame()

# ==========================================
# 3. SIDEBAR CONTROLS
# ==========================================
st.sidebar.header("🕹️ Control Panel")

# --- NEW: MODE SELECTOR ---
dashboard_mode = st.sidebar.radio("Dashboard Mode", ["Live Market", "Options Chain"], index=0)
st.sidebar.markdown("---")

# ==========================================
# 4. MODE A: LIVE MARKET (Existing Logic)
# ==========================================
if dashboard_mode == "Live Market":
    
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
    df = load_market_data(selected_asset)

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

        # --- DASHBOARD UI ---
        st.title(f"💹 {selected_asset} Live Terminal")

        if selected_tickers:
            filtered_df = df[df['Ticker'].isin(selected_tickers)].copy()
            
            # KPI ROW
            cols = st.columns(min(len(selected_tickers), 4))
            for i, ticker in enumerate(selected_tickers[:4]):
                ticker_df = filtered_df[filtered_df['Ticker'] == ticker]
                if not ticker_df.empty:
                    latest = ticker_df.iloc[-1]
                    start = ticker_df.iloc[0]
                    price = latest['Close']
                    change = ((price - start['Open']) / start['Open']) * 100
                    with cols[i]:
                        st.metric(label=ticker, value=f"${price:,.2f}", delta=f"{change:.2f}% (1d)")

            # CHART AREA
            st.markdown("### Price Action")
            if chart_type == "Line":
                plot_df = filtered_df.copy()
                plot_df['SMA_20'] = plot_df.groupby('Ticker')['Close'].transform(lambda x: x.rolling(window=20).mean())

                if normalize:
                    normalized_data = []
                    for ticker in selected_tickers:
                        t_df = plot_df[plot_df['Ticker'] == ticker].copy()
                        start_price = t_df.iloc[0]['Open']
                        t_df['Rel_Performance'] = ((t_df['Close'] - start_price) / start_price) * 100
                        normalized_data.append(t_df)
                    
                    if normalized_data:
                        plot_df = pd.concat(normalized_data)
                        fig = px.line(plot_df, x='Date', y='Rel_Performance', color='Ticker', height=500)
                    else:
                        fig = go.Figure()
                else:
                    fig = px.line(plot_df, x='Date', y='Close', color='Ticker', height=500)
                    if show_sma:
                        for ticker in selected_tickers:
                            t_data = plot_df[plot_df['Ticker'] == ticker]
                            fig.add_scatter(x=t_data['Date'], y=t_data['SMA_20'], mode='lines', 
                                          name=f"{ticker} SMA", line=dict(width=1, dash='dot'), opacity=0.7)

                fig.update_layout(hovermode="x unified", template="plotly_dark")
                st.plotly_chart(fig, use_container_width=True)

            else:
                # CANDLESTICK
                primary = selected_tickers[0]
                if len(selected_tickers) > 1: st.info(f"Showing Candlestick for: {primary}")
                candle_data = filtered_df[filtered_df['Ticker'] == primary]
                fig = go.Figure(data=[go.Candlestick(x=candle_data['Date'], open=candle_data['Open'], 
                                                   high=candle_data['High'], low=candle_data['Low'], close=candle_data['Close'])])
                fig.update_layout(title=f"{primary} Price Action", height=500, template="plotly_dark", xaxis_rangeslider_visible=False)
                st.plotly_chart(fig, use_container_width=True)

            with st.expander("📂 View Underlying Data Grid"):
                st.dataframe(filtered_df.sort_values(by=['Date', 'Ticker'], ascending=[False, True]), use_container_width=True)

        if auto_refresh:
            time.sleep(15)
            st.rerun()

# ==========================================
# 5. MODE B: OPTIONS CHAIN
# ==========================================
else: # Options Mode
    st.title("⛓️ Options Chain Viewer")
    
    # Fetch available tickers from Options Table
    if engine:
        try:
            opt_tickers = pd.read_sql("SELECT DISTINCT Underlying_Ticker FROM Options_Data", engine)
            opt_list = opt_tickers['Underlying_Ticker'].tolist()
        except:
            opt_list = []
    else:
        opt_list = []

    if not opt_list:
        st.warning("No Options data found. Please run 'etl_options.py' to fetch data.")
    else:
        # Selector
        target_ticker = st.selectbox("Select Underlying Asset", opt_list)
        
        # Load Data
        df_opt = load_options_data(target_ticker)
        
        if not df_opt.empty:
            # Stats
            expiry_date = df_opt['Expiry'].iloc[0] # Assuming one expiry per fetch for now
            last_updated = df_opt['Last_Updated'].max()
            
            c1, c2, c3 = st.columns(3)
            c1.metric("Underlying", target_ticker)
            c2.metric("Expiry Date", str(expiry_date))
            c3.metric("Last Updated", str(last_updated.strftime('%H:%M:%S')))
            
            # --- VOLATILITY SMILE CHART ---
            st.subheader("Volatility Smile (IV vs Strike)")
            
            fig = px.scatter(
                df_opt, 
                x='Strike', 
                y='Implied_Volatility', 
                color='Type', 
                title=f"{target_ticker} Implied Volatility",
                color_discrete_map={'Call': '#00CC96', 'Put': '#EF553B'},
                hover_data=['Last_Price', 'Contract_Symbol']
            )
            fig.update_traces(marker=dict(size=8, opacity=0.7))
            fig.update_layout(template="plotly_dark", height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # --- DATA TABLES (Split View) ---
            st.subheader("Chain Details")
            
            col_calls, col_puts = st.columns(2)
            
            with col_calls:
                st.markdown("#### 🟢 Calls")
                calls = df_opt[df_opt['Type'] == 'Call'].sort_values('Strike')
                st.dataframe(calls[['Strike', 'Last_Price', 'Implied_Volatility']], use_container_width=True, hide_index=True)
                
            with col_puts:
                st.markdown("#### 🔴 Puts")
                puts = df_opt[df_opt['Type'] == 'Put'].sort_values('Strike')
                st.dataframe(puts[['Strike', 'Last_Price', 'Implied_Volatility']], use_container_width=True, hide_index=True)