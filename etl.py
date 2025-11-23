import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
from datetime import datetime
import time  # Added for the loop delay

# ==========================================
# 1. CONFIGURATION: TOP 15 ASSETS
# ==========================================
assets = {
    'Stocks': [
        'AAPL', 'NVDA', 'MSFT', 'GOOG', 'AMZN',  # Tech Giants
        'META', 'TSLA', 'BRK-B', 'LLY', 'AVGO',  # High Cap
        'JPM', 'V', 'XOM', 'UNH', 'WMT'          # Finance/Retail/Energy
    ],
    'Crypto': [
        'BTC-USD', 'ETH-USD', 'BNB-USD', 'SOL-USD', 'XRP-USD', 
        'DOGE-USD', 'ADA-USD', 'TRX-USD', 'AVAX-USD', 'SHIB-USD', 
        'LINK-USD', 'BCH-USD', 'DOT-USD', 'LTC-USD', 'NEAR-USD'
    ],
    'Indices': [
        '^GSPC', '^DJI', '^IXIC', '^RUT',        # US Major
        '^FTSE', '^N225', '^GDAXI', '^FCHI',     # Global
        '^HSI', '000001.SS', '^BVSP', '^AXJO',   
        '^STOXX50E', '^KS11', '^TWII'            
    ],
    'Currencies': [
        'EURUSD=X', 'JPY=X', 'GBPUSD=X', 'AUDUSD=X', 'NZDUSD=X', 
        'USDCAD=X', 'USDCHF=X', 'EURJPY=X', 'GBPJPY=X', 'EURGBP=X', 
        'AUDJPY=X', 'EURCHF=X', 'USDCNY=X', 'USDHKD=X', 'USDSGD=X'
    ],
    'Treasury': [
        '^TNX', '^TYX', '^FVX', '^IRX',          
        'TLT', 'IEF', 'SHY', 'BIL', 'GOVT',      
        'AGG', 'BND', 'VGIT', 'SCHO', 'SPTL', 'EDV' 
    ]
}

# ==========================================
# 2. SSMS DATABASE CONNECTION
# ==========================================

# !!! REMINDER: UPDATE THIS TO THE NAME THAT WORKED FOR YOU !!!
SERVER_NAME = r'localhost\fyt' 

DATABASE_NAME = 'YahooFinanceDB'
DRIVER = 'ODBC Driver 17 for SQL Server'

try:
    params = urllib.parse.quote_plus(
        f"DRIVER={{{DRIVER}}};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};Trusted_Connection=yes;"
    )
    connection_string = f"mssql+pyodbc:///?odbc_connect={params}"
    engine = create_engine(connection_string, fast_executemany=True)
except Exception as e:
    print(f"Configuration Error: {e}")

# ==========================================
# 3. ETL LOGIC
# ==========================================

def fetch_and_load(asset_dict):
    print(f"--- Starting ETL Job at {datetime.now()} ---")
    
    all_data = []

    for category, tickers in asset_dict.items():
        print(f"Fetching data for: {category}...")
        
        try:
            # Download batch data: 1 Day period, 15 Minute interval
            raw_data = yf.download(tickers, period="1d", interval="15m", group_by='ticker', auto_adjust=True, progress=False)
            
            if raw_data.empty:
                print(f"No data found for {category}")
                continue

            for ticker in tickers:
                try:
                    # Handle yfinance multi-index structure
                    if len(tickers) > 1:
                        if ticker not in raw_data.columns.levels[0]:
                            continue
                        df = raw_data[ticker].copy()
                    else:
                        df = raw_data.copy()

                    if df.empty:
                        continue
                    
                    # Standardize Date Column
                    df = df.reset_index()
                    col_map = {'Datetime': 'Date', 'index': 'Date'}
                    df.rename(columns=col_map, inplace=True)

                    # --- CRITICAL FIX START ---
                    # Remove Timezone info to prevent SQL "String data, right truncation" error
                    if 'Date' in df.columns and pd.api.types.is_datetime64_any_dtype(df['Date']):
                        df['Date'] = df['Date'].dt.tz_localize(None)
                    # --- CRITICAL FIX END ---
                    
                    df['Ticker'] = ticker
                    df['Asset_Type'] = category
                    df['Last_Updated'] = datetime.now()
                    
                    df.rename(columns={
                        'Open': 'Open', 'High': 'High', 'Low': 'Low', 'Close': 'Close'
                    }, inplace=True)

                    cols_to_keep = ['Ticker', 'Asset_Type', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Last_Updated']
                    
                    for col in cols_to_keep:
                        if col not in df.columns:
                            df[col] = None
                            
                    df = df[cols_to_keep]
                    df.dropna(subset=['Close'], inplace=True)
                    all_data.append(df)

                except Exception as e:
                    print(f"Error processing ticker {ticker}: {e}")

        except Exception as e:
            print(f"Batch download error for {category}: {e}")

    # ==========================================
    # 4. LOAD TO SQL SERVER
    # ==========================================
    if all_data:
        final_df = pd.concat(all_data)
        print(f"Uploading {len(final_df)} rows to SQL Server...")
        
        try:
            final_df.to_sql('MarketData', engine, if_exists='append', index=False)
            print("Success! Data loaded.")
        except Exception as e:
            print(f"SQL Connection Error: {e}")
            print("\n*** TROUBLESHOOTING ***")
            print("1. If you get 'String data, right truncation', verify the Timezone fix code block is present.")
            print("2. Ensure columns in SQL Match columns in Python exactly.")
    else:
        print("No data fetched to upload.")

# ==========================================
# 5. MAIN LOOP (RUNS FOREVER)
# ==========================================
if __name__ == "__main__":
    while True:
        try:
            fetch_and_load(assets)
        except Exception as e:
            print(f"Critical Job Failure: {e}")
        
        print("Sleeping for 15 minutes... (Press Ctrl+C to stop)")
        time.sleep(900) # 900 seconds = 15 minutes