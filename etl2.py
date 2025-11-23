import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
from datetime import datetime
import time

# ==========================================
# 1. CONFIGURATION
# ==========================================
# We focus on the most active Option Chains to keep performance high
TARGET_TICKERS = ['AAPL', 'NVDA', 'TSLA', 'SPY', 'QQQ', 'MSFT', 'AMZN']

# ==========================================
# 2. DATABASE CONNECTION
# ==========================================
# Update this to match your working server name from the previous script
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
# 3. OPTIONS FETCHING LOGIC
# ==========================================
def fetch_options(tickers):
    print(f"\n--- Starting Options Scan at {datetime.now().strftime('%H:%M:%S')} ---")
    all_options = []

    for ticker_symbol in tickers:
        try:
            print(f"Checking Options for {ticker_symbol}...")
            tk = yf.Ticker(ticker_symbol)
            
            # Get available expiration dates
            expirations = tk.options
            if not expirations:
                print(f"  > No options found for {ticker_symbol}")
                continue
                
            # STRATEGY: Only fetch the NEAREST expiry date (Most Liquid / Active)
            # Fetching all dates takes too long and creates massive data
            target_date = expirations[0] 
            print(f"  > Fetching chain for nearest expiry: {target_date}")
            
            # Get the chain
            chain = tk.option_chain(target_date)
            
            # Process CALLS
            calls = chain.calls.copy()
            calls['Type'] = 'Call'
            
            # Process PUTS
            puts = chain.puts.copy()
            puts['Type'] = 'Put'
            
            # Combine
            df = pd.concat([calls, puts])
            
            # Clean and Format for SQL
            df['Underlying_Ticker'] = ticker_symbol
            df['Expiry'] = target_date
            df['Last_Updated'] = datetime.now()
            
            # Map YFinance columns to SQL Columns
            # YF: contractSymbol, strike, lastPrice, impliedVolatility
            df = df.rename(columns={
                'contractSymbol': 'Contract_Symbol',
                'strike': 'Strike',
                'lastPrice': 'Last_Price',
                'impliedVolatility': 'Implied_Volatility'
            })
            
            # Select only columns that match our SQL Table
            cols_to_keep = ['Underlying_Ticker', 'Contract_Symbol', 'Type', 'Strike', 'Expiry', 'Last_Price', 'Implied_Volatility', 'Last_Updated']
            
            # Ensure columns exist
            for c in cols_to_keep:
                if c not in df.columns:
                    df[c] = None
            
            df = df[cols_to_keep]
            all_options.append(df)
            
        except Exception as e:
            print(f"  > Error fetching {ticker_symbol}: {e}")

    # ==========================================
    # 4. LOAD TO DATABASE
    # ==========================================
    if all_options:
        final_df = pd.concat(all_options)
        print(f"Uploading {len(final_df)} option contracts to SQL Server...")
        
        try:
            # We use 'append' here. In a real production app for options, 
            # you often want to clear old data or use a 'Snapshot_Time' column.
            # For this demo, we append to build history.
            final_df.to_sql('Options_Data', engine, if_exists='append', index=False)
            print("Success! Options loaded.")
        except Exception as e:
            print(f"SQL Error: {e}")
    else:
        print("No options data retrieved.")

if __name__ == "__main__":
    # Run once immediately
    fetch_options(TARGET_TICKERS)
    
    # Optional: Loop automatically
    while True:
        print("Sleeping for 30 minutes (Options update slower than prices)...")
        time.sleep(1800) # 30 mins
        fetch_options(TARGET_TICKERS)