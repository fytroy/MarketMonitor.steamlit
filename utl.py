import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
import sqlite3

# 1. Connect to Local SQL Server
SERVER_NAME = r'localhost\fyt' 
DATABASE_NAME = 'YahooFinanceDB'
DRIVER = 'ODBC Driver 17 for SQL Server'

print("Reading data from SQL Server...")
params = urllib.parse.quote_plus(f"DRIVER={{{DRIVER}}};SERVER={SERVER_NAME};DATABASE={DATABASE_NAME};Trusted_Connection=yes;")
sql_engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# 2. Fetch Data
try:
    df_market = pd.read_sql("SELECT * FROM MarketData", sql_engine)
    df_options = pd.read_sql("SELECT * FROM Options_Data", sql_engine)
    print(f"Fetched {len(df_market)} market rows and {len(df_options)} option rows.")
except Exception as e:
    print(f"Error reading SQL Server: {e}")
    exit()

# 3. Save to SQLite (The file we will upload to GitHub)
print("Saving to 'MarketData.db' (SQLite)...")
sqlite_conn = sqlite3.connect('MarketData.db')
df_market.to_sql('MarketData', sqlite_conn, if_exists='replace', index=False)
df_options.to_sql('Options_Data', sqlite_conn, if_exists='replace', index=False)
sqlite_conn.close()

print("Success! 'MarketData.db' created. Now upload this file to GitHub.")