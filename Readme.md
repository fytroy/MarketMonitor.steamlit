📈 Yahoo Finance ETL & Market Monitor

A full-stack data engineering project that ingests live financial data, stores it in SQL Server, and visualizes it in a real-time trading terminal.

🏗️ Architecture

Source: Yahoo Finance API (yfinance)

ETL: Python (Pandas, SQLAlchemy)

Database: Microsoft SQL Server (ODBC Driver 17)

Frontend: Streamlit + Plotly

🚀 How to Run

Double-click LAUNCH_SYSTEM.bat to start all services.

Manual Startup

Price Feeds: python etl.py

Options Chain: python etl2.py

UI: python -m streamlit run dashboard2.py

📊 Features

Live Market Data: Tracks Top 15 assets across Stocks, Crypto, Indices, Forex, and Treasury.

Options Analysis: Visualizes Volatility Smile and Option Chains for major tickers (AAPL, NVDA, SPY, etc.).

Technical Analysis: Real-time 20-Period SMA overlay.

Auto-Refresh: Dashboard updates every 15 seconds; Database updates every 15 minutes.

🛠️ Requirements

Python 3.8+

SQL Server (Express or Developer)

Libraries: yfinance, pandas, sqlalchemy, pyodbc, streamlit, plotly