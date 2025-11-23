@echo off
TITLE Market Monitor System Launcher

echo ==================================================
echo   STARTING FINANCIAL DATA PIPELINE
echo ==================================================
echo.

:: 1. Start Price ETL in a new window
echo 1. Launching Price ETL (15m Interval)...
start "Price ETL Worker" cmd /k "python etl.py"

:: 2. Start Options ETL in a new window
echo 2. Launching Options ETL (30m Interval)...
start "Options ETL Worker" cmd /k "python etl2.py"

:: 3. Start Dashboard in a new window
echo 3. Launching Streamlit Dashboard...
start "Market Dashboard" cmd /k "python -m streamlit run dashboard2.py"

echo.
echo ==================================================
echo   SYSTEM IS RUNNING
echo   - Window 1: Fetching Prices
echo   - Window 2: Fetching Options
echo   - Window 3: Hosting Dashboard
echo.
echo   Do not close the worker windows!
echo ==================================================
pause