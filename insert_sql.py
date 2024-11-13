import yfinance as yf
import pandas as pd
import json
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Retrieve the Postgres connection details from the environment variable
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def getStockDataAndInsert(ticker):
    ticker_symbol = ticker

    # Fetch the last 5 years of data, including hourly data for the most recent month
    msft = yf.Ticker(ticker_symbol)

    # Fetch the past 5 years of daily data (adjusted for closing price)
    historical_data = msft.history(period='5y')

    # Get the most recent date in the dataset
    last_date = historical_data.index[-1]

    # Calculate the date for "the most recent month" (last 30 days)
    recent_month_start = last_date - pd.DateOffset(months=1)

    # Filter data for the last month (hourly data)
    recent_month_data = msft.history(start=recent_month_start, end=last_date, interval='1h')

    # Filter data for the last 4 years and 11 months (daily closing prices)
    four_years_eleven_months_data = historical_data[historical_data.index < recent_month_start]

    # Combine both datasets
    combined_data = pd.concat([four_years_eleven_months_data[['Close']], recent_month_data[['Close']]])

    # Reset index for easier manipulation and SQL insertion
    combined_data.reset_index(inplace=True)

    # Rename columns for clarity
    combined_data.rename(columns={'index': 'timestamp', 'Close': 'price'}, inplace=True)
    print(combined_data)

    conn = None
    # Connect to the PostgreSQL database
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        print("Connected to PostgreSQL successfully!")

        # Prepare the SQL insert statement
        insert_sql = """
            INSERT INTO stock_prices (symbol, timestamp, price)
            VALUES (%s, %s, %s)
        """

        # Insert data into SQL database in batches of 100
        batch_size = 100
        for i in range(0, len(combined_data), batch_size):
            batch = combined_data.iloc[i:i + batch_size]
            records = [(ticker_symbol, row['timestamp'], row['price']) for _, row in batch.iterrows()]
            cursor.executemany(insert_sql, records)
            conn.commit()  # Commit each batch
            print(f"Inserted {len(records)} records into SQL database.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the database connection
        if conn:
            cursor.close()
            conn.close()

# List of securities to process
securities = [
    'MSFT', 'SPY', 'AAPL', 'BRK-B', 'ORCL', 'VOO', 'QQQ', 'META', 'XOM', 'GOOG',
    'LLY', 'IVV', 'AVGO', 'TSLA', 'WMT', 'JPM', 'NVDA', 'MA', 'COST', 'HD', 'PG',
    'UNH', 'VTI', 'AMZN', 'V'
]

for ticker in securities:
    getStockDataAndInsert(ticker)