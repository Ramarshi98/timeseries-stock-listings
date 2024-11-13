import psycopg2
from psycopg2 import sql
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

# SQL query to get the most recent entry for each symbol
query = """
SELECT DISTINCT ON (symbol) symbol, timestamp, price
FROM stock_prices
ORDER BY symbol, timestamp DESC;
"""

# Establishing the connection to the PostgreSQL database
try:
    # Connect to the database
    conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

    # Create a cursor object
    cur = conn.cursor()

    # Execute the query
    cur.execute(query)

    # Fetch all results
    rows = cur.fetchall()

    # Print the results
    for row in rows:
        print(f"symbol: {row[0]}, timestamp: {row[1]}, price: {row[2]}")

    # Close the cursor and the connection
    cur.close()
    conn.close()

except Exception as e:
    print(f"Error: {e}")
