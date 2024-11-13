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

# Data to insert
data = [
    {
        "name": "NVIDIA",
        "symbol": "NVDA",
        "attributes": {"market_cap": 3606000},
        "type": "stock"
    },
    {
        "name": "Apple",
        "symbol": "AAPL",
        "attributes": {"market_cap": 3379000},
        "type": "stock"
    },
    {
        "name": "Microsoft",
        "symbol": "MSFT",
        "attributes": {"market_cap": 3126000},
        "type": "stock"
    },
    {
        "name": "Alphabet (Google)",
        "symbol": "GOOG",
        "attributes": {"market_cap": 2220000},
        "type": "stock"
    },
    {
        "name": "Amazon",
        "symbol": "AMZN",
        "attributes": {"market_cap": 2184000},
        "type": "stock"
    },
    {
        "name": "Meta Platforms (Facebook)",
        "symbol": "META",
        "attributes": {"market_cap": 1471000},
        "type": "stock"
    },
    {
        "name": "Tesla",
        "symbol": "TSLA",
        "attributes": {"market_cap": 1054000},
        "type": "stock"
    },
    {
        "name": "Berkshire Hathaway",
        "symbol": "BRK-B",
        "attributes": {"market_cap": 1006000},
        "type": "stock"
    },
    {
        "name": "Broadcom",
        "symbol": "AVGO",
        "attributes": {"market_cap": 812000},
        "type": "stock"
    },
    {
        "name": "Eli Lilly",
        "symbol": "LLY",
        "attributes": {"market_cap": 771000},
        "type": "stock"
    },
    {
        "name": "Walmart",
        "symbol": "WMT",
        "attributes": {"market_cap": 683000},
        "type": "stock"
    },
    {
        "name": "JPMorgan Chase",
        "symbol": "JPM",
        "attributes": {"market_cap": 672000},
        "type": "stock"
    },
    {
        "name": "Visa",
        "symbol": "V",
        "attributes": {"market_cap": 598000},
        "type": "stock"
    },
    {
        "name": "UnitedHealth",
        "symbol": "UNH",
        "attributes": {"market_cap": 567000, "is_esg": True},
        "type": "stock"
    },
    {
        "name": "Exxon Mobil",
        "symbol": "XOM",
        "attributes": {"market_cap": 529000},
        "type": "stock"
    },
    {
        "name": "Oracle",
        "symbol": "ORCL",
        "attributes": {"market_cap": 522000},
        "type": "stock"
    },
    {
        "name": "Mastercard",
        "symbol": "MA",
        "attributes": {"market_cap": 484000},
        "type": "stock"
    },
    {
        "name": "Costco",
        "symbol": "COST",
        "attributes": {"market_cap": 412000},
        "type": "stock"
    },
    {
        "name": "Home Depot",
        "symbol": "HD",
        "attributes": {"market_cap": 401000},
        "type": "stock"
    },
    {
        "name": "Procter & Gamble",
        "symbol": "PG",
        "attributes": {"market_cap": 390000, "is_esg": True},
        "type": "stock"
    },
    {
        "symbol": "SPY",
        "name": "SPDR S&P 500 ETF Trust",
        "attributes": {
        "aum": 619519000
        },
        "type": "ETF"
    },
    {
        "symbol": "VOO",
        "name": "Vanguard S&P 500 ETF",
        "attributes": {
        "aum": 562998000
        },
        "type": "ETF"
    },
    {
        "symbol": "IVV",
        "name": "iShares Core S&P 500 ETF",
        "attributes": {
        "aum": 561727000
        },
        "type": "ETF"
    },
    {
        "symbol": "VTI",
        "name": "Vanguard Total Stock Market ETF",
        "attributes": {
        "aum": 456167000
        },
        "type": "ETF"
    },
    {
        "symbol": "QQQ",
        "name": "Invesco QQQ Trust Series I",
        "attributes": {
        "aum": 306101000,
        "is_esg":True
        },
        "type": "ETF"
    },
    {
        "symbol": "BTC",
        "name": "Bitcoin",
        "attributes": {
            "proof":"work",
            "jimCramerFavorite":True
        },
        "type": "Crypto"
    }
]

# Establish PostgreSQL connection
try:
    # Create a connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST
    )
    # Create a cursor object
    cursor = conn.cursor()
    
    # Ensure the table exists, create if it doesn't
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS securities (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        symbol VARCHAR(10) UNIQUE NOT NULL,
        type VARCHAR(50)
    );
    '''
    cursor.execute(create_table_query)
    
    # Insert the data into the securities table
    for record in data:
        # Extract fields for insertion
        name = record["name"]
        symbol = record["symbol"]
        type = record["type"]
        
        # Insert data into PostgreSQL
        insert_query = '''
        INSERT INTO securities (name, symbol, type)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol) DO NOTHING;
        '''
        cursor.execute(insert_query, (name, symbol, type))
    
    # Commit the transaction
    conn.commit()
    
    print(f"Successfully inserted {len(data)} records into the 'securities' table.")
    
except Exception as e:
    print(f"An error occurred: {e}")
    
finally:
    # Close the cursor and connection
    cursor.close()
    conn.close()
