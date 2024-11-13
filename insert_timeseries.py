import random
import time
from datetime import datetime, timedelta
from pymongo import MongoClient
import pytz

# Mock function to simulate fetching stock price (you would replace this with an actual API call)
def get_stock_price(symbol, previous_price):
    # In a real case, this would fetch the actual stock price, here we just generate a random value
    lower_bound = previous_price * 0.98
    upper_bound = previous_price * 1.03
    return round(random.uniform(lower_bound, upper_bound), 2)  # Random price between 100 and 500

# Function to generate a datetime in the past (within a specified range in years)
def generate_past_timestamp(years_range, interval_minutes):
    # Get the current time in UTC
    now = datetime.now(pytz.utc)
    
    # Calculate the maximum range (in days)
    max_delta = timedelta(days=365 * years_range)
    
    # Subtract the random number of seconds from the current time to get a random past time
    past_time = now - timedelta(minutes=interval_minutes)
    
    # Return the datetime object (which BSON can handle)
    return past_time

# Function to generate a list of objects with timestamp and stock price
def generate_stock_data(symbol, initial_price, num_samples):
    stock_data = []
    previous_price = initial_price  # Initial price (can be modified as needed)
    time_frequency = 5 #generate entries every X minutes

    for _ in range(num_samples):
        #Get the time interval
        interval_minutes = _ * time_frequency

        # Get a random timestamp from the past 2 years
        timestamp = generate_past_timestamp(2, interval_minutes)
        
        # Get the stock price (using the mock function here)
        price = get_stock_price(symbol, previous_price)
        
        # Store the previous price for the next iteration
        previous_price = price
        
        # Create an object (dict in this case) with timestamp, price, and symbol as meta field
        stock_data.append({
            'timestamp': timestamp, 
            'price': price,
            'symbol': symbol  # Symbol as meta field to group the data
        })
        # Simulate waiting for a minute before getting the next price (for demonstration purposes)
        #time.sleep(1)  # Adjust sleep time for actual time intervals you want
        
    return stock_data

# MongoDB connection setup and time series collection creation
def create_time_series_collection(db, symbol):
    # Check if the collection exists, if not, create it as a time series collection
    collection_name = "timeSeries"
    if collection_name not in db.list_collection_names():
        # Create the collection with time series options
        db.create_collection(
            collection_name,
            timeseries={
                'timeField': 'timestamp',  # The time field in the document
                'metaField': 'symbol',  # Meta field to group the time series data
                'granularity': 'seconds'  # Granularity of time (adjust as needed)
            }
        )
        print(f"Created time series collection: {collection_name}")
    else:
        print(f"Time series collection '{collection_name}' already exists.")
    return db[collection_name]

# Function to insert data into MongoDB time series collection
def insert_data_into_mongo(stock_data, symbol):
    # Replace 'localhost' with the MongoDB server address if it's hosted remotely
    client = MongoClient('mongodb+srv://main_user_pov:Password123@cluster1.gj7nm.mongodb.net/?retryWrites=true&w=majority&appName=Cluster1')
    
    # Select the database (create it if it doesn't exist)
    db = client['mongoFi']
    
    # Create or get the time series collection for the symbol
    collection = create_time_series_collection(db, symbol)

    # Insert the data into MongoDB time series collection
    if stock_data:
        collection.insert_many(stock_data)
        print(f"Inserted {len(stock_data)} records into MongoDB time series collection '{symbol}_time_series'")
    else:
        print("No data to insert.")

# Example usage
symbol = 'AAPL'  # You can replace this with any ticker symbol
num_samples = 1000  # Number of data points to generate (there are 105,120 5-minute intervals in a year)
initial_price = 100.0

# Generate the stock data
stock_data = generate_stock_data(symbol, initial_price, num_samples )

# Insert the generated data into MongoDB
insert_data_into_mongo(stock_data, symbol)
