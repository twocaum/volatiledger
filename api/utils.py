import os
import pandas as pd
from pymongo import ASCENDING, MongoClient
from dotenv import load_dotenv
from datetime import datetime
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load environment variables from .env file
load_dotenv()

# Binance API keys from .env
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')

# MongoDB URI from .env
MONGO_URI = os.getenv('MONGO_URI')

# Define file paths and MongoDB client
current_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(current_dir, 'dados_completos.csv')
client = MongoClient(MONGO_URI)
db = client['binance_data']

# MongoDB collections
collection_csv = db['csv_data']
collection_historical_exercise = db['historical_exercise_data']

# Create indexes to optimize queries
def create_indexes():
    # Indexes for collection_csv
    collection_csv.create_index([("time", ASCENDING)], name="idx_time")
    collection_csv.create_index([("symbol", ASCENDING)], name="idx_symbol")

    # Indexes for collection_historical_exercise
    collection_historical_exercise.create_index([("expiryDate", ASCENDING)], name="idx_expiryDate")
    collection_historical_exercise.create_index([("symbol", ASCENDING)], name="idx_symbol")

    logging.info("Indexes created for collections.")

# Ensure indexes are created at script startup
create_indexes()

# Function to insert data into MongoDB in batches
def insert_data_into_mongo(df, collection):
    try:
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        elif 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], errors='coerce')
        payload = df.to_dict(orient='records')
        if payload:
            collection.insert_many(payload, ordered=False)
            logging.info(f"{len(payload)} records inserted into MongoDB successfully in collection '{collection.name}'.")
        else:
            logging.info("No records to insert.")
    except Exception as e:
        logging.error(f"Error inserting data into MongoDB: {e}")

# Function to retrieve the latest record from collection_csv
def get_latest_record():
    latest_record = collection_csv.find_one(sort=[("time", -1)])
    return latest_record if latest_record else None

# Function to download and save BTC/USD data from Binance API
def download_and_save_btcusd(symbol, start_time, end_time, limit=1000):
    base_url = "https://api.binance.com/api/v3/aggTrades"
    try:
        params = {
            "symbol": symbol,
            "startTime": start_time,
            "endTime": end_time,
            "limit": limit
        }
        while True:
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                if not data:
                    break  # Exit loop if there's no data

                # Prepare and insert records in batches
                records = [{
                    "symbol": symbol,
                    "price": float(trade['p']),
                    "time": int(trade['T']),
                    "quantity": float(trade['q'])
                } for trade in data]
                
                insert_data_into_mongo(pd.DataFrame(records), collection_csv)
                last_time = data[-1]['T']
                params['startTime'] = last_time + 1  # Avoid duplicates
            else:
                logging.error(f"Error downloading data: {response.status_code} - {response.text}")
                break
    except Exception as e:
        logging.error(f"Error in download_and_save_btcusd: {e}")

# Function to fetch data from MongoDB and return it as a DataFrame
def fetch_data(collection):
    cursor = collection.find()
    df = pd.DataFrame(list(cursor))
    if df.empty:
        logging.info(f"No data found in collection '{collection.name}' in MongoDB")
    else:
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], errors='coerce')
        if '_id' in df.columns:
            df.drop(columns=['_id'], inplace=True)
    return df

# Function to fetch historical exercise records from Binance API
def fetch_historical_exercise_records(symbol="BTCUSDT", start_time=None, end_time=None, limit=1000):
    url = "https://eapi.binance.com/eapi/v1/exerciseHistory"
    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY,
    }
    params = {
        "underlying": symbol,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        # Log and process each record
        for record in data:
            record['expiryDate'] = datetime.fromtimestamp(record['expiryDate'] / 1000)  # Convert timestamp
            logging.info(f"Fetched record for {record['symbol']} with strike result {record['strikeResult']}")

        # Convert data to DataFrame
        df = pd.DataFrame(data)
        return df if not df.empty else None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching historical exercise records: {e}")
        return None

# Function to fetch and store historical exercise records in MongoDB
def fetch_and_store_historical_exercise_data(start_time=int(datetime(2020, 1, 1).timestamp() * 1000), 
                                             end_time=int(datetime(2023, 12, 31).timestamp() * 1000)):
    logging.info("Starting Historical Exercise Records data collection...")
    
    # Fetch data within defined start and end times
    df_historical_exercise = fetch_historical_exercise_records(start_time=start_time, end_time=end_time)
    if df_historical_exercise is not None and not df_historical_exercise.empty:
        insert_data_into_mongo(df_historical_exercise, collection_historical_exercise)
        logging.info("Historical Exercise Records data updated.")
    else:
        logging.info("Historical Exercise Records DataFrame is empty or not loaded correctly.")

# Function to read CSV file into a DataFrame
def read_csv_file():
    try:
        logging.info(f"Reading CSV file from path: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        logging.info("CSV read successfully.")
        return df
    except FileNotFoundError:
        logging.error(f"File {csv_file_path} not found.")
        return None
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        return None
