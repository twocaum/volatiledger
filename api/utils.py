import os
import pandas as pd
from pymongo import ASCENDING, MongoClient
from dotenv import load_dotenv
from datetime import datetime
import requests
import logging
from time import sleep

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
    collection_csv.create_index([("time", ASCENDING)], name="idx_time")
    collection_csv.create_index([("symbol", ASCENDING)], name="idx_symbol")
    collection_historical_exercise.create_index([("expiryDate", ASCENDING)], name="idx_expiryDate")
    collection_historical_exercise.create_index([("symbol", ASCENDING)], name="idx_symbol")
    logging.info("Indexes created for collections.")

# Ensure indexes are created at script startup
create_indexes()

def insert_data_into_mongo(df, collection):
    try:
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        elif 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], errors='coerce')
        payload = df.to_dict(orient='records')
        if payload:
            collection.insert_many(payload, ordered=False)
            logging.info(f"{len(payload)} records inserted into '{collection.name}' collection.")
        else:
            logging.info("No records to insert.")
    except Exception as e:
        logging.error(f"Error inserting data into MongoDB: {e}")

def get_latest_record():
    return collection_csv.find_one(sort=[("time", -1)])

def download_and_save_btcusd(symbol, start_time, end_time, limit=1000):
    base_url = "https://api.binance.com/api/v3/aggTrades"
    params = {"symbol": symbol, "startTime": start_time, "endTime": end_time, "limit": limit}
    try:
        while True:
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                if not data:
                    logging.info("No more BTC/USD data to download.")
                    break
                records = [{"symbol": symbol, "price": float(trade['p']), "time": int(trade['T']), "quantity": float(trade['q'])} for trade in data]
                insert_data_into_mongo(pd.DataFrame(records), collection_csv)
                params['startTime'] = data[-1]['T'] + 1
            elif response.status_code == 429:
                logging.warning("Rate limit exceeded. Waiting before retry...")
                sleep(60)  # Sleep for a minute if rate limited
            else:
                logging.error(f"Error downloading data: {response.status_code} - {response.text}")
                break
    except Exception as e:
        logging.error(f"Error in download_and_save_btcusd: {e}")

def fetch_data(collection):
    cursor = collection.find()
    df = pd.DataFrame(list(cursor))
    if df.empty:
        logging.info(f"No data found in '{collection.name}' collection.")
    else:
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], unit='ms', errors='coerce')
            df.set_index('time', inplace=True)
        df.drop(columns=['_id'], inplace=True, errors='ignore')
    return df

def resample_daily(df):
    if df.empty:
        logging.warning("DataFrame is empty. Cannot resample.")
        return pd.DataFrame()
    
    try:
        # Resample data daily
        df_daily = df.resample('D').agg({
            'price': ['mean', 'min', 'max'],
            'quantity': 'sum'
        }).reset_index()
        
        # Flatten MultiIndex columns
        df_daily.columns = ['time', 'price_mean', 'price_min', 'price_max', 'total_quantity']
        logging.info(f"Daily resampled data:\n{df_daily.head()}")
        return df_daily
    except Exception as e:
        logging.error(f"Error resampling data: {e}")
        return pd.DataFrame()

def fetch_historical_exercise_records(symbol="BTCUSDT", start_time=None, end_time=None, limit=1000):
    url = "https://eapi.binance.com/eapi/v1/exerciseHistory"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    params = {"underlying": symbol, "startTime": start_time, "endTime": end_time, "limit": limit}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        if data:
            for record in data:
                record['expiryDate'] = datetime.fromtimestamp(record['expiryDate'] / 1000)
            return pd.DataFrame(data)
        else:
            logging.info("No historical exercise records found.")
            return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching historical exercise records: {e}")
        return pd.DataFrame()

def fetch_and_store_historical_exercise_data(start_time=None, end_time=None):
    start_time = start_time or int(datetime(2024, 1, 1).timestamp() * 1000)
    end_time = end_time or int(datetime.now().timestamp() * 1000)
    logging.info("Starting Historical Exercise Records data collection...")
    df_historical_exercise = fetch_historical_exercise_records(start_time=start_time, end_time=end_time)
    if not df_historical_exercise.empty:
        insert_data_into_mongo(df_historical_exercise, collection_historical_exercise)
    else:
        logging.info("Historical Exercise Records DataFrame is empty or not loaded correctly.")

def read_csv_file():
    try:
        logging.info(f"Reading CSV file from path: {csv_file_path}")
        return pd.read_csv(csv_file_path)
    except FileNotFoundError:
        logging.error(f"File {csv_file_path} not found.")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        return pd.DataFrame()

# Load initial data and resample it
df_csv = fetch_data(collection_csv)
if not df_csv.empty and isinstance(df_csv.index, pd.DatetimeIndex):
    logging.info("DatetimeIndex is set correctly.")
    df_daily = resample_daily(df_csv)
else:
    logging.warning("The 'time' column could not be converted to a DateTimeIndex or data is empty.")
