import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import requests
import logging

# Configurar o logging
logging.basicConfig(level=logging.INFO)

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Chaves da API Binance a partir do .env
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')

# Definir a URI do MongoDB a partir do .env
MONGO_URI = os.getenv('MONGO_URI')

current_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(current_dir, 'dados_completos.csv')

# Conectar ao MongoDB
client = MongoClient(MONGO_URI)
db = client['binance_data']

# Coleções do MongoDB
collection_csv = db['csv_data']
collection_historical_exercise = db['historical_exercise_data']


# Função para inserir dados no MongoDB
def insert_data_into_mongo(df, collection):
    try:
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        elif 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], errors='coerce')
        payload = df.to_dict(orient='records')
        if payload:
            collection.insert_many(payload)
            logging.info(f"{len(payload)} records inserted into MongoDB successfully in collection '{collection.name}'.")
        else:
            logging.info("No records to insert.")
    except Exception as e:
        logging.error(f"Error inserting data into MongoDB: {e}")



def get_latest_record():
    latest_record = list(collection_csv.find().sort("time", -1).limit(1))
    return latest_record[0] if latest_record else None

def download_and_save_btcusd(symbol, start_time, end_time, limit=1000):
    base_url = "https://api.binance.com/api/v3/aggTrades"
    try:
        params = {
            "symbol": symbol,
            "startTime": start_time,
            "endTime": end_time,
            "limit": limit
        }
        data_fetched = True
        while data_fetched:
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and data:
                    for trade in data:
                        trade_data = {
                            "symbol": symbol,
                            "price": float(trade['p']),
                            "time": int(trade['T']),
                            "quantity": float(trade['q'])
                        }
                        try:
                            collection_csv.insert_one(trade_data)
                            logging.info(f"Inserted record for {datetime.fromtimestamp(trade_data['time'] / 1000).strftime('%Y-%m-%d %H:%M:%S')} with price {trade_data['price']}")
                        except Exception as e:
                            logging.error(f"Error inserting data: {e}")
                    last_time = data[-1]['T']
                    params['startTime'] = last_time + 1  # Avoid duplicates
                else:
                    data_fetched = False
            else:
                logging.error(f"Error downloading data: {response.status_code} - {response.text}")
                data_fetched = False
    except Exception as e:
        logging.error(f"Error downloading or saving data: {e}")

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

# Função para coletar histórico de registros de exercícios
# Function to fetch historical exercise records from Binance
def fetch_historical_exercise_records(symbol="BTCUSDT", start_time=None, end_time=None, limit=5000):
    """
    Fetches historical exercise records for options from the Binance API.
    Args:
        symbol (str): Underlying symbol (e.g., "BTCUSDT").
        start_time (int): Start time in milliseconds since epoch.
        end_time (int): End time in milliseconds since epoch.
        limit (int): Number of records to fetch.
    """
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
        if not df.empty:
            return df
        else:
            logging.info("No historical exercise records found.")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching historical exercise records: {e}")
        return None



# Function to fetch and store historical exercise records in MongoDB
def fetch_and_store_historical_exercise_data():
    logging.info("Starting Historical Exercise Records data collection...")
    
    # Define start and end times (replace with appropriate timestamps as needed)
    start_time = int(datetime(2024, 1, 1).timestamp() * 1000)
    end_time = int(datetime.now().timestamp() * 1000)

    df_historical_exercise = fetch_historical_exercise_records(start_time=start_time, end_time=end_time)
    if df_historical_exercise is not None and not df_historical_exercise.empty:
        insert_data_into_mongo(df_historical_exercise, collection_historical_exercise)
        logging.info("Historical Exercise Records data updated.")
    else:
        logging.info("Historical Exercise Records DataFrame is empty or not loaded correctly.")


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
