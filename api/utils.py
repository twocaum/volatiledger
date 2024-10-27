import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
from binance.cm_futures import CMFutures as BinanceFutures
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import logging

# Configurar o logging
logging.basicConfig(level=logging.INFO)

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Definir a URI do MongoDB a partir do .env
MONGO_URI = os.getenv('MONGO_URI')

current_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(current_dir, 'dados_completos.csv')

# Chaves da API Binance a partir do .env
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')

# Conectar ao MongoDB
client = MongoClient(MONGO_URI)
db = client['binance_data']

# Coleções do MongoDB
collection_csv = db['csv_data']
collection_futures = db['futures_data']
collection_open_interest = db['open_interest_data']

# Inicializar cliente da Binance
binance_futures = BinanceFutures(key=BINANCE_API_KEY, secret=BINANCE_API_SECRET)

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
                    params['startTime'] = last_time + 1  # Evitar duplicatas
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
        logging.info(f"Nenhum dado encontrado na coleção '{collection.name}' no MongoDB")
    else:
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], errors='coerce')
        if '_id' in df.columns:
            df.drop(columns=['_id'], inplace=True)
    return df

# Função para coletar dados históricos do contrato contínuo
def get_historical_futures_data(symbol, interval, start_time, end_time, limit=1000):
    try:
        candles = binance_futures.continuous_klines(
            pair=symbol,
            contractType="PERPETUAL",
            interval=interval,
            startTime=start_time,
            endTime=end_time,
            limit=limit
        )
        return candles
    except Exception as e:
        logging.error(f"Erro ao buscar dados históricos: {e}")
        return None

# Função para coletar dados de futuros de BTC
def fetch_future_data(symbol, interval, start_time, end_time):
    try:
        # Usa o símbolo correto e o tipo de contrato "PERPETUAL"
        candles = binance_futures.continuous_klines(
            pair=symbol,
            contractType="PERPETUAL",
            interval=interval,
            startTime=start_time,
            endTime=end_time,
            limit=1000
        )
        
        # Verifica se a resposta é uma lista de candles
        if isinstance(candles, list):
            # Transformar cada candle em um dicionário com campos nomeados
            candle_data = []
            for candle in candles:
                candle_data.append({
                    "open_time": candle[0],
                    "open": float(candle[1]),
                    "high": float(candle[2]),
                    "low": float(candle[3]),
                    "close": float(candle[4]),
                    "volume": float(candle[5]),
                    "close_time": candle[6],
                    "quote_asset_volume": float(candle[7]),
                    "number_of_trades": candle[8],
                    "taker_buy_base_asset_volume": float(candle[9]),
                    "taker_buy_quote_asset_volume": float(candle[10])
                })
            return candle_data
        else:
            logging.error(f"Formato inesperado de dados para {symbol}: {candles}")
            return None
    except Exception as e:
        logging.error(f"Erro ao buscar dados históricos para {symbol}: {e}")
        return None

# Coleta de dados de futuros em paralelo
def fetch_futures_data_parallel(symbols, interval, start_time, end_time):
    data_list = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(fetch_future_data, symbol, interval, start_time, end_time): symbol
            for symbol in symbols
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                data = future.result()
                if data:
                    for record in data:
                        record['symbol'] = symbol  # Adiciona o símbolo em cada candle
                    data_list.extend(data)
            except Exception as exc:
                logging.error(f"Símbolo {symbol} gerou uma exceção: {exc}")
    return data_list

# Processar e armazenar dados de futuros
def process_futures_data(data):
    if not data:
        return None
    df = pd.DataFrame(data)
    
    # Converte colunas numéricas
    numeric_columns = ['open', 'high', 'low', 'close', 'volume']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Converte o tempo de abertura para formato datetime
    if 'open_time' in df.columns:
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    
    return df



# Função para coletar histórico de Open Interest para BTC
def fetch_open_interest_data(symbol="BTCUSD", contract_type="PERPETUAL", interval="1h", limit=100):
    try:
        open_interest_data = binance_futures.open_interest_hist(symbol, contract_type, interval, limit=limit)
        df_open_interest = pd.DataFrame(open_interest_data)
        df_open_interest['timestamp'] = pd.to_datetime(df_open_interest['timestamp'], unit='ms')
        return df_open_interest
    except Exception as e:
        logging.error(f"Erro ao buscar dados de Open Interest para {symbol}: {e}")
        return None

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
            logging.info(f"{len(payload)} registros inseridos no MongoDB com sucesso na coleção '{collection.name}'.")
        else:
            logging.info("Nenhum registro para inserir.")
    except Exception as e:
        logging.error(f"Erro ao inserir dados no MongoDB: {e}")

def fetch_and_store_futures_data():
    logging.info("Iniciando coleta de dados de futuros de BTC...")
    symbols = ["BTCUSD"]
    interval = "1h"
    
    # Defina o start_time como uma data passada e o end_time como a data atual
    start_time = int(datetime(2023, 1, 1).timestamp() * 1000)  # Ajuste a data conforme necessário
    end_time = int(datetime.now().timestamp() * 1000)  # Coleta até o horário atual

    futures_data = fetch_futures_data_parallel(symbols, interval, start_time, end_time)
    df_futures = process_futures_data(futures_data)
    
    if df_futures is not None and not df_futures.empty:
        insert_data_into_mongo(df_futures, collection_futures)
        logging.info("Dados de futuros de BTC atualizados.")
    else:
        logging.info("DataFrame de futuros de BTC está vazio ou não foi carregado corretamente.")

def fetch_and_store_open_interest_data():
    logging.info("Iniciando coleta de dados de Open Interest para BTC...")
    df_open_interest = fetch_open_interest_data()
    if df_open_interest is not None and not df_open_interest.empty:
        insert_data_into_mongo(df_open_interest, collection_open_interest)
        logging.info("Dados de Open Interest de BTC atualizados.")
    else:
        logging.info("DataFrame de Open Interest de BTC está vazio ou não foi carregado corretamente.")

def read_csv_file():
    try:
        logging.info(f"Lendo arquivo CSV do caminho: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        logging.info("CSV lido com sucesso.")
        return df
    except FileNotFoundError:
        logging.error(f"Arquivo {csv_file_path} não encontrado.")
        return None
    except Exception as e:
        logging.error(f"Erro ao ler o arquivo CSV: {e}")
        return None
