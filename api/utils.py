# utils.py

import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import requests
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Definir a URI do MongoDB a partir do .env
MONGO_URI = os.getenv('MONGO_URI')

# Conectar ao MongoDB
client = MongoClient(MONGO_URI)
db = client['binance_data']

# Coleções do MongoDB
collection_csv = db['csv_data']
collection_options = db['options_data']
collection_futures = db['futures_data']

# Caminho para o arquivo CSV relativo à localização do script
current_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(current_dir, 'dados_completos.csv')

def get_latest_record():
    latest_record = list(collection_csv.find().sort("time", -1).limit(1))
    if len(latest_record) > 0:
        return latest_record[0]
    return None

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
                            print(f"Inserted record for {datetime.fromtimestamp(trade_data['time'] / 1000).strftime('%Y-%m-%d %H:%M:%S')} with price {trade_data['price']}")
                        except Exception as e:
                            print(f"Error inserting data: {e}")
                    # Atualizar os parâmetros para a próxima requisição
                    last_time = data[-1]['T']
                    params['startTime'] = last_time + 1  # Evitar duplicatas
                else:
                    data_fetched = False
            else:
                print(f"Error downloading data: {response.status_code} - {response.text}")
                data_fetched = False

    except Exception as e:
        print(f"Error downloading or saving data: {e}")

def get_all_option_symbols():
    url = 'https://eapi.binance.com/eapi/v1/exchangeInfo'  # Endpoint correto para opções
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Inspecionar as chaves disponíveis na resposta
        print("Chaves na resposta da API exchangeInfo:", data.keys())

        # Extrair os símbolos de opções
        if 'symbols' in data:
            symbols = [s['symbol'] for s in data['symbols']]
            print(f"Encontrados {len(symbols)} símbolos de opções.")
        else:
            print("Nenhum símbolo de opção encontrado na resposta da API.")
            symbols = []

        return symbols
    except requests.exceptions.RequestException as e:
        print(f"Erro ao obter informações de opções: {e}")
        return []
    except KeyError as e:
        print(f"Chave inesperada na resposta da API: {e}")
        return []

def fetch_option_data(symbol):
    url = 'https://eapi.binance.com/eapi/v1/mark'
    params = {'symbol': symbol}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar dados para o símbolo {symbol}: {e}")
        return None

def fetch_options_data_parallel():
    symbols = get_all_option_symbols()
    data_list = []

    if not symbols:
        print("Nenhum símbolo de opção encontrado.")
        return None

    # Limites de taxa da Binance para opções
    # Consulte a documentação para obter informações atualizadas sobre os limites de taxa
    max_requests_per_minute = 1200  # Ajuste conforme necessário
    batch_size = max_requests_per_minute

    # Dividir símbolos em lotes
    symbol_batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]

    try:
        for batch in symbol_batches:
            with ThreadPoolExecutor(max_workers=100) as executor:
                future_to_symbol = {executor.submit(fetch_option_data, symbol): symbol for symbol in batch}
                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    try:
                        data = future.result()
                        if data:
                            data_list.append(data)
                    except Exception as exc:
                        print(f"Símbolo {symbol} gerou uma exceção: {exc}")
            # Esperar 60 segundos entre os lotes para respeitar o limite de taxa
            if len(symbol_batches) > 1:
                print("Esperando 60 segundos para respeitar o limite de taxa da API...")
                time.sleep(60)
    except Exception as e:
        print(f"Erro ao buscar dados da API: {e}")
        return None

    return data_list

def process_options_data(data):
    if data is None:
        return None
    df = pd.DataFrame(data)
    numeric_columns = [
        'markPrice', 'bidIV', 'askIV', 'markIV',
        'delta', 'theta', 'gamma', 'vega',
        'highPriceLimit', 'lowPriceLimit', 'riskFreeInterest'
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df['time'] = pd.Timestamp.now()
    return df

def get_all_futures_symbols():
    url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Inspecionar as chaves disponíveis na resposta
        print("Chaves na resposta da API exchangeInfo:", data.keys())

        # Extrair os símbolos de futuros
        if 'symbols' in data:
            symbols = [s['symbol'] for s in data['symbols']]
            print(f"Encontrados {len(symbols)} símbolos de futuros.")
        else:
            print("Nenhum símbolo de futuros encontrado na resposta da API.")
            symbols = []

        return symbols
    except requests.exceptions.RequestException as e:
        print(f"Erro ao obter informações de futuros: {e}")
        return []

def fetch_future_data(symbol):
    url = 'https://fapi.binance.com/fapi/v1/ticker/24hr'
    params = {'symbol': symbol}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar dados para o símbolo {symbol}: {e}")
        return None

def fetch_futures_data_parallel():
    symbols = get_all_futures_symbols()
    data_list = []

    if not symbols:
        print("Nenhum símbolo de futuros encontrado.")
        return None

    # Limites de taxa da Binance para futuros
    # Consulte a documentação para obter informações atualizadas sobre os limites de taxa
    max_requests_per_minute = 1200  # Ajuste conforme necessário
    batch_size = max_requests_per_minute

    # Dividir símbolos em lotes
    symbol_batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]

    try:
        for batch in symbol_batches:
            with ThreadPoolExecutor(max_workers=100) as executor:
                future_to_symbol = {executor.submit(fetch_future_data, symbol): symbol for symbol in batch}
                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    try:
                        data = future.result()
                        if data:
                            data_list.append(data)
                    except Exception as exc:
                        print(f"Símbolo {symbol} gerou uma exceção: {exc}")
            # Esperar 60 segundos entre os lotes para respeitar o limite de taxa
            if len(symbol_batches) > 1:
                print("Esperando 60 segundos para respeitar o limite de taxa da API...")
                time.sleep(60)
    except Exception as e:
        print(f"Erro ao buscar dados da API: {e}")
        return None

    return data_list

def process_futures_data(data):
    if data is None:
        return None
    df = pd.DataFrame(data)
    numeric_columns = ['lastPrice', 'priceChangePercent', 'volume', 'openPrice', 'highPrice', 'lowPrice']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df['time'] = pd.Timestamp.now()
    return df

def insert_data_into_mongo(df, collection):
    try:
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], errors='coerce')

        payload = df.to_dict(orient='records')
        if payload:
            collection.insert_many(payload)
            print(f"{len(payload)} registros inseridos no MongoDB com sucesso na coleção '{collection.name}'.")
        else:
            print("Nenhum registro para inserir.")
    except Exception as e:
        print(f"Erro ao inserir dados no MongoDB: {e}")

def fetch_data(collection):
    cursor = collection.find()
    df = pd.DataFrame(list(cursor))
    if df.empty:
        print(f"Nenhum dado encontrado na coleção '{collection.name}' no MongoDB")
    else:
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], errors='coerce')
        if '_id' in df.columns:
            df.drop(columns=['_id'], inplace=True)
    return df

def read_csv_file():
    try:
        print(f"Lendo arquivo CSV do caminho: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        print("CSV lido com sucesso.")
        return df
    except FileNotFoundError:
        print(f"Arquivo {csv_file_path} não encontrado.")
        return None
    except Exception as e:
        print(f"Erro ao ler o arquivo CSV: {e}")
        return None

def fetch_and_store_options_data():
    print("Iniciando coleta de dados de opções...")
    options_data = fetch_options_data_parallel()
    df_options = process_options_data(options_data)
    if df_options is not None and not df_options.empty:
        insert_data_into_mongo(df_options, collection_options)
        print("Dados de opções atualizados.")
    else:
        print("DataFrame de opções está vazio ou não foi carregado corretamente.")

def fetch_and_store_futures_data():
    print("Iniciando coleta de dados de futuros...")
    futures_data = fetch_futures_data_parallel()
    df_futures = process_futures_data(futures_data)
    if df_futures is not None and not df_futures.empty:
        insert_data_into_mongo(df_futures, collection_futures)
        print("Dados de futuros atualizados.")
    else:
        print("DataFrame de futuros está vazio ou não foi carregado corretamente.")
