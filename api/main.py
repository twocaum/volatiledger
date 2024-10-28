import os
import pandas as pd
from utils import (
    read_csv_file,
    insert_data_into_mongo,
    collection_csv,
    download_and_save_btcusd,
    fetch_and_store_historical_exercise_data,
    csv_file_path
)
from dash_app import app_dash
import threading
import time
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Lock para sincronizar o acesso aos DataFrames
data_lock = threading.Lock()

def load_csv_once():
    """Load CSV data into MongoDB if the collection is empty."""
    if collection_csv.estimated_document_count() == 0 and os.path.exists(csv_file_path):
        logging.info("Arquivo CSV detectado, iniciando leitura.")
        df_csv = read_csv_file()
        if not df_csv.empty:
            insert_data_into_mongo(df_csv, collection_csv)
            logging.info("Dados do CSV inseridos no MongoDB.")
        else:
            logging.warning("DataFrame está vazio ou não foi carregado corretamente.")
    else:
        logging.info("Coleção 'csv_data' já possui dados ou o arquivo CSV não existe. Pulando carregamento do CSV.")

def start_dash_server():
    """Start the Dash server."""
    app_dash.run_server(debug=False, host='127.0.0.1', port=8050)

def continuous_btc_download(symbol, start_time, end_time):
    """Continuous loop to download BTCUSD data."""
    while True:
        download_and_save_btcusd(symbol, start_time, end_time)
        time.sleep(1)  # Aumentar o intervalo para 1 minuto para evitar sobrecarga

def continuous_historical_data(start_time, end_time):
    """Continuous loop to fetch historical exercise data."""
    while True:
        fetch_and_store_historical_exercise_data(start_time, end_time)
        time.sleep(1)  # Aumentar o intervalo para 1 minuto para evitar sobrecarga

def main():
    load_csv_once()

    # Start the Dash server in a separate thread
    dash_thread = threading.Thread(target=start_dash_server)
    dash_thread.daemon = True
    dash_thread.start()
    logging.info("Dash server started in a separate thread.")

    # Define parameters for BTCUSD and historical data download
    symbol = "BTCUSDT"
    start_time = int(datetime(2020, 1, 1).timestamp() * 1000)
    end_time = int(datetime(2023, 12, 31, 23, 59, 59).timestamp() * 1000)

    # Threads for continuous BTCUSD and historical data download
    btc_download_thread = threading.Thread(target=continuous_btc_download, args=(symbol, start_time, end_time))
    btc_download_thread.daemon = True
    btc_download_thread.start()
    logging.info("Thread de download contínuo de BTCUSDT iniciada.")

    historical_data_thread = threading.Thread(target=continuous_historical_data, args=(start_time, end_time))
    historical_data_thread.daemon = True
    historical_data_thread.start()
    logging.info("Thread de download contínuo de dados históricos de exercício iniciada.")

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
