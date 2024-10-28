from utils import (
    read_csv_file,
    insert_data_into_mongo,
    collection_csv,
    get_latest_record,
    download_and_save_btcusd,
    fetch_and_store_historical_exercise_data,
    csv_file_path
)
from dash_app import app_dash
import threading
import schedule
import time
import os
from datetime import datetime

def load_csv_once():
    # Check if the CSV data is already in MongoDB
    if collection_csv.estimated_document_count() == 0:
        # Process and insert CSV data if collection is empty
        if os.path.exists(csv_file_path):
            print("Arquivo CSV detectado, iniciando leitura.")
            df_csv = read_csv_file()
            if df_csv is not None and not df_csv.empty:
                insert_data_into_mongo(df_csv, collection_csv)
            else:
                print("DataFrame está vazio ou não foi carregado corretamente.")
        else:
            print("Arquivo CSV não encontrado. Nenhum dado foi carregado para o MongoDB.")
    else:
        print("Coleção 'csv_data' já possui dados. Pulando carregamento do CSV.")

def main():
    # Load CSV data only once if not already in MongoDB
    load_csv_once()

    # Determine the start and end times for historical data collection
    latest_record = get_latest_record()
    if latest_record:
        # Convert datetime to timestamp in milliseconds and add 1 ms
        start_time = int(latest_record['time'].timestamp() * 1000) + 1
        print(f"Último registro encontrado. Iniciando coleta a partir de {start_time}.")
    else:
        # Default start date: January 1, 2024
        start_time = int(datetime(2024, 1, 1).timestamp() * 1000)
        print("Nenhum registro anterior encontrado. Iniciando coleta a partir de 2024-01-01.")

    # End date: Current time
    end_time = int(datetime(2024, 8, 1).timestamp() * 1000)
    print(f"Coletando dados até {end_time}.")

    # Download historical data
    symbol = "BTCUSDT"
    download_and_save_btcusd(symbol, start_time, end_time)

    # Schedule data collection
    schedule.every(1).minutes.do(fetch_and_store_historical_exercise_data)
    
    # Run immediately on startup
    fetch_and_store_historical_exercise_data()

    # Start the scheduler in a separate thread
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()

    # Start the Dash server
    app_dash.run_server(debug=True, host='127.0.0.1', port=8050)

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
