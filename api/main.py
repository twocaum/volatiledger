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
import logging

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

def start_dash_server():
    """Run the Dash server in a separate thread."""
    app_dash.run_server(debug=False, host='127.0.0.1', port=8050)

def main():
    

    # Load CSV data only once if not already in MongoDB
    load_csv_once()

    # Start the Dash server in a separate thread at the beginning
    dash_thread = threading.Thread(target=start_dash_server)
    dash_thread.daemon = True
    dash_thread.start()
    print("Dash server started in a separate thread.")

    # Determine the start and end times for historical data collection
    latest_record = get_latest_record()
    if latest_record and 'time' in latest_record:
        # Extract and validate the 'time' field
        record_time = latest_record['time']
        try:
            # Check if record_time is a datetime, or try to convert it
            if isinstance(record_time, datetime):
                start_time = int(record_time.timestamp() * 1000) + 1
            elif isinstance(record_time, (int, float)):
                # Assume it's already a timestamp in milliseconds
                start_time = int(record_time) + 1
            else:
                # Attempt to parse if it's a string
                record_time = pd.to_datetime(record_time, errors='coerce')
                if pd.notnull(record_time):
                    start_time = int(record_time.timestamp() * 1000) + 1
                else:
                    raise ValueError("Invalid 'time' format in latest_record")
            print(f"Último registro encontrado. Iniciando coleta a partir de {start_time}.")
        except (OSError, ValueError) as e:
            # Log the error and default to a known start time
            logging.error(f"Error processing 'time' in latest_record: {e}. Using default start time.")
            start_time = int(datetime(2020, 1, 1).timestamp() * 1000)
    else:
        # Default start date: January 1, 2020
        start_time = int(datetime(2020, 1, 1).timestamp() * 1000)
        print("Nenhum registro anterior encontrado. Iniciando coleta a partir de 2020-01-01.")

    # End date: Current time
    end_time = int(datetime(2023, 12, 31).timestamp() * 1000)
    print(f"Coletando dados até {end_time}.")

    # Schedule data collection
    schedule.every(1).minutes.do(fetch_and_store_historical_exercise_data, start_time, end_time)

    # Run immediately on startup
    fetch_and_store_historical_exercise_data(start_time, end_time)

    # Download historical data
    symbol = "BTCUSDT"
    download_and_save_btcusd(symbol, start_time, end_time)

    # Start the scheduler in a separate thread
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()

    # Keep the main thread alive
    while True:
        time.sleep(1)

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
