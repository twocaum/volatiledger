# main.py

from utils import (
    read_csv_file,
    insert_data_into_mongo,
    collection_csv,
    get_latest_record,
    download_and_save_btcusd,
    fetch_and_store_options_data,
    fetch_and_store_futures_data,
    csv_file_path
)
from dash_app import app_dash
import threading
import schedule
import time
import os
from datetime import datetime

def main():
    # Processar e inserir dados do CSV
    if os.path.exists(csv_file_path):
        print("Arquivo CSV detectado, iniciando leitura.")
        df_csv = read_csv_file()
        if df_csv is not None and not df_csv.empty:
            insert_data_into_mongo(df_csv, collection_csv)
        else:
            print("DataFrame está vazio ou não foi carregado corretamente.")
    else:
        print("Arquivo CSV não encontrado. Nenhum dado foi carregado para o MongoDB.")
        # Coletar dados históricos se o CSV não estiver disponível
        latest_record = get_latest_record()
        if latest_record:
            start_time = latest_record['time'] + 1  # Continue a partir do último timestamp registrado
        else:
            # Data de início padrão: 1º de janeiro de 2024
            start_time = int(datetime(2024, 1, 1).timestamp() * 1000)

        # Data de término: horário atual
        end_time = int(datetime.now().timestamp() * 1000)

        # Baixar dados históricos
        symbol = "BTCUSDT"
        download_and_save_btcusd(symbol, start_time, end_time)

    # Iniciar o agendamento da coleta de dados
    schedule.every(10).minutes.do(fetch_and_store_options_data)
    schedule.every(10).minutes.do(fetch_and_store_futures_data)
    # Executar imediatamente na inicialização
    fetch_and_store_options_data()
    fetch_and_store_futures_data()

    # Iniciar o agendador em uma thread separada
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()

    # O servidor será iniciado pelo Waitress, então não precisamos chamar app_dash.run_server() aqui

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

# Expor o aplicativo WSGI para o Waitress
# server = app_dash.server

if __name__ == "__main__":
    main()
    app_dash.run_server(debug=True, host='127.0.0.1', port=8050)
