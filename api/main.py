from utils import (
    read_csv_file,
    insert_data_into_mongo,
    collection_csv,
    get_latest_record,
    download_and_save_btcusd,
    fetch_and_store_open_interest_data,
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
    # Verificar se a coleção CSV já possui dados
    if collection_csv.estimated_document_count() == 0:
        # Processar e inserir dados do CSV se a coleção estiver vazia
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

    # Coletar dados históricos se o CSV não estiver disponível
    latest_record = get_latest_record()
    if latest_record:
        # Converter datetime para timestamp em milissegundos e adicionar 1 ms
        start_time = int(latest_record['time'].timestamp() * 1000) + 1
        print(f"Último registro encontrado. Iniciando coleta a partir de {start_time}.")
    else:
        # Data de início padrão: 1º de janeiro de 2024
        start_time = int(datetime(2024, 1, 1).timestamp() * 1000)
        print("Nenhum registro anterior encontrado. Iniciando coleta a partir de 2024-01-01.")

    # Data de término: horário atual
    # end_time = int(datetime.now().timestamp() * 1000)
    end_time = int(datetime(2024, 8, 1).timestamp() * 1000)
    print(f"Coletando dados até {end_time}.")

    # Baixar dados históricos
    symbol = "BTCUSDT"
    download_and_save_btcusd(symbol, start_time, end_time)

    # Iniciar o agendamento da coleta de dados
    schedule.every(10).minutes.do(fetch_and_store_open_interest_data)
    schedule.every(10).minutes.do(fetch_and_store_futures_data)
    
    # Executar imediatamente na inicialização
    fetch_and_store_open_interest_data()
    fetch_and_store_futures_data()

    # Iniciar o agendador em uma thread separada
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()

    # Iniciar o servidor Dash
    app_dash.run_server(debug=True, host='127.0.0.1', port=8050)

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
