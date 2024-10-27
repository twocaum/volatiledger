from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import plotly.express as px
import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from flask import Flask, send_file
import requests
import time
import threading
import schedule
from datetime import datetime, timedelta

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Definir a URI do MongoDB a partir do .env
MONGO_URI = os.getenv('MONGO_URI')

# Conectar ao MongoDB
client = MongoClient(MONGO_URI)
db = client['binance_data']
collection_csv = db['csv_data']
collection_options = db['options_data']
collection_futures = db['futures_data']  # Nova coleção para dados de futuros

# Caminho para o arquivo CSV relativo à localização do script
current_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(current_dir, 'dados_completos.csv')

# Configurar Flask para servir o CSV completo
app = Flask(__name__)

@app.route('/api/dados_completos', methods=['GET'])
def download_completo():
    if os.path.exists(csv_file_path):
        return send_file(csv_file_path, as_attachment=True)
    else:
        return "Arquivo CSV completo não encontrado.", 404

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
        last_trade_id = None

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
                    # Update params for next request
                    last_trade_id = data[-1]['a']
                    params['fromId'] = last_trade_id + 1
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

def fetch_and_store_options_data():
    global df_options
    print("Iniciando coleta de dados de opções...")
    options_data = fetch_options_data_parallel()
    df_options = process_options_data(options_data)
    if df_options is not None and not df_options.empty:
        insert_data_into_mongo(df_options, collection_options)
        print("Dados de opções atualizados.")
    else:
        print("DataFrame de opções está vazio ou não foi carregado corretamente.")

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

# Variáveis globais para armazenar os DataFrames
df_csv = pd.DataFrame()
df_options = pd.DataFrame()
df_futures = pd.DataFrame()
df_daily = pd.DataFrame()

# Criar visualização com Dash
app_dash = dash.Dash(__name__, server=app, external_stylesheets=[dbc.themes.BOOTSTRAP])

def main():
    global df_csv, df_options, df_futures, df_daily

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
            start_time = latest_record['time'] + 1  # Continue from the last recorded timestamp
        else:
            # Default start date: 1st January 2024
            start_time = int(datetime(2024, 1, 1).timestamp() * 1000)

        # End date: Current time
        end_date = int(datetime.now().timestamp() * 1000)

        # Baixar dados históricos
        symbol = "BTCUSDT"
        download_and_save_btcusd(symbol, start_time, end_date)

    # Iniciar o agendamento da coleta de dados de opções
    schedule.every(10).minutes.do(fetch_and_store_options_data)
    # Executar imediatamente na inicialização
    fetch_and_store_options_data()

    # Iniciar o agendamento da coleta de dados de futuros
    schedule.every(10).minutes.do(fetch_and_store_futures_data)
    # Executar imediatamente na inicialização
    fetch_and_store_futures_data()

    # Iniciar o agendador em uma thread separada
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()

    # Buscar dados do MongoDB e processar para Dash
    df_csv = fetch_data(collection_csv)
    df_options = fetch_data(collection_options)
    df_futures = fetch_data(collection_futures)

    # Processar dados do CSV
    if not df_csv.empty and 'time' in df_csv.columns and 'price' in df_csv.columns:
        df_csv['price'] = pd.to_numeric(df_csv['price'], errors='coerce')
        df_csv['quantity'] = pd.to_numeric(df_csv['quantity'], errors='coerce')
        df_csv['time'] = pd.to_datetime(df_csv['time'], unit='ms')
        df_csv.set_index('time', inplace=True)

        # Resample diário para obter agregações diferentes dos dados
        df_daily = df_csv.resample('D').agg({
            'price': ['mean', 'min', 'max'],
            'quantity': 'sum'
        }).reset_index()

        # Ajustar o nome das colunas após a agregação
        df_daily.columns = ['time', 'price_mean', 'price_min', 'price_max', 'total_quantity']
    else:
        df_daily = pd.DataFrame()

    # Executar o servidor Dash na thread principal
    app_dash.run_server(debug=False, host='127.0.0.1', port=8050)

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

def fetch_and_store_futures_data():
    global df_futures
    print("Iniciando coleta de dados de futuros...")
    futures_data = fetch_futures_data_parallel()
    df_futures = process_futures_data(futures_data)
    if df_futures is not None and not df_futures.empty:
        insert_data_into_mongo(df_futures, collection_futures)
        print("Dados de futuros atualizados.")
    else:
        print("DataFrame de futuros está vazio ou não foi carregado corretamente.")

# Layout do Dash com abas para CSV, Dados de Opções e Dados de Futuros
app_dash.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H1("Dashboard Binance Data", className="text-center my-4"), width=12)
    ]),
    dbc.Row([
        dbc.Col(
            dcc.Tabs(id='tabs', value='tab-csv', children=[
                dcc.Tab(label='Dados CSV', value='tab-csv'),
                dcc.Tab(label='Dados de Opções', value='tab-options'),
                dcc.Tab(label='Dados de Futuros', value='tab-futures'),
            ]),
            width=12
        )
    ]),
    html.Div(id='tabs-content')
], fluid=True)

# Callback para renderizar o conteúdo com base na aba selecionada
@app_dash.callback(Output('tabs-content', 'children'),
                   [Input('tabs', 'value')])
def render_content(tab):
    if tab == 'tab-csv':
        return generate_csv_layout()
    elif tab == 'tab-options':
        return generate_options_layout()
    elif tab == 'tab-futures':
        return generate_futures_layout()

def generate_csv_layout():
    if not df_daily.empty:
        # Gráficos para os dados do CSV
        fig_mean = px.line(
            df_daily,
            x='time',
            y='price_mean',
            title='Preço Médio Diário ao Longo do Tempo',
            labels={'time': 'Data', 'price_mean': 'Preço Médio (USD)'},
            markers=True
        )

        fig_min_max = px.line(
            df_daily,
            x='time',
            y=['price_min', 'price_max'],
            title='Preços Mínimo e Máximo Diários ao Longo do Tempo',
            labels={'time': 'Data', 'value': 'Preço (USD)'},
            markers=True
        )

        fig_quantity = px.bar(
            df_daily,
            x='time',
            y='total_quantity',
            title='Quantidade Total Diária de Transações',
            labels={'time': 'Data', 'total_quantity': 'Quantidade Total'},
        )

        layout = dbc.Container([
            dbc.Row([
                dbc.Col(dcc.Graph(id='mean-price', figure=fig_mean), md=12)
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id='min-max-price', figure=fig_min_max), md=12)
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id='total-quantity', figure=fig_quantity), md=12)
            ]),
            dbc.Row([
                dbc.Col([
                    html.Button("Baixar CSV Agregado", id="btn-download-aggregated", className="mt-3 btn btn-primary"),
                    dcc.Download(id="download-dataframe-csv")
                ], width='auto'),
                dbc.Col([
                    html.A("Baixar CSV Completo", href="/api/dados_completos", target="_blank", className="btn btn-secondary mt-3")
                ], width='auto'),
            ], className="mt-3")
        ], fluid=True)
    else:
        layout = dbc.Container([
            dbc.Row([
                dbc.Col(html.H3("Nenhum dado encontrado ou erro ao carregar os dados CSV.", className="text-center"))
            ])
        ], fluid=True)
    return layout

# Callback para baixar o CSV agregado
@app_dash.callback(
    Output("download-dataframe-csv", "data"),
    Input("btn-download-aggregated", "n_clicks"),
    prevent_initial_call=True
)
def download_csv(n_clicks):
    return dcc.send_data_frame(df_daily.to_csv, "dados_resumidos.csv")

def generate_options_layout():
    if not df_options.empty and 'symbol' in df_options.columns:
        symbols = df_options['symbol'].unique()
        layout = dbc.Container([
            dbc.Row([
                dbc.Col(
                    dcc.Dropdown(
                        id='symbol-dropdown-options',
                        options=[{'label': sym, 'value': sym} for sym in symbols],
                        value=symbols[0],
                        placeholder="Selecione um símbolo de opção"
                    ), width=6
                )
            ], className="mb-4"),
            dbc.Row([
                dbc.Col(dcc.Graph(id='markIV-graph'), md=6),
                dbc.Col(dcc.Graph(id='markPrice-graph'), md=6),
            ]),
            dbc.Row([
                dbc.Col([
                    html.P("Selecione um intervalo de tempo:"),
                    dcc.DatePickerRange(
                        id='date-picker-range-options',
                        start_date=df_options['time'].min().date(),
                        end_date=df_options['time'].max().date()
                    )
                ], width=6)
            ], className="mt-4"),
            dbc.Row([
                dbc.Col(dcc.Graph(id='filtered-markIV-graph'), md=6),
                dbc.Col(dcc.Graph(id='filtered-markPrice-graph'), md=6),
            ]),
            # Botão de download dos dados de opções
            dbc.Row([
                dbc.Col([
                    html.Button("Baixar Dados de Opções", id="btn-download-options", className="mt-3 btn btn-primary"),
                    dcc.Download(id="download-options-csv")
                ], width='auto'),
            ], className="mt-3"),
        ], fluid=True)
    else:
        layout = dbc.Container([
            dbc.Row([
                dbc.Col(html.H3("Nenhum dado encontrado ou erro ao carregar os dados de opções.", className="text-center"))
            ])
        ], fluid=True)
    return layout

def generate_futures_layout():
    if not df_futures.empty and 'symbol' in df_futures.columns:
        symbols = df_futures['symbol'].unique()
        layout = dbc.Container([
            dbc.Row([
                dbc.Col(
                    dcc.Dropdown(
                        id='symbol-dropdown-futures',
                        options=[{'label': sym, 'value': sym} for sym in symbols],
                        value=symbols[0],
                        placeholder="Selecione um símbolo de futuros"
                    ), width=6
                )
            ], className="mb-4"),
            dbc.Row([
                dbc.Col(dcc.Graph(id='lastPrice-graph'), md=6),
                dbc.Col(dcc.Graph(id='volume-graph'), md=6),
            ]),
            # Botão de download dos dados de futuros
            dbc.Row([
                dbc.Col([
                    html.Button("Baixar Dados de Futuros", id="btn-download-futures", className="mt-3 btn btn-primary"),
                    dcc.Download(id="download-futures-csv")
                ], width='auto'),
            ], className="mt-3"),
        ], fluid=True)
    else:
        layout = dbc.Container([
            dbc.Row([
                dbc.Col(html.H3("Nenhum dado encontrado ou erro ao carregar os dados de futuros.", className="text-center"))
            ])
        ], fluid=True)
    return layout

# Callback para atualizar os gráficos das opções
@app_dash.callback(
    [Output('markIV-graph', 'figure'),
     Output('markPrice-graph', 'figure')],
    [Input('symbol-dropdown-options', 'value')]
)
def update_options_graphs(selected_symbol):
    if selected_symbol:
        filtered_df = df_options[df_options['symbol'] == selected_symbol]
        fig_markIV = px.line(
            filtered_df,
            x='time',
            y='markIV',
            title=f'Volatilidade Implícita (markIV) para {selected_symbol}',
            labels={'time': 'Tempo', 'markIV': 'Volatilidade Implícita'},
            markers=True
        )
        fig_markPrice = px.line(
            filtered_df,
            x='time',
            y='markPrice',
            title=f'Preço de Marca (markPrice) para {selected_symbol}',
            labels={'time': 'Tempo', 'markPrice': 'Preço de Marca'},
            markers=True
        )
        return fig_markIV, fig_markPrice
    else:
        return {}, {}

# Callback para atualizar gráficos filtrados por data (opções)
@app_dash.callback(
    [Output('filtered-markIV-graph', 'figure'),
     Output('filtered-markPrice-graph', 'figure')],
    [Input('symbol-dropdown-options', 'value'),
     Input('date-picker-range-options', 'start_date'),
     Input('date-picker-range-options', 'end_date')]
)
def update_filtered_options_graphs(selected_symbol, start_date, end_date):
    if selected_symbol and start_date and end_date:
        mask = (
            (df_options['symbol'] == selected_symbol) &
            (df_options['time'] >= pd.to_datetime(start_date)) &
            (df_options['time'] <= pd.to_datetime(end_date))
        )
        filtered_df = df_options[mask]
        fig_markIV = px.line(
            filtered_df,
            x='time',
            y='markIV',
            title=f'Volatilidade Implícita (markIV) para {selected_symbol} ({start_date} a {end_date})',
            labels={'time': 'Tempo', 'markIV': 'Volatilidade Implícita'},
            markers=True
        )
        fig_markPrice = px.line(
            filtered_df,
            x='time',
            y='markPrice',
            title=f'Preço de Marca (markPrice) para {selected_symbol} ({start_date} a {end_date})',
            labels={'time': 'Tempo', 'markPrice': 'Preço de Marca'},
            markers=True
        )
        return fig_markIV, fig_markPrice
    else:
        return {}, {}

# Callback para baixar o CSV de opções
@app_dash.callback(
    Output("download-options-csv", "data"),
    Input("btn-download-options", "n_clicks"),
    prevent_initial_call=True
)
def download_options_csv(n_clicks):
    if not df_options.empty:
        return dcc.send_data_frame(df_options.to_csv, "dados_opcoes.csv")
    else:
        return None

# Callback para atualizar os gráficos dos futuros
@app_dash.callback(
    [Output('lastPrice-graph', 'figure'),
     Output('volume-graph', 'figure')],
    [Input('symbol-dropdown-futures', 'value')]
)
def update_futures_graphs(selected_symbol):
    if selected_symbol:
        filtered_df = df_futures[df_futures['symbol'] == selected_symbol]
        fig_lastPrice = px.line(
            filtered_df,
            x='time',
            y='lastPrice',
            title=f'Último Preço para {selected_symbol}',
            labels={'time': 'Tempo', 'lastPrice': 'Último Preço'},
            markers=True
        )
        fig_volume = px.line(
            filtered_df,
            x='time',
            y='volume',
            title=f'Volume para {selected_symbol}',
            labels={'time': 'Tempo', 'volume': 'Volume'},
            markers=True
        )
        return fig_lastPrice, fig_volume
    else:
        return {}, {}

# Callback para baixar o CSV de futuros
@app_dash.callback(
    Output("download-futures-csv", "data"),
    Input("btn-download-futures", "n_clicks"),
    prevent_initial_call=True
)
def download_futures_csv(n_clicks):
    if not df_futures.empty:
        return dcc.send_data_frame(df_futures.to_csv, "dados_futuros.csv")
    else:
        return None

if __name__ == "__main__":
    main()
