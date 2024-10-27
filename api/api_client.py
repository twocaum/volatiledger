import requests
import dash
from dash import dcc, html, dash_table
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import logging
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)

# Connect to MongoDB using the URI from .env
MONGO_URI = os.getenv('MONGO_URI')
client = MongoClient(MONGO_URI)
db = client['binance_data']
collection = db['options_chain']
api_key = os.getenv('API_KEY')

# Binance API URL
BASE_URL = "https://api.binance.com/api/v3/aggTrades"

def get_latest_record():
    latest_record = list(collection.find().sort("time", -1).limit(1))
    if len(latest_record) > 0:
        return latest_record[0]
    return None

csv_file_path = 'dados_completos.csv'  # Substitua pelo caminho correto do seu CSV

# Ler o CSV usando pandas
def read_csv_file(csv_path):
    try:
        df = pd.read_csv(csv_path)
        logging.info("CSV lido com sucesso.")
        return df
    except FileNotFoundError:
        logging.error(f"Arquivo {csv_path} não encontrado.")
        return None
    except Exception as e:
        logging.error(f"Erro ao ler o arquivo CSV: {e}")
        return None

# Inserir os dados no MongoDB
def insert_data_into_mongo(df):
    try:
        # Converter a coluna 'time' para datetime, se existir
        if 'time' in df.columns:
            try:
                df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                logging.warning("Erro ao converter a coluna 'time' para datetime. Verifique o formato dos dados.")
                return
        
        # Limpar a coleção antes de inserir novos dados
        collection.delete_many({})
        
        # Inserir dados no MongoDB
        payload = df.to_dict(orient='records')
        if payload:
            collection.insert_many(payload)
            logging.info(f"{len(payload)} registros inseridos no MongoDB com sucesso.")
        else:
            logging.warning("Nenhum registro para inserir.")
    except Exception as e:
        logging.error(f"Erro ao inserir dados no MongoDB: {e}")

def download_and_save_options(symbol, start_time, end_time, limit=1000, api_key=None):
    try:
        params = {
            "symbol": symbol,
            "startTime": start_time,
            "endTime": end_time,
            "limit": limit
        }
        headers = {
            "X-MBX-APIKEY": api_key
        }
        response = requests.get(BASE_URL, params=params, headers=headers)

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
                    # Insert into MongoDB only if the record does not already exist
                    if collection.count_documents({"time": trade_data["time"]}) == 0:
                        collection.insert_one(trade_data)
                        logging.info(f"Inserted record for {datetime.fromtimestamp(trade_data['time'] / 1000).strftime('%Y-%m-%d %H:%M:%S')} with price {trade_data['price']}")
            else:
                logging.warning("No data received from Binance API.")
        else:
            logging.error(f"Error downloading data: {response.status_code} - {response.text}")

    except Exception as e:
        logging.error(f"Error downloading or saving data: {e}")

def main():
    symbol = "BTCUSDT"

    if os.path.exists(csv_file_path):
        logging.info("Arquivo CSV detectado, iniciando leitura.")
        df = read_csv_file(csv_file_path)
        if df is not None and not df.empty:
            insert_data_into_mongo(df)
        else:
            logging.warning("DataFrame está vazio ou não foi carregado corretamente.")
            # Start time for data retrieval
            latest_record = get_latest_record()
            if latest_record:
                start_time = latest_record['time'] + 1  # Continue from the last recorded timestamp
            else:
                # Default start date: 1st January 2024
                start_time = int(datetime(2024, 1, 1).timestamp() * 1000)

            # End date: 31st December 2024
            end_date = int(datetime(2024, 7, 31, 23, 59, 59).timestamp() * 1000)

            # Retrieve data in parallel chunks for beginning, middle, and end of the day
            current_time = start_time
            tasks = []

            # Create ThreadPoolExecutor to manage threads
            with ThreadPoolExecutor(max_workers=12) as executor:
                while current_time < end_date:
                    day_start = current_time
                    day_mid = day_start + (12 * 60 * 60 * 1000)  # Middle of the day (12 hours later in milliseconds)
                    day_end = day_start + (23 * 60 * 60 * 1000)  # End of the day (23 hours later in milliseconds)

                    # Add tasks for start, middle, and end of each day
                    tasks.append(executor.submit(download_and_save_options, symbol, day_start, day_mid, api_key=api_key))
                    tasks.append(executor.submit(download_and_save_options, symbol, day_mid + 1, day_end, api_key=api_key))

                    # Move to next day
                    current_time += 24 * 60 * 60 * 1000  # Increment by 24 hours in milliseconds

                # Wait for all threads to complete
                for future in as_completed(tasks):
                    try:
                        future.result()  # Get result to check if any exceptions occurred
                    except Exception as exc:
                        logging.error(f"Generated an exception: {exc}")
    else:
        logging.warning("Arquivo CSV não encontrado. Nenhum dado foi carregado para o MongoDB.")

    
    

    # Create dashboard with Dash to visualize data
    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

    def fetch_data():
        cursor = collection.find()
        df = pd.DataFrame(list(cursor))
        if df.empty:
            logging.warning("No data found in MongoDB")
        else:
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'], unit='ms')
            if '_id' in df.columns:
                df.drop(columns=['_id'], inplace=True)
        return df

    df = fetch_data()

    if not df.empty and 'time' in df.columns and 'price' in df.columns:
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df.set_index('time', inplace=True)

        # Resample daily to get the average price per day
        df_daily = df.resample('D').mean(numeric_only=True).reset_index()

        fig = px.line(df_daily, x='time', y='price', title='Preço Médio Diário ao Longo do Tempo')

        app.layout = dbc.Container([
            dbc.Row([
                dbc.Col(html.H1("BTC Histórico de preços ($)", className="text-center my-4"))
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id='mongo-data', figure=fig))
            ]),
            dbc.Row([
                dbc.Col(html.P("Quantidade de registros a serem mostrados:", className="text-center")),
                dbc.Col(dcc.Slider(
                    id='slider-registros',
                    min=1,
                    max=len(df_daily),
                    step=10,
                    value=len(df_daily),
                    marks={i: str(i) for i in range(1, len(df_daily) + 1, 30)}
                ))
            ]),
            dbc.Row([
                dbc.Col(html.Button("Baixar CSV", id="btn-download", className="mt-3")),
                dcc.Download(id="download-dataframe-csv")
            ]),
            dbc.Row([
                dbc.Col(html.Button("Mostrar Tabela", id="btn-show-table", className="mt-3")),
                dbc.Col(html.Div(id="table-container", style={"display": "none"}))
            ])
        ], fluid=True)
    else:
        app.layout = html.Div(children=[
            html.H1(children='Dashboard MongoDB', className="text-center"),
            html.P("No data found or error loading data.", className="text-center")
        ])

    # Callback to update the graph based on the slider
    @app.callback(
        Output('mongo-data', 'figure'),
        Input('slider-registros', 'value')
    )
    def update_graph(num_records):
        df_updated = df_daily.head(num_records)
        fig = px.line(df_updated, x='time', y='price', title='Preço Médio Diário ao Longo do Tempo')
        fig.update_layout(
            xaxis=dict(
                tickformat="%b %d, %Y",
                title="Tempo",
            ),
            yaxis_title="Preço Médio"
        )
        return fig

    # Callback for CSV download button
    @app.callback(
        Output("download-dataframe-csv", "data"),
        Input("btn-download", "n_clicks"),
        prevent_initial_call=True
    )
    def download_csv(n_clicks):
        return dcc.send_data_frame(df_daily.to_csv, "dados_completos.csv")

    # Callback to show/hide the table
    @app.callback(
        Output("table-container", "children"),
        Output("table-container", "style"),
        Input("btn-show-table", "n_clicks"),
        State("table-container", "style"),
        prevent_initial_call=True
    )
    def toggle_table(n_clicks, current_style):
        if current_style["display"] == "none":
            table = dash_table.DataTable(
                columns=[{"name": i, "id": i} for i in df_daily.columns],
                data=df_daily.to_dict('records'),
                page_size=10,
                style_table={'overflowX': 'auto'}
            )
            return table, {"display": "block"}
        else:
            return None, {"display": "none"}

    if __name__ == '__main__':
        app.run_server(debug=True, host='127.0.0.1', port=8050)

if __name__ == "__main__":
    main()
