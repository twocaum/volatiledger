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

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Definir a URI do MongoDB a partir do .env
MONGO_URI = os.getenv('MONGO_URI')

# Conectar ao MongoDB
client = MongoClient(MONGO_URI)
db = client['binance_data']
collection = db['options_chain']

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

# Ler o CSV usando pandas
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

# Inserir os dados no MongoDB
def insert_data_into_mongo(df):
    try:
        # Converter a coluna 'time' para datetime, se existir
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
        
        # Verificar se a coluna 'price' existe
        if 'price' in df.columns:
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
        else:
            print("Coluna 'price' não encontrada no DataFrame. Certifique-se de que os dados estejam corretos.")
        
        # Limpar a coleção antes de inserir novos dados
        collection.delete_many({})
        
        # Inserir dados no MongoDB
        payload = df.to_dict(orient='records')
        if payload:
            collection.insert_many(payload)
            print(f"{len(payload)} registros inseridos no MongoDB com sucesso.")
        else:
            print("Nenhum registro para inserir.")
    except Exception as e:
        print(f"Erro ao inserir dados no MongoDB: {e}")

# Função principal para ler o CSV e inserir os dados no MongoDB
def main():
    if os.path.exists(csv_file_path):
        print("Arquivo CSV detectado, iniciando leitura.")
        df = read_csv_file()
        if df is not None and not df.empty:
            insert_data_into_mongo(df)
        else:
            print("DataFrame está vazio ou não foi carregado corretamente.")
    else:
        print("Arquivo CSV não encontrado. Nenhum dado foi carregado para o MongoDB.")

    # Criar visualização com Dash
    app_dash = dash.Dash(__name__, server=app, external_stylesheets=[dbc.themes.BOOTSTRAP])

    def fetch_data():
        cursor = collection.find()
        df = pd.DataFrame(list(cursor))
        if df.empty:
            print("Nenhum dado encontrado no MongoDB")
        else:
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'], unit='ms', errors='coerce')
            if '_id' in df.columns:
                df.drop(columns=['_id'], inplace=True)
        return df

    df = fetch_data()

    if not df.empty and 'time' in df.columns and 'price' in df.columns:
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
        df.set_index('time', inplace=True)

        # Resample diário para obter agregações diferentes dos dados
        df_daily = df.resample('D').agg({
            'price': ['mean', 'min', 'max'],
            'quantity': 'sum'
        }).reset_index()

        # Ajustar o nome das colunas após a agregação
        df_daily.columns = ['time', 'price_mean', 'price_min', 'price_max', 'total_quantity']

        # Gráfico de preço médio diário
        fig_mean = px.line(
            df_daily,
            x='time',
            y='price_mean',
            title='Preço Médio Diário ao Longo do Tempo',
            labels={'time': 'Data', 'price_mean': 'Preço Médio (USD)'},
            markers=True
        )

        # Gráfico de preço mínimo e máximo diário
        fig_min_max = px.line(
            df_daily,
            x='time',
            y=['price_min', 'price_max'],
            title='Preços Mínimo e Máximo Diários ao Longo do Tempo',
            labels={'time': 'Data', 'value': 'Preço (USD)'},
            markers=True
        )

        # Gráfico de quantidade total diária
        fig_quantity = px.bar(
            df_daily,
            x='time',
            y='total_quantity',
            title='Quantidade Total Diária de Transações',
            labels={'time': 'Data', 'total_quantity': 'Quantidade Total'},
        )

        # Atualizar layout do Dash para incluir todos os gráficos e botões de download
        app_dash.layout = dbc.Container([
            dbc.Row([
                dbc.Col(html.H1("BTC Histórico de preços e Quantidade ($)", className="text-center my-4"))
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id='mean-price', figure=fig_mean))
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id='min-max-price', figure=fig_min_max))
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id='total-quantity', figure=fig_quantity))
            ]),
            dbc.Row([
                dbc.Col(html.Button("Baixar CSV Agregado", id="btn-download-aggregated", className="mt-3")),
                dcc.Download(id="download-dataframe-csv")
            ]),
            dbc.Row([
                dbc.Col(html.A("Baixar CSV Completo", href="/api/dados_completos", target="_blank", className="btn btn-primary mt-3"))
            ]),
        ], fluid=True)

    else:
        app_dash.layout = html.Div(children=[
            html.H1(children='Dashboard MongoDB', className="text-center"),
            html.P("No data found or error loading data.", className="text-center")
        ])

    # Callback para baixar o CSV agregado
    @app_dash.callback(
        Output("download-dataframe-csv", "data"),
        Input("btn-download-aggregated", "n_clicks"),
        prevent_initial_call=True
    )
    def download_csv(n_clicks):
        return dcc.send_data_frame(df_daily.to_csv, "dados_resumidos.csv")

    app_dash.run_server(debug=True, host='127.0.0.1', port=8050)

if __name__ == "__main__":
    main()
