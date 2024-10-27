import pandas as pd
import logging
import plotly.express as px
import dash
from dash import dcc, html, dash_table
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.INFO)

# Definir a URI do MongoDB a partir do .env
MONGO_URI = os.getenv('MONGO_URI')

# Conectar ao MongoDB
client = MongoClient(MONGO_URI)
db = client['binance_data']
collection = db['options_chain']

# Caminho para o arquivo CSV
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
                df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
            except ValueError:
                logging.warning("Erro ao converter a coluna 'time' para datetime. Verifique o formato dos dados.")
                return
        
        # Verificar se a coluna 'price' existe
        if 'price' in df.columns:
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
        else:
            logging.warning("Coluna 'price' não encontrada no DataFrame. Certifique-se de que os dados estejam corretos.")
        
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

# Executar as funções para ler o CSV e inserir os dados no MongoDB
def main():
    if os.path.exists(csv_file_path):
        logging.info("Arquivo CSV detectado, iniciando leitura.")
        df = read_csv_file(csv_file_path)
        if df is not None and not df.empty:
            insert_data_into_mongo(df)
        else:
            logging.warning("DataFrame está vazio ou não foi carregado corretamente.")
    else:
        logging.warning("Arquivo CSV não encontrado. Nenhum dado foi carregado para o MongoDB.")

    # Criar visualização com Dash
    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

    def fetch_data():
        cursor = collection.find()
        df = pd.DataFrame(list(cursor))
        if df.empty:
            logging.warning("Nenhum dado encontrado no MongoDB")
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

        # Atualizar layout do Dash para incluir todos os gráficos
        app.layout = dbc.Container([
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

    # Callback para atualizar o gráfico com base no slider
    @app.callback(
        Output('mongo-data', 'figure'),
        Input('slider-registros', 'value')
    )
    def update_graph(num_records):
        df_updated = df_daily.head(num_records)
        fig = px.line(
            df_updated,
            x='time',
            y='price',
            title='Preço Médio Diário ao Longo do Tempo',
            labels={'time': 'Data', 'price': 'Preço Médio (USD)'},
            markers=True
        )
        fig.update_layout(
            hovermode='x unified',
            xaxis=dict(
                showgrid=True,
                tickformat="%b %d, %Y",
                title="Data"
            ),
            yaxis=dict(
                title="Preço Médio (USD)",
                showgrid=True
            )
        )
        return fig

    # Callback para baixar o CSV
    @app.callback(
        Output("download-dataframe-csv", "data"),
        Input("btn-download", "n_clicks"),
        prevent_initial_call=True
    )
    def download_csv(n_clicks):
        return dcc.send_data_frame(df_daily.to_csv, "dados_completos.csv")

    # Callback para mostrar/ocultar a tabela
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
