# Import necessary libraries
import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import pandas as pd
from api_client import fetch_data, collection_csv, collection_historical_exercise
import plotly.express as px
import plotly.graph_objs as go
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Create Dash app with suppress_callback_exceptions=True to allow dynamic layout
app_dash = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP],
                     suppress_callback_exceptions=True)

# Global variables for DataFrames
df_csv = pd.DataFrame()
df_historical_exercise = pd.DataFrame()
df_daily = pd.DataFrame()

# Function to load and process data
def load_data():
    global df_csv, df_historical_exercise, df_daily
    logging.info("Carregando dados das coleções MongoDB.")
    try:
        # Recuperar dados da coleção 'csv_data'
        df_csv = fetch_data(collection_csv)
        logging.info(f"Dados do CSV recuperados:\n{df_csv.head()}")
        logging.info(f"Tipos de dados do df_csv:\n{df_csv.dtypes}")
        
        if not df_csv.empty:
            logging.info("Dados históricos de BTC carregados com sucesso.")
            if {'price', 'quantity'}.issubset(df_csv.columns):
                df_csv['price'] = pd.to_numeric(df_csv['price'], errors='coerce')
                df_csv['quantity'] = pd.to_numeric(df_csv['quantity'], errors='coerce')
                logging.info("Colunas 'price' e 'quantity' convertidas para numéricas.")
                
                # Filtrar dados a partir de 2020 e reamostrar diariamente
                df_filtered = df_csv[df_csv.index >= pd.Timestamp("2020-01-01")]
                logging.info(f"Filtrando dados a partir de 2020: {df_filtered.shape[0]} registros restantes.")
                
                df_daily = df_filtered.resample('D').agg({
                    'price': ['mean', 'min', 'max'],
                    'quantity': 'sum'
                }).reset_index()
                df_daily.columns = ['time', 'price_mean', 'price_min', 'price_max', 'total_quantity']
                logging.info(f"Dados reamostrados diariamente:\n{df_daily.head()}")
            else:
                logging.warning("Colunas esperadas 'price' ou 'quantity' estão ausentes nos dados de preço de BTC.")
                df_daily = pd.DataFrame()
        else:
            logging.warning("Dados históricos de BTC estão vazios ou não puderam ser carregados.")
            df_daily = pd.DataFrame()

        # Recuperar dados da coleção 'historical_exercise_data'
        df_historical_exercise = fetch_data(collection_historical_exercise)
        logging.info(f"Dados de exercício histórico recuperados:\n{df_historical_exercise.head()}")
        logging.info(f"Tipos de dados do df_historical_exercise:\n{df_historical_exercise.dtypes}")
        
        if not df_historical_exercise.empty:
            if 'expiryDate' in df_historical_exercise.columns:
                if not pd.api.types.is_datetime64_any_dtype(df_historical_exercise['expiryDate']):
                    df_historical_exercise['expiryDate'] = pd.to_datetime(df_historical_exercise['expiryDate'], errors='coerce')
                    logging.info("Coluna 'expiryDate' convertida para datetime.")
            else:
                logging.warning("Coluna esperada 'expiryDate' está ausente nos dados de exercício histórico.")
        else:
            logging.warning("Dados de exercício histórico estão vazios ou não puderam ser carregados.")

    except Exception as e:
        logging.error(f"Erro ao carregar dados: {e}")

# Load data at startup
load_data()

# Layout
app_dash.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H1("Dashboard Binance Data", className="text-center my-4"), width=12)
    ]),
    dbc.Row([
        dbc.Col(
            dcc.Tabs(id='tabs', value='tab-csv', children=[
                dcc.Tab(label='Dados CSV', value='tab-csv'),
                dcc.Tab(label='Historical Exercise Records', value='tab-historical-exercise'),
            ]),
            width=12
        )
    ]),
    html.Div(id='tabs-content'),
    dcc.Interval(
        id='interval-component',
        interval=10*60*1000,  # Update every 10 minutes
        n_intervals=0
    ),
    # Componentes de Download Separados
    dcc.Download(id="download-aggregated-csv"),
    dcc.Download(id="download-complete-csv"),
    dcc.Download(id="download-historical-exercise-csv"),
    html.Div(id='dummy-output', style={'display': 'none'})
], fluid=True)

# Callback to render tab content
@app_dash.callback(Output('tabs-content', 'children'), [Input('tabs', 'value')])
def render_content(tab):
    if tab == 'tab-csv':
        return generate_csv_layout()
    elif tab == 'tab-historical-exercise':
        return generate_historical_exercise_layout()

def generate_csv_layout():
    if not df_daily.empty:
        # Gráfico para preço médio
        fig_mean = px.line(
            df_daily,
            x='time',
            y='price_mean',
            title='Preço Médio Diário ao Longo do Tempo',
            labels={'time': 'Data', 'price_mean': 'Preço Médio (USD)'},
            markers=True
        )

        # Gráfico para preço mínimo e máximo
        fig_min_max = px.line(
            df_daily,
            x='time',
            y=['price_min', 'price_max'],
            title='Preços Mínimo e Máximo Diários ao Longo do Tempo',
            labels={'time': 'Data', 'value': 'Preço (USD)', 'variable': 'Tipo'},
            markers=True
        )

        # Gráfico para quantidade total
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
                ], width='auto'),
                dbc.Col([
                    html.Button("Baixar CSV Completo", id="btn-download-complete", className="mt-3 btn btn-secondary")
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

def generate_historical_exercise_layout():
    if not df_historical_exercise.empty:
        dropdown = dcc.Dropdown(
            id='filter-strikeResult',
            options=[{'label': res, 'value': res} for res in df_historical_exercise['strikeResult'].unique()],
            placeholder="Filtrar por Resultado de Strike"
        )
        date_picker = dcc.DatePickerRange(
            id='date-picker',
            min_date_allowed=df_historical_exercise['expiryDate'].min(),
            max_date_allowed=df_historical_exercise['expiryDate'].max(),
            start_date=df_historical_exercise['expiryDate'].min(),
            end_date=df_historical_exercise['expiryDate'].max()
        )
        
        layout = dbc.Container([
            dbc.Row([
                dbc.Col(dropdown, width=4),
                dbc.Col(date_picker, width=8),
            ], className="mb-4"),
            dbc.Row([
                dbc.Col(dcc.Graph(id='historical-exercise-graph'), md=12)
            ]),
            dbc.Row([
                dbc.Col([
                    html.Button("Download Historical Exercise Data", id="btn-download-historical-exercise", className="mt-3 btn btn-primary"),
                ], width='auto'),
            ], className="mt-3")
        ], fluid=True)
    else:
        layout = dbc.Container([
            dbc.Row([
                dbc.Col(html.H3("Nenhum dado de exercício histórico disponível.", className="text-center"))
            ])
        ], fluid=True)
    return layout

# Callback para download do CSV Agregado
@app_dash.callback(
    Output("download-aggregated-csv", "data"),
    Input("btn-download-aggregated", "n_clicks"),
    prevent_initial_call=True
)
def download_aggregated_csv(n_clicks):
    logging.info(f"Botão de download CSV Agregado clicado {n_clicks} vezes.")
    if not df_daily.empty:
        return dcc.send_data_frame(df_daily.to_csv, "dados_resumidos.csv")
    return None

# Callback para download do CSV Completo
@app_dash.callback(
    Output("download-complete-csv", "data"),
    Input("btn-download-complete", "n_clicks"),
    prevent_initial_call=True
)
def download_complete_csv(n_clicks):
    logging.info(f"Botão de download CSV Completo clicado {n_clicks} vezes.")
    if not df_csv.empty:
        return dcc.send_data_frame(df_csv.to_csv, "dados_completos.csv")
    return None

# Callback para download do CSV de Exercício Histórico
@app_dash.callback(
    Output("download-historical-exercise-csv", "data"),
    Input("btn-download-historical-exercise", "n_clicks"),
    prevent_initial_call=True
)
def download_historical_exercise_csv(n_clicks):
    logging.info(f"Botão de download Histórico de Exercício clicado {n_clicks} vezes.")
    if not df_historical_exercise.empty:
        return dcc.send_data_frame(df_historical_exercise.to_csv, "dados_historical_exercise.csv")
    return None

# Callback para atualizar o gráfico de exercício histórico
@app_dash.callback(
    Output('historical-exercise-graph', 'figure'),
    [Input('filter-strikeResult', 'value'), Input('date-picker', 'start_date'), Input('date-picker', 'end_date')]
)
def update_historical_exercise_graph(strike_result, start_date, end_date):
    df_filtered = df_historical_exercise.copy()
    
    if strike_result:
        df_filtered = df_filtered[df_filtered['strikeResult'] == strike_result]
    if start_date and end_date:
        df_filtered = df_filtered[(df_filtered['expiryDate'] >= start_date) & (df_filtered['expiryDate'] <= end_date)]
    
    if df_filtered.empty:
        return go.Figure()

    df_candlestick = df_filtered.groupby(df_filtered['expiryDate'].dt.date).agg(
        open_price=('realStrikePrice', 'first'),
        high_price=('realStrikePrice', 'max'),
        low_price=('realStrikePrice', 'min'),
        close_price=('realStrikePrice', 'last')
    ).reset_index()
    
    fig = go.Figure(data=[go.Candlestick(
        x=df_candlestick['expiryDate'],
        open=df_candlestick['open_price'],
        high=df_candlestick['high_price'],
        low=df_candlestick['low_price'],
        close=df_candlestick['close_price'],
        increasing_line_color='green', decreasing_line_color='red'
    )])

    fig.update_layout(
        title="Real Strike Price Candlestick Chart Over Time",
        xaxis_title="Date",
        yaxis_title="Real Strike Price",
        plot_bgcolor="rgba(240, 240, 240, 0.5)",
        paper_bgcolor="rgba(255, 255, 255, 1)"
    )
    
    return fig

# Callback para atualizar dados periodicamente
@app_dash.callback(
    Output('dummy-output', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_data(n):
    load_data()
    return ''
