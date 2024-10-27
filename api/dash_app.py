# dash_app.py

import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import pandas as pd
from utils import (
    fetch_data,
    collection_csv,
    collection_options,
    collection_futures,
)
import plotly.express as px
from flask import Flask
from api_client import app as flask_app  # Importa o app Flask do api.py

# Criar visualização com Dash
app_dash = dash.Dash(__name__, server=flask_app, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Variáveis globais para armazenar os DataFrames
df_csv = pd.DataFrame()
df_options = pd.DataFrame()
df_futures = pd.DataFrame()
df_daily = pd.DataFrame()

# Função para carregar e processar os dados
def load_data():
    global df_csv, df_options, df_futures, df_daily

    # Carregar dados do MongoDB
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

    # Processar dados de opções, se necessário
    if not df_options.empty and 'time' in df_options.columns:
        df_options['time'] = pd.to_datetime(df_options['time'], errors='coerce')

    # Processar dados de futuros, se necessário
    if not df_futures.empty and 'time' in df_futures.columns:
        df_futures['time'] = pd.to_datetime(df_futures['time'], errors='coerce')

# Carregar os dados na inicialização
load_data()

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
    html.Div(id='tabs-content'),
    # Componente Interval para atualizar os dados periodicamente
    dcc.Interval(
        id='interval-component',
        interval=10*60*1000,  # Atualiza a cada 10 minutos
        n_intervals=0
    ),
    html.Div(id='dummy-output', style={'display': 'none'})
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
            labels={'time': 'Data', 'value': 'Preço (USD)', 'variable': 'Tipo'},
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
    if not df_daily.empty:
        return dcc.send_data_frame(df_daily.to_csv, "dados_resumidos.csv")
    else:
        return None

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

# Callback para atualizar os dados periodicamente
@app_dash.callback(
    Output('dummy-output', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_data(n):
    load_data()
    return ''
