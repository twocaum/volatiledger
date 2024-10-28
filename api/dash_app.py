# dash_app.py

import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import pandas as pd
from utils import (
    fetch_data,
    collection_csv,
    collection_historical_exercise,
)
import plotly.express as px
from flask import Flask
from api_client import app as flask_app  # Importa o app Flask do api.py
import plotly.graph_objs as go
# Create Dash app
app_dash = dash.Dash(__name__, server=flask_app, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Global variables for DataFrames
df_csv = pd.DataFrame()
df_historical_exercise = pd.DataFrame()
df_daily = pd.DataFrame()

# Load and process data
def load_data():
    global df_csv, df_historical_exercise, df_daily
    df_csv = fetch_data(collection_csv)
    df_historical_exercise = fetch_data(collection_historical_exercise)

    if not df_csv.empty and 'time' in df_csv.columns and 'price' in df_csv.columns:
        df_csv['price'] = pd.to_numeric(df_csv['price'], errors='coerce')
        df_csv['quantity'] = pd.to_numeric(df_csv['quantity'], errors='coerce')
        df_csv['time'] = pd.to_datetime(df_csv['time'], unit='ms')
        df_csv.set_index('time', inplace=True)
        df_csv = df_csv[df_csv.index >= "2020-01-01"]

        df_daily = df_csv.resample('W').agg({
            'price': ['mean', 'min', 'max'],
            'quantity': 'sum'
        }).reset_index()
        df_daily.columns = ['time', 'price_mean', 'price_min', 'price_max', 'total_quantity']
    else:
        df_daily = pd.DataFrame()

    if not df_historical_exercise.empty:
        df_historical_exercise['expiryDate'] = pd.to_datetime(df_historical_exercise['expiryDate'], errors='coerce')

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
    html.Div(id='dummy-output', style={'display': 'none'})
], fluid=True)

@app_dash.callback(Output('tabs-content', 'children'), [Input('tabs', 'value')])
def render_content(tab):
    if tab == 'tab-csv':
        return generate_csv_layout()
    elif tab == 'tab-historical-exercise':
        return generate_historical_exercise_layout()

def generate_csv_layout():
    if not df_daily.empty:
        fig_mean = px.line(df_daily, x='time', y='price_mean', title='Preço Médio Diário ao Longo do Tempo',
                           labels={'time': 'Data', 'price_mean': 'Preço Médio (USD)'}, markers=True)
        fig_min_max = px.line(df_daily, x='time', y=['price_min', 'price_max'],
                              title='Preços Mínimo e Máximo Diários ao Longo do Tempo',
                              labels={'time': 'Data', 'value': 'Preço (USD)', 'variable': 'Tipo'}, markers=True)
        fig_quantity = px.bar(df_daily, x='time', y='total_quantity',
                              title='Quantidade Total Diária de Transações',
                              labels={'time': 'Data', 'total_quantity': 'Quantidade Total'})

        layout = dbc.Container([
            dbc.Row([dbc.Col(dcc.Graph(id='mean-price', figure=fig_mean), md=12)]),
            dbc.Row([dbc.Col(dcc.Graph(id='min-max-price', figure=fig_min_max), md=12)]),
            dbc.Row([dbc.Col(dcc.Graph(id='total-quantity', figure=fig_quantity), md=12)]),
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
        layout = dbc.Container([dbc.Row([dbc.Col(html.H3("Nenhum dado encontrado ou erro ao carregar os dados CSV.", className="text-center"))])], fluid=True)
    return layout

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
    

# Download callback for Historical Exercise Records
@app_dash.callback(
    Output("download-historical-exercise-csv", "data"),
    Input("btn-download-historical-exercise", "n_clicks"),
    prevent_initial_call=True
)
def download_historical_exercise_csv(n_clicks):
    if not df_historical_exercise.empty:
        return dcc.send_data_frame(df_historical_exercise.to_csv, "dados_historical_exercise.csv")
    else:
        return None


def generate_historical_exercise_layout():
    if not df_historical_exercise.empty:
        layout = dbc.Container([
            dbc.Row([
                dbc.Col(dcc.Dropdown(
                    id='filter-strikeResult',
                    options=[{'label': res, 'value': res} for res in df_historical_exercise['strikeResult'].unique()],
                    value=None,
                    placeholder="Filtrar por Strike Result"
                ), width=4),
                dbc.Col(dcc.DatePickerRange(
                    id='date-picker',
                    min_date_allowed=df_historical_exercise['expiryDate'].min(),
                    max_date_allowed=df_historical_exercise['expiryDate'].max(),
                    start_date=df_historical_exercise['expiryDate'].min(),
                    end_date=df_historical_exercise['expiryDate'].max()
                ), width=8),
            ], className="mb-4"),
            dbc.Row([
                dbc.Col(dcc.Graph(id='historical-exercise-graph'), md=12)
            ]),
            dbc.Row([
                dbc.Col([
                    html.Button("Baixar Dados de Historical Exercise", id="btn-download-historical-exercise", className="mt-3 btn btn-primary"),
                    dcc.Download(id="download-historical-exercise-csv")  # Ensure this component is here for download to work
                ], width='auto'),
            ], className="mt-3"),
        ], fluid=True)
    else:
        layout = dbc.Container([
            dbc.Row([
                dbc.Col(html.H3("Nenhum dado encontrado ou erro ao carregar os dados de Historical Exercise Records.", className="text-center"))
            ])
        ], fluid=True)
    return layout


@app_dash.callback(
    Output('historical-exercise-graph', 'figure'),
    [Input('filter-strikeResult', 'value'), Input('date-picker', 'start_date'), Input('date-picker', 'end_date')]
)


def update_historical_exercise_graph(strike_result, start_date, end_date):
    df_filtered = df_historical_exercise.copy()
    
    # Apply filters
    if strike_result:
        df_filtered = df_filtered[df_filtered['strikeResult'] == strike_result]
    if start_date and end_date:
        df_filtered = df_filtered[(df_filtered['expiryDate'] >= start_date) & (df_filtered['expiryDate'] <= end_date)]
    
    # Aggregate data for candlestick chart
    df_candlestick = df_filtered.groupby(df_filtered['expiryDate'].dt.date).agg(
        open_price=('realStrikePrice', 'first'),
        high_price=('realStrikePrice', 'max'),
        low_price=('realStrikePrice', 'min'),
        close_price=('realStrikePrice', 'last')
    ).reset_index()

    # Create candlestick chart
    fig = go.Figure(data=[go.Candlestick(
        x=df_candlestick['expiryDate'],
        open=df_candlestick['open_price'],
        high=df_candlestick['high_price'],
        low=df_candlestick['low_price'],
        close=df_candlestick['close_price'],
        increasing_line_color='green', decreasing_line_color='red'
    )])

    # Update layout for better visualization
    fig.update_layout(
        title="Real Strike Price Candlestick Chart Over Time",
        xaxis_title="Date",
        yaxis_title="Real Strike Price",
        plot_bgcolor="rgba(240, 240, 240, 0.5)",
        paper_bgcolor="rgba(255, 255, 255, 1)"
    )
    
    return fig


@app_dash.callback(
    Output('dummy-output', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_data(n):
    load_data()
    return ''
